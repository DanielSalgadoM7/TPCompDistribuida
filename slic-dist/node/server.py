"""Ponto de entrada de cada nó.   Uso (a partir da pasta slic-dist):  python node/server.py --id 1"""
import argparse
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import coordinator
import grpc
import numpy as np
import slic_core
import slic_pb2
import slic_pb2_grpc
import yaml
from election import Election
from lamport import LamportClock, make_logger

MAX_MSG = 512 * 1024 * 1024  # o padrão do gRPC (4 MB) não comporta imagens
OPTIONS = [("grpc.max_send_message_length", MAX_MSG),
           ("grpc.max_receive_message_length", MAX_MSG)]


class NodeService(slic_pb2_grpc.NodeServicer):
    def __init__(self, node_id, stubs, clock, log, election):
        self.id = node_id
        self.stubs = stubs
        self.clock = clock
        self.log = log
        self.election = election
        self.jobs = {}  # job_id -> {"lab", "g0", "labels"}  (estado do worker)
        self.jobs_lock = threading.Lock()

    def _cleanup_jobs(self):
        now = time.monotonic()
        with self.jobs_lock:
            expired = [key for key, value in self.jobs.items()
                       if now - value["updated_at"] > 3600]
            for key in expired:
                self.jobs.pop(key, None)

    # ---------- cliente -> líder ----------
    def SubmitImage(self, request, context):
        self.clock.receive(request.lamport)
        leader_id = self.election.current_leader
        if leader_id != self.id:
            return slic_pb2.ImageResult(lamport=self.clock.send(), ok=False,
                                        message="este nó não é o líder",
                                        leader_id=leader_id or 0)
        self.log("SubmitImage recebido do cliente")
        try:
            return coordinator.process(request, self.stubs, self.clock, self.log)
        except ValueError as e:
            return slic_pb2.ImageResult(lamport=self.clock.send(), ok=False,
                                        message=f"dados inválidos: {e}")
        except (grpc.RpcError, RuntimeError) as e:
            motivo = f"{e.code().name}: {e.details()}" if isinstance(e, grpc.RpcError) else str(e)
            self.log(f"falha durante o job: {motivo}")
            return slic_pb2.ImageResult(lamport=self.clock.send(), ok=False,
                                        message=f"falha controlada no processamento: {motivo}")
        except Exception as e:
            self.log(f"erro inesperado no job: {type(e).__name__}: {e}")
            return slic_pb2.ImageResult(lamport=self.clock.send(), ok=False,
                                        message=f"erro interno durante o processamento ({type(e).__name__})")

    # ---------- líder -> worker ----------
    def LoadStrip(self, request, context):
        self.clock.receive(request.lamport)
        if not request.job_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "job_id não pode ser vazio")
        if request.width <= 0 or request.row_start < 0 or request.row_end <= request.row_start:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "limites da faixa ou largura inválidos")
        expected = (request.row_end - request.row_start) * request.width * 3 * np.dtype(np.float32).itemsize
        if len(request.lab_data) != expected:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tamanho de lab_data incompatível com a faixa")
        lab = np.frombuffer(request.lab_data, dtype=np.float32).reshape(
            request.row_end - request.row_start, request.width, 3)
        if not np.isfinite(lab).all():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "lab_data deve conter valores finitos")
        self._cleanup_jobs()
        now = time.monotonic()
        with self.jobs_lock:
            previous = self.jobs.get(request.job_id)
            if previous is not None:
                if (previous["lab"] is not None and previous["g0"] == request.row_start and
                        previous["lab"].shape == lab.shape and
                        np.array_equal(previous["lab"], lab)):
                    previous["updated_at"] = now
                    return slic_pb2.Ack(ok=True, lamport=self.clock.send())
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "job_id já carregado com outra faixa")
            completed = [key for key, value in self.jobs.items()
                         if value["label_result"] is not None]
            while len(self.jobs) >= 64 and completed:
                self.jobs.pop(completed.pop(0), None)
            job = {"lab": lab, "g0": request.row_start, "labels": None,
                   "last_ts": request.lamport, "last_it": -1, "results": {},
                   "lock": threading.RLock(), "label_result": None,
                   "updated_at": now}
            self.jobs[request.job_id] = job
        self.log(f"LoadStrip: linhas {request.row_start}-{request.row_end}")
        return slic_pb2.Ack(ok=True, lamport=self.clock.send())

    def RunIteration(self, request, context):
        self.clock.receive(request.lamport)
        with self.jobs_lock:
            job = self.jobs.get(request.job_id)
        if job is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "job desconhecido")
        if (request.iteration < 0 or not request.values or len(request.values) % 5 or
                not np.isfinite(request.values).all()):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "centros ou iteração inválidos")
        if request.step <= 0 or not math.isfinite(request.step) or not math.isfinite(request.compactness) or request.compactness < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "step/compactness inválidos")
        with job["lock"]:
            cached = job["results"].get(request.iteration)
            if cached is not None:
                signature = (tuple(request.values), request.step, request.compactness)
                if cached[0] != signature:
                    context.abort(grpc.StatusCode.ALREADY_EXISTS,
                                  "iteração repetida com centros diferentes")
                return slic_pb2.PartialSums(lamport=self.clock.send(), iteration=request.iteration,
                                            sums=cached[1], counts=cached[2])
            if request.iteration <= job["last_it"] or request.lamport <= job["last_ts"]:
                self.log(f"DESCARTADA mensagem obsoleta: L={request.lamport}, it={request.iteration}")
                context.abort(grpc.StatusCode.ABORTED, "mensagem obsoleta")
            try:
                centers = np.asarray(request.values, dtype=np.float32).reshape(-1, 5)
                labels = slic_core.assign(job["lab"], job["g0"], centers,
                                          request.step, request.compactness)
                sums, counts = slic_core.accumulate(job["lab"], job["g0"], labels, len(centers))
            except Exception as error:
                self.log(f"falha ao processar iteração: {type(error).__name__}: {error}")
                context.abort(grpc.StatusCode.INTERNAL, "falha interna ao processar faixa")
            job["last_ts"] = request.lamport
            job["last_it"] = request.iteration
            job["updated_at"] = time.monotonic()
            job["labels"] = labels
            signature = (tuple(request.values), request.step, request.compactness)
            cached = (signature, tuple(sums.ravel().tolist()), tuple(counts.tolist()))
            job["results"] = {request.iteration: cached}
            self.log(f"RunIteration {request.iteration} concluída", event_type="task_completed",
                     job_id=request.job_id, message_id=f"iteration-{request.iteration}",
                     iteration=request.iteration)
            return slic_pb2.PartialSums(lamport=self.clock.send(), iteration=request.iteration,
                                        sums=cached[1], counts=cached[2])

    def GetLabels(self, request, context):
        self.clock.receive(request.lamport)
        with self.jobs_lock:
            job = self.jobs.get(request.job_id)
        if job is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "job desconhecido")
        with job["lock"]:
            if job["label_result"] is not None:
                result = job["label_result"]
                job["updated_at"] = time.monotonic()
                return slic_pb2.LabelStrip(lamport=self.clock.send(), row_start=result[0],
                                           row_end=result[1], width=result[2], labels=result[3])
            labels = job["labels"]
            if labels is None:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              "rótulos ainda não estão disponíveis")
            row_end = job["g0"] + labels.shape[0]
            job["label_result"] = (job["g0"], row_end, labels.shape[1], labels.tobytes())
            job["updated_at"] = time.monotonic()
            # A resposta serializada basta para retries; solte os arrays grandes do job.
            job["lab"] = None
            job["labels"] = None
            job["results"].clear()
            self.log("GetLabels: enviando rótulos")
            return slic_pb2.LabelStrip(lamport=self.clock.send(), row_start=job["g0"],
                                       row_end=row_end, width=labels.shape[1],
                                       labels=job["label_result"][3])

    # ---------- eleição e liveness ----------
    def Election(self, request, context):
        self.clock.receive(request.lamport)
        self.log(f"Election recebido do nó {request.from_id}")
        self.election.on_election(request.from_id)
        return slic_pb2.ElectionAnswer(alive=True, lamport=self.clock.send())

    def Coordinator(self, request, context):
        self.clock.receive(request.lamport)
        self.election.on_coordinator(request.leader_id)
        return slic_pb2.Ack(ok=True, lamport=self.clock.send())

    def Heartbeat(self, request, context):
        self.clock.receive(request.lamport)
        self._cleanup_jobs()
        return slic_pb2.Ack(ok=True, lamport=self.clock.send())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    nodes = cfg.get("nodes") if isinstance(cfg, dict) else None
    if not isinstance(nodes, list) or not nodes:
        parser.error("configuração deve conter uma lista não vazia em 'nodes'")
    ids = [node.get("id") for node in nodes if isinstance(node, dict)]
    if (len(ids) != len(nodes) or
            any(not isinstance(node_id, int) or isinstance(node_id, bool) or node_id <= 0
                for node_id in ids) or len(ids) != len(set(ids))):
        parser.error("cada nó deve ter um ID inteiro positivo e único")
    for node in nodes:
        if (not isinstance(node.get("host"), str) or not node["host"] or
                not isinstance(node.get("port"), int) or isinstance(node.get("port"), bool) or
                not 1 <= node["port"] <= 65535):
            parser.error("cada nó deve ter host e porta entre 1 e 65535")
    addrs = {n["id"]: f'{n["host"]}:{n["port"]}' for n in nodes}
    if args.id not in addrs:
        parser.error(f"ID {args.id} não está definido na configuração")
    port = next(node["port"] for node in nodes if node["id"] == args.id)

    clock = LamportClock()
    log = make_logger(args.id, clock)
    # um stub para cada nó (inclusive este, assim o líder trata a si mesmo como worker)
    channels = {nid: grpc.insecure_channel(addr, options=OPTIONS)
                for nid, addr in addrs.items()}
    stubs = {nid: slic_pb2_grpc.NodeStub(channel) for nid, channel in channels.items()}
    election = Election(args.id, stubs, clock, log)

    server = grpc.server(ThreadPoolExecutor(max_workers=16), options=OPTIONS)
    slic_pb2_grpc.add_NodeServicer_to_server(
        NodeService(args.id, stubs, clock, log, election), server)
    if server.add_insecure_port(f"[::]:{port}") == 0:
        for channel in channels.values():
            channel.close()
        raise RuntimeError(f"não foi possível abrir a porta {port}")
    try:
        server.start()
    except Exception:
        for channel in channels.values():
            channel.close()
        raise
    log(f"nó {args.id} escutando na porta {port}")
    election.start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        server.stop(0).wait()
        for channel in channels.values():
            channel.close()


if __name__ == "__main__":
    main()
