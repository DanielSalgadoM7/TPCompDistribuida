"""Ponto de entrada de cada nó.   Uso (a partir da pasta slic-dist):  python node/server.py --id 1"""
import argparse
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

    # ---------- cliente -> líder ----------
    def SubmitImage(self, request, context):
        self.clock.receive(request.lamport)
        if self.election.leader_id != self.id:
            return slic_pb2.ImageResult(lamport=self.clock.send(), ok=False,
                                        message="este nó não é o líder",
                                        leader_id=self.election.leader_id or 0)
        self.log("SubmitImage recebido do cliente")
        try:
            return coordinator.process(request, self.stubs, self.clock, self.log)
        except grpc.RpcError as e:
            self.log(f"falha em worker durante o job: {e.code()}")
            return slic_pb2.ImageResult(lamport=self.clock.send(), ok=False,
                                        message=f"falha em worker: {e.code()}")

    # ---------- líder -> worker ----------
    def LoadStrip(self, request, context):
        self.clock.receive(request.lamport)
        lab = np.frombuffer(
            request.lab_data, dtype=np.float32).reshape(-1, request.width, 3)
        self.jobs[request.job_id] = {"lab": lab,
                                     "g0": request.row_start, "labels": None}
        self.log(f"LoadStrip: linhas {request.row_start}-{request.row_end}")
        return slic_pb2.Ack(ok=True, lamport=self.clock.send())

    def RunIteration(self, request, context):
        self.clock.receive(request.lamport)
        job = self.jobs[request.job_id]
        centers = np.array(request.values, dtype=np.float32).reshape(-1, 5)
        labels = slic_core.assign(
            job["lab"], job["g0"], centers, request.step, request.compactness)
        sums, counts = slic_core.accumulate(
            job["lab"], job["g0"], labels, len(centers))
        job["labels"] = labels
        self.log(f"RunIteration {request.iteration} concluída")
        return slic_pb2.PartialSums(lamport=self.clock.send(), iteration=request.iteration,
                                    sums=sums.ravel().tolist(), counts=counts.tolist())

    def GetLabels(self, request, context):
        self.clock.receive(request.lamport)
        job = self.jobs.pop(request.job_id)  # libera memória
        labels = job["labels"]
        self.log("GetLabels: enviando rótulos")
        return slic_pb2.LabelStrip(lamport=self.clock.send(), row_start=job["g0"],
                                   row_end=job["g0"] + labels.shape[0], width=labels.shape[1],
                                   labels=labels.tobytes())

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
        return slic_pb2.Ack(ok=True, lamport=self.clock.send())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    addrs = {n["id"]: f'{n["host"]}:{n["port"]}' for n in cfg["nodes"]}
    port = addrs[args.id].split(":")[1]

    clock = LamportClock()
    log = make_logger(args.id, clock)
    # um stub para cada nó (inclusive este, assim o líder trata a si mesmo como worker)
    stubs = {nid: slic_pb2_grpc.NodeStub(grpc.insecure_channel(addr, options=OPTIONS))
             for nid, addr in addrs.items()}
    election = Election(args.id, stubs, clock, log)

    server = grpc.server(ThreadPoolExecutor(max_workers=16), options=OPTIONS)
    slic_pb2_grpc.add_NodeServicer_to_server(
        NodeService(args.id, stubs, clock, log, election), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log(f"nó {args.id} escutando na porta {port}")
    election.start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
