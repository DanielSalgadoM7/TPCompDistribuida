"""Lógica do líder: particiona a imagem, orquestra as iterações e agrega os centros."""
import math
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import grpc
import numpy as np
import slic_core
import slic_pb2
from skimage.color import rgb2lab

RPC_TIMEOUT = 30.0


def _par(fn, items):
    """Executa fn em paralelo (uma thread por item), preservando a ordem."""
    with ThreadPoolExecutor(max_workers=len(items)) as ex:
        return list(ex.map(fn, items))


def _alive_nodes(stubs, clock):
    alive = []
    for nid, stub in sorted(stubs.items()):
        try:
            ack = stub.Heartbeat(slic_pb2.Ping(from_id=0, lamport=clock.send()), timeout=1.0)
            clock.receive(ack.lamport)
            alive.append(nid)
        except grpc.RpcError:
            pass
    return alive


def _rpc_error(action, nid, error):
    code = error.code()
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return f"timeout ao {action} no nó {nid}"
    if code == grpc.StatusCode.UNAVAILABLE:
        return f"nó {nid} indisponível ao {action}"
    return f"falha RPC {code.name} ao {action} no nó {nid}: {error.details()}"


def process(req, stubs, clock, log):
    """Executa um job completo e devolve um ImageResult."""
    t0 = time.time()
    h, w = req.height, req.width
    if h <= 0 or w <= 0:
        raise ValueError("altura e largura devem ser maiores que zero")
    if len(req.rgb) != h * w * 3:
        raise ValueError("buffer RGB deve conter exatamente altura x largura x 3 bytes")
    if req.k < 0 or req.k > h * w:
        raise ValueError("k deve ser zero (padrão) ou estar entre 1 e a quantidade de pixels")
    if req.max_iter < 0:
        raise ValueError("max_iter não pode ser negativo")
    if not math.isfinite(req.compactness) or req.compactness < 0:
        raise ValueError("compactness deve ser um número finito não negativo")
    m = req.compactness or 10.0
    max_iter = req.max_iter or 10

    rgb = np.frombuffer(req.rgb, dtype=np.uint8).reshape(h, w, 3)
    lab = rgb2lab(rgb).astype(np.float32)
    centers, s = slic_core.init_centers(lab, req.k or 500)
    k = len(centers)
    job_id = uuid.uuid4().hex

    nodes = _alive_nodes(stubs, clock)
    if not nodes:
        raise RuntimeError("nenhum nó respondeu ao heartbeat")
    # Faixas vazias não são válidas para o processamento SLIC.
    nodes = nodes[:h]
    bounds = np.linspace(0, h, len(nodes) + 1).astype(int)
    parts = [{"partition": idx, "worker": nid, "start": int(r0), "end": int(r1),
              "tasks": {}}
             for idx, (nid, r0, r1) in enumerate(zip(nodes, bounds[:-1], bounds[1:]))]
    unavailable = set()
    unavailable_lock = Lock()
    log(f"job {job_id}: {w}x{h}, K={k}, S={s}, workers={nodes}",
        event_type="job_started", job_id=job_id)

    def task_for(part, iteration):
        return part["tasks"].setdefault(iteration, {
            "job_id": job_id, "partition": part["partition"], "iteration": iteration,
            "worker": part["worker"], "state": "PENDING", "attempts": 0,
            "result": None})

    def invoke(part, task, operation, make_request, action, prepare=None):
        """Tenta a faixa em workers vivos; cada worker guarda seu próprio estado."""
        last_error = None
        preferred = task["worker"]
        candidates = [preferred] + [nid for nid in nodes if nid != preferred]
        for nid in candidates:
            with unavailable_lock:
                if nid in unavailable:
                    continue
            part["worker"] = task["worker"] = nid
            task["attempts"] += 1
            task["state"] = "RUNNING"
            try:
                if nid != preferred and prepare is not None:
                    prepare(stubs[nid], nid)
                response = operation(stubs[nid], make_request(nid))
                if hasattr(response, "lamport"):
                    clock.receive(response.lamport)
                task["state"] = "COMPLETED"
                task["result"] = response
                return response
            except grpc.RpcError as error:
                last_error = _rpc_error(action, nid, error)
                with unavailable_lock:
                    unavailable.add(nid)
                task["state"] = "PENDING"
                log(f"job {job_id} faixa {part['partition']}: {last_error}; tentando outro worker",
                    event_type="rpc_failure", job_id=job_id,
                    message_id=f"partition-{part['partition']}")
        task["state"] = "FAILED"
        raise RuntimeError(last_error or
                           f"job {job_id} faixa {part['partition']}: nenhum worker disponível")

    # Identificador independente por faixa: chamadas atrasadas de um worker não
    # alteram o estado nem os resultados mantidos por outro worker.
    def part_job(part):
        return f"{job_id}-{part['partition']}"

    # 1) envia cada faixa uma única vez
    def load(part):
        r0, r1 = part["start"], part["end"]
        task = task_for(part, "load")
        return invoke(part, task, lambda stub, msg: stub.LoadStrip(msg, timeout=RPC_TIMEOUT),
                      lambda _nid: slic_pb2.Strip(lamport=clock.send(), job_id=part_job(part),
                          row_start=r0, row_end=r1, width=w, lab_data=lab[r0:r1].tobytes()),
                      "carregar faixa")

    _par(load, parts)
    log(f"job {job_id}: faixas enviadas", event_type="strips_loaded", job_id=job_id)

    def load_on(stub, part):
        ack = stub.LoadStrip(slic_pb2.Strip(lamport=clock.send(), job_id=part_job(part),
            row_start=part["start"], row_end=part["end"], width=w,
            lab_data=lab[part["start"]:part["end"]].tobytes()), timeout=RPC_TIMEOUT)
        clock.receive(ack.lamport)

    final_iteration = -1
    final_values = None

    # 2) loop de iterações: só centros e somas parciais trafegam
    for it in range(max_iter):
        vals = centers.ravel().tolist()
        final_iteration, final_values = it, vals

        def restore_iteration(stub, part, iteration, iteration_values):
            load_on(stub, part)
            response = stub.RunIteration(slic_pb2.Centers(lamport=clock.send(),
                job_id=part_job(part), iteration=iteration, values=iteration_values,
                step=s, compactness=m), timeout=RPC_TIMEOUT)
            clock.receive(response.lamport)

        def run(part, it=it, vals=vals):
            task = task_for(part, it)
            for _attempt in range(3):
                r = invoke(part, task, lambda stub, request: stub.RunIteration(
                    request, timeout=RPC_TIMEOUT),
                    lambda _nid: slic_pb2.Centers(lamport=clock.send(), job_id=part_job(part),
                        iteration=it, values=vals, step=s, compactness=m),
                    "executar iteração",
                    prepare=lambda stub, _nid: restore_iteration(stub, part, it, vals))
                if r.iteration == it and len(r.sums) == k * 5 and len(r.counts) == k:
                    result = (np.asarray(r.sums, dtype=np.float64).reshape(k, 5),
                              np.asarray(r.counts, dtype=np.int64))
                    task["result"] = result
                    return result
                log(f"job {job_id} faixa {part['partition']}: resposta inválida para iteração {it}")
            task["state"] = "FAILED"
            raise RuntimeError(f"resultado inválido na faixa {part['partition']}, iteração {it}")

        results = _par(run, parts)
        sums = sum(r[0] for r in results)
        counts = sum(r[1] for r in results)
        centers, residual = slic_core.update_centers(sums, counts, centers)
        log(f"job {job_id}: iteração {it} resíduo={residual:.3f}")
        if residual < 0.1:
            break

    # 3) coleta os rótulos e monta o mapa completo
    def get(part):
        task = task_for(part, f"labels-{final_iteration}")
        def restore(stub, _nid):
            load_on(stub, part)
            response = stub.RunIteration(slic_pb2.Centers(lamport=clock.send(),
                job_id=part_job(part), iteration=final_iteration, values=final_values,
                step=s, compactness=m), timeout=RPC_TIMEOUT)
            clock.receive(response.lamport)
        r = invoke(part, task, lambda stub, request: stub.GetLabels(request, timeout=RPC_TIMEOUT),
                   lambda _nid: slic_pb2.JobRef(lamport=clock.send(), job_id=part_job(part)),
                   "coletar rótulos", prepare=restore)
        expected_rows = part["end"] - part["start"]
        if (r.row_start != part["start"] or r.row_end != part["end"] or r.width != w or
                len(r.labels) != expected_rows * w * np.dtype(np.int32).itemsize):
            task["state"] = "FAILED"
            raise RuntimeError(f"worker retornou faixa inválida para a partição {part['partition']}")
        return r.row_start, np.frombuffer(r.labels, dtype=np.int32).reshape(expected_rows, w)

    returned = sorted(_par(get, parts), key=lambda item: item[0])
    expected_start = 0
    for row_start, strip in returned:
        if row_start != expected_start:
            raise RuntimeError("agregação incompleta: há lacunas ou sobreposições entre faixas")
        expected_start += strip.shape[0]
    if not returned or expected_start != h:
        raise RuntimeError("agregação incompleta: as faixas não cobrem a imagem")
    labels = np.concatenate([item[1] for item in returned], axis=0)
    labels = slic_core.enforce_connectivity(labels, s)
    elapsed = time.time() - t0
    log(f"job {job_id}: concluído, {labels.max() + 1} superpixels em {elapsed:.2f}s",
        event_type="job_completed", job_id=job_id)

    return slic_pb2.ImageResult(lamport=clock.send(), ok=True, width=w, height=h,
                                labels=labels.tobytes(), num_superpixels=int(labels.max() + 1),
                                elapsed=elapsed)
