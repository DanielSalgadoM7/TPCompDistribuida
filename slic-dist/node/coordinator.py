"""Lógica do líder: particiona a imagem, orquestra as iterações e agrega os centros."""
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc
import numpy as np
import slic_core
import slic_pb2
from skimage.color import rgb2lab


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


def process(req, stubs, clock, log):
    """Executa um job completo e devolve um ImageResult."""
    t0 = time.time()
    h, w = req.height, req.width
    m = req.compactness or 10.0
    max_iter = req.max_iter or 10

    rgb = np.frombuffer(req.rgb, dtype=np.uint8).reshape(h, w, 3)
    lab = rgb2lab(rgb).astype(np.float32)
    centers, s = slic_core.init_centers(lab, req.k or 500)
    k = len(centers)
    job_id = uuid.uuid4().hex[:8]

    nodes = _alive_nodes(stubs, clock)
    bounds = np.linspace(0, h, len(nodes) + 1).astype(int)
    parts = list(zip(nodes, bounds[:-1], bounds[1:]))
    log(f"job {job_id}: {w}x{h}, K={k}, S={s}, workers={nodes}")

    # 1) envia cada faixa uma única vez
    def load(part):
        nid, r0, r1 = part
        msg = slic_pb2.Strip(lamport=clock.send(), job_id=job_id, row_start=int(r0),
                             row_end=int(r1), width=w, lab_data=lab[r0:r1].tobytes())
        clock.receive(stubs[nid].LoadStrip(msg).lamport)

    _par(load, parts)
    log(f"job {job_id}: faixas enviadas")

    # 2) loop de iterações: só centros e somas parciais trafegam
    for it in range(max_iter):
        vals = centers.ravel().tolist()

        def run(part, it=it, vals=vals):
            nid = part[0]
            for attempt in range(3):
                sent = clock.send()
                msg = slic_pb2.Centers(lamport=sent, job_id=job_id, iteration=it,
                                       values=vals, step=s, compactness=m)
                r = stubs[nid].RunIteration(msg)
                clock.receive(r.lamport)
                # Uma resposta legítima é causalmente posterior ao pedido (r.lamport > sent)
                # e pertence a esta iteração. Caso contrário é obsoleta: descarta e reenvia.
                if r.lamport > sent and r.iteration == it:
                    return np.array(r.sums).reshape(k, 5), np.array(r.counts, dtype=np.int64)
                log(f"job {job_id}: resposta obsoleta do nó {nid} descartada "
                    f"(L_resp={r.lamport}, L_envio={sent}, it={r.iteration}, esperado {it})")
            raise RuntimeError(f"nó {nid} não respondeu corretamente à iteração {it}")

        results = _par(run, parts)
        sums = sum(r[0] for r in results)
        counts = sum(r[1] for r in results)
        centers, residual = slic_core.update_centers(sums, counts, centers)
        log(f"job {job_id}: iteração {it} resíduo={residual:.3f}")
        if residual < 0.1:
            break

    # 3) coleta os rótulos e monta o mapa completo
    def get(part):
        nid = part[0]
        r = stubs[nid].GetLabels(slic_pb2.JobRef(lamport=clock.send(), job_id=job_id))
        clock.receive(r.lamport)
        return np.frombuffer(r.labels, dtype=np.int32).reshape(-1, w)

    labels = np.concatenate(_par(get, parts), axis=0)
    labels = slic_core.enforce_connectivity(labels, s)
    elapsed = time.time() - t0
    log(f"job {job_id}: concluído, {labels.max() + 1} superpixels em {elapsed:.2f}s")

    return slic_pb2.ImageResult(lamport=clock.send(), ok=True, width=w, height=h,
                                labels=labels.tobytes(), num_superpixels=int(labels.max() + 1),
                                elapsed=elapsed)
