"""Cliente.  Uso:  python client.py foto.jpg --k 800 --out resultado.png"""
import argparse
import math
import sys

sys.path.insert(0, "node")  # para achar slic_pb2 / slic_pb2_grpc

import grpc
import numpy as np
import slic_pb2
import slic_pb2_grpc
import yaml
from skimage import img_as_ubyte, io, segmentation
from skimage.color import gray2rgb

MAX_MSG = 512 * 1024 * 1024
OPTIONS = [("grpc.max_send_message_length", MAX_MSG),
           ("grpc.max_receive_message_length", MAX_MSG)]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("image")
    p.add_argument("--k", type=int, default=500, help="número de superpixels")
    p.add_argument("--m", type=float, default=10.0, help="compactness")
    p.add_argument("--iter", type=int, default=10)
    p.add_argument("--out", default="resultado.png")
    a = p.parse_args()

    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)
    nodes = cfg.get("nodes") if isinstance(cfg, dict) else None
    if not isinstance(nodes, list) or not nodes:
        p.error("configuração deve conter uma lista não vazia em 'nodes'")
    ids = [node.get("id") for node in nodes if isinstance(node, dict)]
    if (len(ids) != len(nodes) or
            any(not isinstance(node_id, int) or isinstance(node_id, bool) or node_id <= 0
                for node_id in ids) or len(ids) != len(set(ids))):
        p.error("cada nó deve ter um ID inteiro positivo e único")
    for node in nodes:
        if (not isinstance(node.get("host"), str) or not node["host"] or
                not isinstance(node.get("port"), int) or isinstance(node.get("port"), bool) or
                not 1 <= node["port"] <= 65535):
            p.error("cada nó deve ter host e porta entre 1 e 65535")
    addrs = {n["id"]: f'{n["host"]}:{n["port"]}' for n in nodes}

    img = io.imread(a.image)
    if img.ndim == 2:
        img = gray2rgb(img)
    if img.ndim != 3 or img.shape[2] < 3:
        p.error("a imagem deve possuir pelo menos três canais de cor")
    img = np.ascontiguousarray(img_as_ubyte(img[..., :3]))
    h, w, _ = img.shape
    if h <= 0 or w <= 0:
        p.error("a imagem deve ter largura e altura maiores que zero")
    if not 1 <= a.k <= h * w:
        p.error("--k deve estar entre 1 e a quantidade de pixels da imagem")
    if a.iter < 1:
        p.error("--iter deve ser maior que zero")
    if not math.isfinite(a.m) or a.m <= 0:
        p.error("--m deve ser um número finito maior que zero")
    req = slic_pb2.ImageRequest(lamport=1, width=w, height=h, rgb=img.tobytes(),
                                k=a.k, compactness=a.m, max_iter=a.iter)

    # tenta o maior ID primeiro (provável líder); se não for líder, o nó indica quem é
    queue = sorted(addrs, reverse=True)
    tried, result, channels = set(), None, {}
    try:
        while queue:
            nid = queue.pop(0)
            if nid in tried:
                continue
            tried.add(nid)
            try:
                channel = channels.get(nid)
                if channel is None:
                    channel = grpc.insecure_channel(addrs[nid], options=OPTIONS)
                    channels[nid] = channel
                stub = slic_pb2_grpc.NodeStub(channel)
                res = stub.SubmitImage(req, timeout=180)
            except grpc.RpcError as e:
                print(f"nó {nid} indisponível ({e.code()})")
                continue
            if res.ok:
                result = res
                break
            print(f"nó {nid}: {res.message}")
            if res.leader_id in addrs and res.leader_id not in tried:
                queue.insert(0, res.leader_id)
    finally:
        for channel in channels.values():
            channel.close()

    if result is None:
        sys.exit("nenhum líder conseguiu processar a imagem")

    if (result.width != w or result.height != h or
            len(result.labels) != h * w * np.dtype(np.int32).itemsize):
        sys.exit("o líder retornou um mapa de rótulos inválido")
    labels = np.frombuffer(result.labels, dtype=np.int32).reshape(h, w)
    print(f"{result.num_superpixels} superpixels em {result.elapsed:.2f}s (coordenado pelo líder)")
    io.imsave(a.out, img_as_ubyte(segmentation.mark_boundaries(img, labels)))
    print(f"salvo em {a.out}")


if __name__ == "__main__":
    main()
