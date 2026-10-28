"""Cliente.  Uso:  python client.py foto.jpg --k 800 --out resultado.png"""
import argparse
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
    addrs = {n["id"]: f'{n["host"]}:{n["port"]}' for n in cfg["nodes"]}

    img = io.imread(a.image)
    if img.ndim == 2:
        img = gray2rgb(img)
    img = np.ascontiguousarray(img_as_ubyte(img[..., :3]))
    h, w, _ = img.shape
    req = slic_pb2.ImageRequest(lamport=1, width=w, height=h, rgb=img.tobytes(),
                                k=a.k, compactness=a.m, max_iter=a.iter)

    # tenta o maior ID primeiro (provável líder); se não for líder, o nó indica quem é
    queue = sorted(addrs, reverse=True)
    tried, result = set(), None
    while queue:
        nid = queue.pop(0)
        if nid in tried:
            continue
        tried.add(nid)
        try:
            stub = slic_pb2_grpc.NodeStub(
                grpc.insecure_channel(addrs[nid], options=OPTIONS))
            res = stub.SubmitImage(req, timeout=900)
        except grpc.RpcError as e:
            print(f"nó {nid} indisponível ({e.code()})")
            continue
        if res.ok:
            result = res
            break
        print(f"nó {nid}: {res.message}")
        if res.leader_id and res.leader_id not in tried:
            queue.insert(0, res.leader_id)

    if result is None:
        sys.exit("nenhum líder conseguiu processar a imagem")

    labels = np.frombuffer(result.labels, dtype=np.int32).reshape(h, w)
    print(f"{result.num_superpixels} superpixels em {result.elapsed:.2f}s (líder processou)")
    io.imsave(a.out, img_as_ubyte(segmentation.mark_boundaries(img, labels)))
    print(f"salvo em {a.out}")


if __name__ == "__main__":
    main()
