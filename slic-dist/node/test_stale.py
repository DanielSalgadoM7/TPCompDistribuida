"""Demonstra o descarte de mensagens obsoletas pelo relógio de Lamport.

Uso: com o nó 1 rodando (python node/server.py --id 1), execute: python test_stale.py
"""
import sys

sys.path.insert(0, "node")

import grpc
import numpy as np
import slic_pb2
import slic_pb2_grpc
import yaml

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)
node = cfg["nodes"][0]
stub = slic_pb2_grpc.NodeStub(grpc.insecure_channel(f'{node["host"]}:{node["port"]}'))

# job de brinquedo: faixa 10x20 pixels, 1 centro
lab = np.random.rand(10, 20, 3).astype(np.float32)
stub.LoadStrip(slic_pb2.Strip(lamport=10, job_id="teste", row_start=0, row_end=10,
                              width=20, lab_data=lab.tobytes()))


def enviar(ts, it):
    msg = slic_pb2.Centers(lamport=ts, job_id="teste", iteration=it,
                           values=[0.5, 0.5, 0.5, 10, 5], step=10, compactness=10)
    try:
        stub.RunIteration(msg)
        print(f"L={ts:>3} iteração {it}: ACEITA")
    except grpc.RpcError as e:
        print(f"L={ts:>3} iteração {it}: REJEITADA ({e.code().name}: {e.details()})")


enviar(20, 0)   # normal
enviar(30, 1)   # normal
enviar(25, 2)   # timestamp antigo (atrasada/duplicada) -> deve ser rejeitada
enviar(40, 0)   # iteração antiga -> deve ser rejeitada
enviar(50, 2)   # normal
