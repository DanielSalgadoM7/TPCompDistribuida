"""Eleição de líder (Bully) + heartbeat."""
import threading
import time

import grpc
import slic_pb2


class Election:
    def __init__(self, node_id, stubs, clock, log, hb_interval=2.0, startup_delay=3.0):
        self.id = node_id
        self.stubs = {i: s for i, s in stubs.items() if i !=
                      node_id}  # só os outros nós
        self.node_ids = set(self.stubs) | {node_id}
        self.clock = clock
        self.log = log
        self.hb_interval = hb_interval
        self.startup_delay = startup_delay
        self.leader_id = None
        self._electing = False
        self._lock = threading.Lock()
        self._coord_event = threading.Event()

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()

    @property
    def current_leader(self):
        with self._lock:
            return self.leader_id

    # ---------- thread de heartbeat ----------
    def _loop(self):
        time.sleep(self.startup_delay)  # dá tempo para os outros nós subirem
        self.start_election()
        fails = 0
        while True:
            time.sleep(self.hb_interval)
            leader_id = self.current_leader
            if leader_id is None:
                self.start_election()
                continue
            if leader_id == self.id:
                continue
            try:
                msg = slic_pb2.Ping(from_id=self.id, lamport=self.clock.send())
                ack = self.stubs[leader_id].Heartbeat(msg, timeout=1.0)
                self.clock.receive(ack.lamport)
                fails = 0
            except grpc.RpcError:
                with self._lock:
                    if self.leader_id != leader_id:
                        fails = 0
                        continue
                    fails += 1
                    should_elect = fails >= 2
                    if should_elect:
                        self.leader_id = None
                if should_elect:
                    self.log(
                        f"líder {leader_id} não responde ao heartbeat -> eleição")
                    fails = 0
                    self.start_election()

    # ---------- algoritmo Bully ----------
    def start_election(self):
        with self._lock:
            if self._electing:
                return
            self._electing = True
        try:
            while True:
                self._coord_event.clear()
                self.log("inicia eleição")
                got_answer = False
                for nid in sorted(i for i in self.stubs if i > self.id):
                    try:
                        msg = slic_pb2.ElectionMsg(
                            from_id=self.id, lamport=self.clock.send())
                        ans = self.stubs[nid].Election(msg, timeout=1.0)
                        self.clock.receive(ans.lamport)
                        if ans.alive:
                            self.log(f"nó {nid} (maior) respondeu à eleição")
                            got_answer = True
                    except grpc.RpcError:
                        pass
                if not got_answer:
                    self._become_leader()
                    return
                # um nó maior assumiu a eleição: espera o anúncio Coordinator
                if self._coord_event.wait(timeout=5.0):
                    return
                self.log("Coordinator não chegou, reiniciando eleição")
        finally:
            with self._lock:
                self._electing = False

    def _become_leader(self):
        with self._lock:
            self.leader_id = self.id
        self.log("venceu a eleição: sou o LÍDER")
        for nid, stub in self.stubs.items():
            try:
                msg = slic_pb2.CoordinatorMsg(
                    leader_id=self.id, lamport=self.clock.send())
                ack = stub.Coordinator(msg, timeout=1.0)
                self.clock.receive(ack.lamport)
            except grpc.RpcError:
                pass

    # ---------- chamados pelos handlers gRPC (server.py) ----------
    def on_election(self, from_id):
        if from_id < self.id:  # um nó menor pediu eleição: respondo e assumo
            threading.Thread(target=self.start_election, daemon=True).start()

    def on_coordinator(self, leader_id):
        if leader_id not in self.node_ids:
            self.log(f"anúncio ignorado: ID de líder desconhecido ({leader_id})")
            return
        # Em Bully, um nó vivo de ID maior não deve aceitar um coordenador menor.
        # Ele inicia/continua sua eleição; épocas formais exigiriam mudar o protobuf.
        if leader_id < self.id:
            self.log(f"anúncio de líder inferior ({leader_id}) ignorado; iniciando eleição")
            threading.Thread(target=self.start_election, daemon=True).start()
            return
        with self._lock:
            self.leader_id = leader_id
        self._coord_event.set()
        self.log(f"novo líder reconhecido: nó {leader_id}")
