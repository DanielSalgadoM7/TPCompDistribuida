import os
import threading


class LamportClock:
    """Relógio lógico de Lamport (thread-safe)."""

    def __init__(self):
        self.t = 0
        self._lock = threading.Lock()

    def tick(self):
        """Evento local."""
        with self._lock:
            self.t += 1
            return self.t

    def send(self):
        """Antes de enviar mensagem: incrementa e devolve o timestamp a ser enviado."""
        return self.tick()

    def receive(self, t_msg):
        """Ao receber mensagem: t = max(t, t_msg) + 1."""
        with self._lock:
            self.t = max(self.t, t_msg) + 1
            return self.t


def make_logger(node_id, clock):
    """Cria uma função log(evento) que imprime e grava (lamport, nó, evento).

    Para juntar os logs de todos os nós em ordem causal:
        cat logs/*.log | sort -t, -k1 -n
    """
    os.makedirs("logs", exist_ok=True)
    path = f"logs/node_{node_id}.log"
    open(path, "w").close()  # zera o log a cada execução
    lock = threading.Lock()

    def log(evento):
        t = clock.tick()
        with lock:
            print(f"[L={t:>4}] [nó {node_id}] {evento}", flush=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(f"{t},{node_id},{evento}\n")

    return log
