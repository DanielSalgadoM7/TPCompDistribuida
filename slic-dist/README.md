# SLIC distribuído

## Instalação
    pip install -r requirements.txt
    python -m grpc_tools.protoc -I proto --python_out=node --grpc_python_out=node proto/slic.proto

## Execução (sempre a partir da pasta slic-dist, um terminal por nó)
    python node/server.py --id 1
    python node/server.py --id 2
    python node/server.py --id 3
    python client.py foto.jpg --k 800 --out resultado.png

O nó de maior ID vira líder (Bully). Derrube o líder (Ctrl+C) e veja outro assumir.
Para várias máquinas, troque os hosts em config.yaml pelos IPs reais.

## Logs de Lamport (linha do tempo global)
    cat logs/*.log | sort -t, -k1 -n

## Teste do descarte de mensagens obsoletas (Lamport)
Com o nó 1 rodando em outro terminal:

    python test_stale.py

Mensagens com timestamp de Lamport antigo ou iteração anterior são rejeitadas pelo worker.
