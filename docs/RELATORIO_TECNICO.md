# Relatório técnico — SLIC Distribuído

**Data da auditoria:** 31/10/2026
**Branch examinada:** `feature/melhorias`
**Escopo:** análise estática do repositório e do diff local. Nenhum teste, benchmark ou servidor foi executado, conforme solicitado.

## 1. Visão geral

O projeto segmenta imagens com uma implementação própria de SLIC dividida entre um líder e workers. O cliente lê RGB, envia um `ImageRequest` por gRPC e salva uma visualização dos limites dos superpixels. O líder converte a imagem para CIELAB, inicializa centros, reparte linhas, carrega as faixas nos workers, coordena iterações, soma resultados parciais, recolhe rótulos e aplica conectividade.

O serviço `Node` e as mensagens são definidos em `slic-dist/proto/slic.proto`. Há RPCs para submissão, carga/processamento/coleta de faixas, eleição, anúncio de coordenador e heartbeat. `ThreadPoolExecutor` paraleliza chamadas por faixa. `Election` implementa Bully com heartbeat e `LamportClock` mantém o contador lógico com lock.

O `rg --files` enumerou 25 arquivos do projeto antes da criação deste relatório. Foram examinados os 14 arquivos existentes de código, documentação, configuração, dependências, schema e stubs textuais; imagens, logs e bytecode compilado foram identificados na árvore, mas não interpretados como fonte. O arquivo deste relatório é uma nova 15ª peça textual. A diferença `main...HEAD` não contém commits; as alterações da branch estão no working tree e foram preservadas.

## 2. Diagnóstico inicial

Na auditoria anterior às melhorias, o código já possuía cliente gRPC, particionamento horizontal, processamento paralelo, agregação, algoritmo SLIC, Bully, heartbeat, Lamport e rejeição de algumas mensagens antigas. O principal problema era a lacuna entre detectar nós ativos antes do job e tratar sua falha durante uma RPC de processamento.

As chamadas internas de `LoadStrip`, `RunIteration` e `GetLabels` não tinham timeout. Uma falha durante a iteração podia bloquear ou abortar o job, sem redistribuir a faixa. O worker sobrescrevia estado se recebesse novamente o mesmo `job_id`; `RunIteration` rejeitava iterações menores, mas aceitava repetição da mesma iteração com timestamp mais alto. `GetLabels` removia o job sem tratar repetição ou chave inexistente. Estado não era replicado quando o líder falhava.

As alterações locais anteriores já haviam introduzido timeout, tentativa em worker alternativo, cache idempotente básico, locks, validações, logs CSV, pins de dependências e documentação de limitações. Esta auditoria encontrou pontos a completar nessa evolução: a assinatura do cache não incluía `step`/`compactness`, os IDs de job tinham apenas 32 bits, o utilitário de mensagens antigas reutilizava ID fixo, as conexões não eram fechadas explicitamente, e a documentação ainda estava duplicada e não cobria a fundamentação pedida.

## 3. Problemas identificados

“Estático” significa que a causa decorre da leitura do código; não significa que o comportamento tenha sido reproduzido em execução. “Risco” indica cenário dependente da rede, carga ou concorrência real.

| Severidade | Arquivo/função | Problema e consequência | Situação |
|---|---|---|---|
| ALTO | `node/coordinator.py:process` | Antes das melhorias, três RPCs internas não tinham deadline; worker indisponível podia deixar o job pendente indefinidamente. | Corrigido com timeout e tratamento. |
| ALTO | `node/coordinator.py:invoke` / `process` | Antes, falha após heartbeat não tinha reatribuição. Uma faixa sem resposta impedia a agregação. | Corrigido para tentar outros workers inicialmente ativos e refazer a operação. |
| ALTO | `node/server.py:RunIteration` | A checagem anterior aceitava a mesma iteração com timestamp maior; retry podia recalcular/alterar estado. | Corrigido com cache por iteração, assinatura e lock por job. |
| ALTO | `node/server.py:RunIteration` | Na primeira versão do cache, só `values` eram comparados; `step` ou `compactness` diferentes podiam receber parciais calculadas com parâmetros antigos. | Corrigido incluindo ambos na assinatura. |
| ALTO | `node/server.py:GetLabels` | A remoção imediata do job fazia retry de resposta perdida falhar com chave inexistente e eliminava o resultado idempotente. | Corrigido com resposta de rótulos serializada em cache. |
| MÉDIO | `node/server.py:LoadStrip` | Reenvio de carga podia sobrescrever faixa/job existente sem verificar se o conteúdo correspondia. | Corrigido: carga idêntica é reconhecida; ID reutilizado para faixa diferente retorna conflito. |
| MÉDIO | `node/coordinator.py:process` | ID truncado a 8 hexadecimais aumentava risco de colisão entre jobs concorrentes. | Corrigido usando UUID completo. |
| MÉDIO | `node/coordinator.py:process` | Uma resposta atrasada após timeout pode continuar consumindo CPU no worker original. gRPC não oferece cancelamento confirmado do cálculo Python. | Mitigação: ID por partição isolada, um resultado agregado por faixa; custo duplicado ainda é possível. |
| MÉDIO | `node/server.py` e `node/coordinator.py` | Estado é volátil; falha do líder perde controle do job e a eleição não o retoma. | Não corrigido: exige persistência/replicação, fora da alteração mínima. |
| MÉDIO | `node/election.py:on_coordinator` | Sem termo/época, um anúncio de coordenador superior atrasado não é associado à eleição que o produziu. Heartbeat detecta ausência posterior, mas não impede toda discordância transitória. | Mitigação local para ID desconhecido/inferior; época requer mudança de protocolo e continua pendente. |
| MÉDIO | `node/election.py:_loop` | Uma falha de heartbeat atrasada podia apagar uma liderança atualizada em paralelo por `Coordinator`. | Corrigido sincronizando leitura/escrita do líder e verificando se a resposta refere-se ao mesmo líder consultado. |
| MÉDIO | `node/server.py:jobs` | Caches e faixas em memória podiam permanecer depois de jobs incompletos. | Mitigado com liberação de arrays em `GetLabels`, expiração preguiçosa após 1 hora em `LoadStrip`/heartbeat e remoção de concluídos sob pressão de 64 entradas; não há limite de bytes nem persistência. |
| MÉDIO | `client.py:main` | Resultado de rótulos era remodelado sem validar dimensão e tamanho; resposta inconsistente causaria exceção no cliente. | Corrigido com validação antes do `reshape`. |
| MÉDIO | `node/server.py:main` | Configuração ruim podia gerar `KeyError` ou IDs duplicados mascarados pelo dicionário; falha de bind não era verificada. | Corrigido com validação de IDs/hosts/portas e retorno do bind. |
| BAIXO | `test_stale.py` | ID fixo com dados aleatórios tornava uma segunda execução incompatível com carga idempotente; RPCs não tinham deadline. | Corrigido com UUID e timeout de 5 s; utilitário não foi executado. |
| BAIXO | `client.py` / `node/server.py` | Canais gRPC não eram fechados explicitamente ao final do cliente ou desligamento do nó. | Corrigido com fechamento no encerramento normal do fluxo. |
| BAIXO | `requirements.txt` e stubs | Requisitos não eram fixados, mas gRPC gerado requer `grpcio>=1.85.0` e código Protobuf valida runtime 7.35.1. | Corrigido com pins correspondentes e pins das bibliotecas numéricas. Compatibilidade de instalação não foi executada. |
| BAIXO | READMEs | Conteúdo estava duplicado e não explicava teoria, payloads, configuração externa, limites e recuperação. | Corrigido: README técnico canônico em `slic-dist/README.md`; raiz virou índice. |

### Avaliação estática do SLIC

Não foi identificado defeito matemático concreto que justificasse substituir `slic_core`. A distância implementada é quadrática: soma dos quadrados de diferenças Lab mais `(m/S)^2` vezes a distância espacial quadrática. A raiz não é calculada, o que preserva a ordem de comparação. Centros são inicializados em grade, atribuições são limitadas a janelas espaciais e os acumuladores somam Lab e coordenadas globais. A agregação por faixa é coerente com a atualização global dos centros. Isso não prova equivalência numérica com uma implementação externa; tal comparação não foi executada.

## 4. Correções realizadas

1. **Deadlines e recuperação de worker:** `coordinator.py` define timeout finito, identifica `DEADLINE_EXCEEDED`/`UNAVAILABLE` e tenta as faixas nos demais nós disponíveis para o job. Se trocar de nó durante uma iteração, recarrega a faixa e calcula novamente; se trocar durante coleta, restaura a última iteração antes de solicitar rótulos. Sem candidato restante, retorna erro controlado ao cliente.
2. **Identidade e cache:** UUID completo identifica o job; índice da faixa é incorporado ao `job_id` enviado ao worker, e a iteração segue como campo próprio. O coordenador registra estado/worker/tentativas/resultado por operação da partição. O worker usa lock por job e cache apenas da última iteração, cuja assinatura inclui centros, `step` e `compactness`; chamadas repetidas idênticas reutilizam a parcial. Uma repetição incompatível retorna conflito.
3. **Carga e coleta idempotentes:** faixa repetida com conteúdo igual recebe ACK; conteúdo divergente sob o mesmo ID é recusado. `GetLabels` mantém bytes do mapa para retry, devolvendo `NOT_FOUND` ou `FAILED_PRECONDITION` quando apropriado.
4. **Validação:** cliente e coordenador validam dimensões, quantidade de bytes RGB, parâmetros, topologia e dimensões do resultado. Worker valida limites positivos, tamanho e finitude de Lab, centros e parâmetros. A agregação valida continuidade, cobertura, largura e tamanho binário.
5. **Eleição:** ID de líder desconhecido é ignorado; nó vivo de ID maior não aceita anúncio de ID inferior e inicia eleição. Não foi adicionada época porque isso exigiria expandir os contratos Protobuf.
6. **Recursos e operação:** canais são fechados no final de cliente/servidor; o servidor verifica bind e valida a topologia; `test_stale.py` usa job único e deadlines; jobs liberam arrays grandes após materializar rótulos e possuem expiração preguiçosa via `LoadStrip`/heartbeat.
7. **Logs e instalação:** logs são CSV e sincronizam a obtenção do timestamp com a escrita; dependências foram fixadas conforme os stubs gerados.

## 5. Otimizações

- Evita recalcular iteração idêntica enquanto sua parcial mais recente está em cache.
- Mantém cache somente da última iteração, em vez de acumular parciais de todas as iterações do job.
- Após serializar os rótulos, libera arrays Lab, rótulos e somas parciais daquele job concluído.
- Limita a quantidade de workers por job ao número de linhas, evitando faixas vazias.
- Reutiliza canal gRPC por nó durante a tentativa do cliente e fecha canais ao terminar.
- Preserva `_par` e o fluxo em lote existente; não foram introduzidas dependências ou um novo framework de tarefas.

Os ganhos são de previsibilidade e memória de estado; não há medidas de desempenho que quantifiquem aceleração.

## 6. Arquivos modificados

| Arquivo | Alteração | Justificativa |
|---|---|---|
| `slic-dist/node/coordinator.py` | timeout, classificação de erro, reatribuição/refazer faixa, IDs completos, estados por operação e validação da agregação | Evitar espera indefinida e perda de parcial em falha de worker. |
| `slic-dist/node/server.py` | validação de config/payload, locks, cache idempotente, retenção/expiração, erros controlados e fechamento de canais/bind | Proteger estado concorrente e tornar RPCs repetidas/falhas previsíveis. |
| `slic-dist/client.py` | validação de topologia/imagem/resultado, canal reutilizado e fechamento, timeout de 180 s | Evitar envio inválido, erro de reshape e vazamento de canal. |
| `slic-dist/node/election.py` | validação de ID no anúncio do líder | Impedir aceitar IDs desconhecidos/inferiores sem eleição. |
| `slic-dist/node/lamport.py` | campos CSV opcionais e escrita protegida | Facilitar correlação de eventos concorrentes. |
| `slic-dist/test_stale.py` | UUID por execução e timeout nas duas RPCs | Permitir reutilização e evitar espera infinita no utilitário manual. |
| `slic-dist/requirements.txt` | pins gRPC/Protobuf/NumPy/SciPy/scikit-image/PyYAML | Reprodutibilidade compatível com metadados dos arquivos gerados. |
| `slic-dist/README.md` | documentação técnica completa, teoria, arquitetura, comandos, limites e Mermaid | Permitir instalação, compreensão e operação sem inventar resultados. |
| `README.md` | índice para README técnico e relatório | Evitar duas cópias extensas divergentes. |
| `docs/RELATORIO_TECNICO.md` | novo registro da auditoria, correções e estado | Separar relatório interno do artigo acadêmico. |

Não foram alterados `slic.proto`, arquivos gerados, `slic_core.py`, `config.yaml`, imagens ou logs.

## 7. Funcionalidades preservadas

Foram mantidos os contratos RPC e os nomes dos métodos, o formato RGB/Lab e dos rótulos, a arquitetura cliente-líder-workers, a divisão horizontal, o `ThreadPoolExecutor`, o algoritmo SLIC, o Bully/heartbeat, as operações `tick`/`send`/`receive` de Lamport e o utilitário para mensagens antigas. `slic_local()` continua disponível como referência sequencial.

## 8. Limitações restantes e riscos

- Uma falha do líder interrompe o job e perde estado volátil; eleição não significa recuperação de tarefas.
- Não existe termo de liderança no protobuf. Mensagens de IDs superiores atrasadas não são correlacionadas por geração/eleição.
- Timeout não cancela código NumPy/SciPy que já esteja executando no worker. Retry pode gerar computação duplicada, embora o líder aceite um resultado por partição.
- Workers usados na recuperação são apenas os que responderam ao heartbeat inicial; nós que retornam depois só participam de jobs futuros.
- O timeout fixo de 30 s pode ser curto para imagens grandes ou workers sobrecarregados; o limite deve ser avaliado em execução real.
- O cliente abandona a espera após 180 s, mas isso não cancela o job remoto.
- Estado é em memória. Limpeza é preguiçosa e baseada em uma hora/quantidade de jobs, sem quota por bytes; jobs ativos simultâneos podem consumir memória elevada.
- gRPC é sem TLS/autenticação e listener escuta nas interfaces da máquina. Firewall/rede confiável são necessários.
- Configuração e parsing atual não documentam suporte a IPv6 literal.
- Nenhum teste, benchmark, eleição real, falha de rede ou execução em múltiplos computadores foi reproduzido nesta auditoria.
- Não há relatório SBC dos integrantes, divisão de contribuição nem comparação sequencial/distribuída neste repositório.

## 9. Situação da implementação

| Funcionalidade | Situação |
|---|---|
| Contratos gRPC e serialização | IMPLEMENTADO; NÃO VALIDADO EM EXECUÇÃO nesta revisão. |
| SLIC distribuído, redução e conectividade | IMPLEMENTADO; equivalência/qualidade NÃO VALIDADA em execução. |
| Bully e heartbeat | IMPLEMENTADO PARCIALMENTE; sem termo e recuperação de estado. |
| Relógio lógico Lamport | IMPLEMENTADO; ordenação de logs não implica ordem total causal entre eventos concorrentes. |
| Timeout e tentativa de worker alternativo | IMPLEMENTADO PARCIALMENTE; depende de worker inicialmente ativo e não cancela cálculo antigo. |
| Idempotência de iteração e coleta | IMPLEMENTADO PARCIALMENTE; cache em memória e somente última iteração. |
| Recuperação após falha do líder | NÃO IMPLEMENTADO. |
| Réplicas/Two-Phase Commit | NÃO IMPLEMENTADO. |
| Comparação de desempenho | NÃO IMPLEMENTADO; `slic_local()` é apenas a referência disponível. |
| Segurança TLS/autenticação | NÃO IMPLEMENTADO. |

## 10. Conclusão e contexto acadêmico

A evolução preserva o desenho e os algoritmos existentes, preenchendo a recuperação de falhas no limite seguro permitido pelo protocolo atual. A primeira entrega pode documentar RPC, Lamport e Bully como mecanismos presentes, mas deve apresentar evidências reais do grupo para comunicação, paralelismo e falha. Para a segunda entrega, o grupo ainda deve validar tratamento de falhas sob execução e escolher uma expansão exigida (desempenho, réplicas ou consistência transacional). Este documento é auditoria interna; não substitui o artigo SBC de até quatro páginas da primeira entrega ou o relatório de até seis páginas da segunda.
