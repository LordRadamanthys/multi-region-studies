# Multi-region lab: roadmap de estudo

Laboratório local para estudar multi-região **ativo-ativo**, failover, perda total de região e failback.
Cada região tem as mesmas peças (serviço Go + Postgres + Redpanda/Kafka). O HAProxy faz o papel de Route53 / Global Accelerator.

```
                    k6 (carga + verifier)          Grafana (A | B)  <-  Prometheus
                           │
                    ┌──────▼───────┐
                    │   HAProxy    │  :8080 ativo-ativo   :8090 failover (A primária, B backup)
                    └───┬──────┬───┘
         ┌──────────────┘      └──────────────┐
  ┌──────▼────────── região A ─┐        ┌─────▼────────── região B ─┐
  │ service-a (hexagonal)      │        │ service-b (hexagonal)     │
  │ postgres-a                 │        │ postgres-b                │
  │ redpanda-a  [a.customers]  │◄──WAN──┤ redpanda-b  [b.customers] │
  └────────────────────────────┘──WAN──►└───────────────────────────┘
        cada serviço consome o tópico da OUTRA região e aplica no seu banco (papel do MM2 / MSK Replicator)
```

## Como rodar

```bash
make tidy     # uma vez, resolve go.mod/go.sum (precisa de rede)
make up       # sobe as duas regiões, router, Prometheus e Grafana
make load     # em outro terminal: carga contínua do k6
open http://localhost:3000   # dashboard "Multi-Region · Customers"
make test     # testes unitários do núcleo (sem Docker)
```

Portas: router 8080 (ativo-ativo) e 8090 (failover), stats do HAProxy 8404, região A direta 18081, região B direta 18082, Prometheus 9090, Grafana 3000.
Cenários de falha: `chaos/chaos.sh <ação> <a|b>` (rode sem argumentos para ver a lista).

## Como o serviço funciona

- **CRUD** de `customer` (`name`, `address`, `email`, `nickname`) com `id` UUID. O header `Idempotency-Key` vira o `id` e torna o POST idempotente.
- **Status no banco** (`PENDING` → `PUBLISHED` ou `PUBLISH_FAILED`). A linha é o outbox: grava no banco, tenta publicar no tópico **da própria região** e atualiza o status. Se o Kafka estiver fora, a escrita continua funcionando e o *relay* (a cada 2s) republica o que não está `PUBLISHED`.
- **Replicação entre regiões** por eventos: cada serviço consome `<outra região>.customers` e aplica com *last-write-wins* (`version` = timestamp em µs, desempate pela região). É idempotente, então duplicata e reentrega não fazem mal. Linhas replicadas entram como `PUBLISHED`, o que evita loop de replicação.
- **`/health`** devolve 503 quando o **banco** está fora. Com `HEALTH_REQUIRE_KAFKA=true` o Kafka local também derruba a região. É isso que o router usa para decidir.
- **Hexagonal**: `domain` (regras) → `ports` (contratos) → `application` (casos de uso) → `adapters/in` (HTTP, replicator Kafka) e `adapters/out` (Postgres, publisher Kafka, métricas). O núcleo é testado só com fakes.

## Sua dúvida: banco da região A fora, grava na B mas publica no Kafka da A?

**Não. A região que fez o commit é a que publica.** O motivo:

1. Se o serviço A gravasse no banco de B e publicasse no Kafka de A, o dado e o evento viveriam em regiões diferentes. Se a A cair depois, o evento fica preso no Kafka dela e o banco de B tem uma linha cujo evento ninguém publicou.
2. O `origin_region` e o consumidor de B passariam a depender de um evento que volta para B, que já tem a linha.
3. O outbox só garante atomicidade quando banco e status estão no mesmo lugar.

O que fazer em vez disso, em ordem de recomendação:

- **Padrão (já implementado):** banco da A fora → `/health` da A dá 503 → o router manda a requisição **inteira** para B. B grava no banco B e publica no Kafka B. A unidade de failover é a região.
- **Degradação parcial (fase 5, para experimentar):** se você quiser que A continue "viva" com o banco fora, o serviço A **encaminha a requisição por HTTP** para o serviço B (nunca fala direto com o banco B). B faz banco + Kafka B, com `origin_region=b`. Mesmo resultado, só que a decisão fica no serviço e não no router.
- **Kafka fora com banco de pé:** não troca de região. Grava local, status `PUBLISH_FAILED`, o relay republica quando o Kafka voltar. Veja o painel "customers by publish status".

## Fases

Marque cada item quando conseguir explicar o que viu no dashboard.

### Fase 0 · Fundação (pronta)
- [x] Compose com duas regiões, redes `region-a`, `region-b`, `wan`, `edge`
- [x] Serviço Go hexagonal, outbox por status, replicação cross-region
- [x] HAProxy ativo-ativo (:8080) e failover (:8090)
- [x] k6 (writers, readers, verifier) e dashboard Grafana por região

### Fase 1 · Ativo-ativo em regime normal
- [ ] `make up && make load`. As duas regiões recebem ~metade do tráfego cada
- [ ] Observe "Replication visibility p95" (quanto tempo uma escrita da A leva para aparecer na B)
- [ ] Escreva o mesmo `id` nas duas regiões quase ao mesmo tempo (`curl` direto em 18081 e 18082) e veja quem vence (LWW) e o status das linhas
- Pergunta: o relógio é confiável para LWW? O que muda com clock skew entre regiões reais?

### Fase 2 · Falha de componente
| Cenário | Comando | O que esperar | Pergunta |
|---|---|---|---|
| Kafka local fora | `chaos.sh kafka-down a` | Escritas em A seguem OK; `PUBLISH_FAILED` sobe; B deixa de receber os eventos de A | Qual é o RPO de A para B agora? |
| Kafka volta | `chaos.sh kafka-up a` | O relay drena o backlog; lag de replicação sobe e depois cai | Por que a ordem por cliente se mantém? (chave = id) |
| Banco fora | `chaos.sh db-down a` | `/health` 503, router tira A em ~2-3s, falhas no k6 só nessa janela | Quanto dura a janela e o que a controla (`inter`, `fall`)? |
| Banco volta | `chaos.sh db-up a` | A volta para o router após `rise`; relay e consumidor retomam | O consumidor perdeu algo enquanto o banco estava fora? |
| `HEALTH_REQUIRE_KAFKA=true` | recriar serviços com a variável | Kafka fora agora derruba a região inteira | Qual dos dois comportamentos é melhor para pagamentos? |

### Fase 3 · Perda total de região e failback
- [ ] `chaos.sh region-down a`: tudo vai para B. Meça o RTO (janela de erros no k6) e o RPO (escritas de A que não chegaram em B: veja `replication timeouts`)
- [ ] `chaos.sh region-up a` (failback com dados preservados): A volta, consome o que B escreveu enquanto ela estava fora. Observe o lag baixar até zero
- [ ] Compare as portas 8080 (ativo-ativo) e 8090 (A primária, B backup: o failback é automático)
- [ ] Failback controlado: no `http://localhost:8404/stats` coloque A em `DRAIN`, valide, depois `READY`
- Pergunta: faz sentido failback automático em produção? O que você validaria antes de devolver tráfego?

### Fase 4 · Partição de rede (split brain) e perda com dados
- [ ] `chaos.sh partition a`: A e B continuam vivas e aceitando escrita, mas não se falam. Os dois lados divergem
- [ ] `chaos.sh heal a`: veja a convergência por LWW, o lag e quantas escritas foram sobrescritas (`skipped_stale`)
- [ ] `chaos.sh region-wipe a` → `region-up a`: A volta **vazia**. O consumidor de A só recebe eventos novos de B, porque o offset do grupo vive no cluster de B
- [ ] `chaos.sh backfill a` (cópia do banco de B) e/ou `chaos.sh reset-offsets a` (replay do tópico de B)
- Pergunta: por que o replay do tópico não reconstrói as linhas que **A originou**? (o histórico de A estava no Kafka de A, que foi destruído) O que resolveria? (snapshot, replicação lógica, retenção infinita)

### Fase 5 · Experimentos avançados
- [ ] **Postgres primary/replica de verdade**: streaming replication A → B, `pg_ctl promote`, `pg_rewind` no failback (ativo-passivo clássico, equivalente a Aurora Global)
- [ ] **Patroni + etcd** para failover automático do banco dentro da região
- [ ] **Degradação parcial**: serviço A encaminha para B por HTTP quando o banco local está fora (ver a dúvida acima)
- [ ] **Latência WAN** com Toxiproxy ou `tc netem` entre os serviços e o Kafka remoto, para ver o lag crescer sem partição
- [ ] **CockroachDB** com `--locality=region=a|b` no lugar de Postgres + eventos, e comparar com a abordagem manual
- [ ] **MirrorMaker 2** real no lugar do consumidor (prefixo de tópico, tradução de offsets no failover)
- [ ] **Chaos aleatório**: script que derruba e levanta componentes em loop enquanto o verifier checa a consistência final

## Mapa para a AWS

| Aqui | Na AWS |
|---|---|
| HAProxy + health checks | Route53 (failover / latency routing) ou Global Accelerator |
| Postgres por região + eventos | Aurora Global Database, DynamoDB Global Tables, ou RDS + replicação própria |
| Redpanda + consumidor cross-region | MSK + MSK Replicator, ou SNS/SQS cross-region |
| `chaos.sh region-down` | AWS FIS / simulação de região indisponível |
| Outbox por status | Outbox + Lambda/Stream, ou transactional outbox no DynamoDB |

## Notas e limites do laboratório

- A "WAN" é só uma rede Docker: sem latência nem perda de pacote até a Fase 5.
- O relógio é o mesmo para as duas regiões (mesma máquina), então o LWW aqui é mais benigno que na vida real.
- Com várias réplicas do serviço na mesma região o relay publicaria em duplicata (sem `SKIP LOCKED`). Isso é seguro porque o consumidor é idempotente, mas vale como exercício.
- `go.sum` vem do `make tidy` rodado na sua máquina. Se atualizar dependências, ajuste a versão do Go na imagem do `Dockerfile` para acompanhar o `go` do `go.mod`.
