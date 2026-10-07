# Multi-region lab

Laboratório local para estudar multi-região ativo-ativo, failover, perda total de região e failback.
Cada região tem: serviço Go (hexagonal) + Postgres + Redpanda (Kafka API). O HAProxy simula Route53 / Global Accelerator.

```
              k6 (writers · readers · verifier)     Grafana ◄── Prometheus
                             │
                      ┌──────▼───────┐
                      │   HAProxy    │  :8080 ativo-ativo   :8090 failover
                      └───┬──────┬───┘
           ┌──────────────┘      └──────────────┐
    ┌──────▼──── região A ────┐          ┌──────▼──── região B ────┐
    │ service-a               │          │ service-b               │
    │ postgres-a              │          │ postgres-b              │
    │ redpanda-a [a.customers]│◄── WAN ──│ redpanda-b [b.customers]│
    └─────────────────────────┘── WAN ──►└─────────────────────────┘
```

---

## Início rápido

```bash
make tidy      # uma vez — resolve go.mod/go.sum
make up        # sobe tudo
make load      # terminal separado — carga contínua k6
make test      # testes unitários (sem Docker)
```

| URL | O que é |
|---|---|
| http://localhost:3000 | Grafana — dashboard "Multi-Region · Customers" |
| http://localhost:8080 | Router ativo-ativo |
| http://localhost:8090 | Router failover (A primária, B backup) |
| http://localhost:8404/stats | HAProxy stats + drain/maint manual |
| http://localhost:9090 | Prometheus |
| http://localhost:18081 | Região A direta (bypass router) |
| http://localhost:18082 | Região B direta (bypass router) |

Cenários de falha: `chaos/chaos.sh` sem argumentos lista todos os comandos.

---

## Arquitetura do serviço

- **CRUD** de `customer` com `id` UUID. `Idempotency-Key` no header vira o `id` → POST idempotente.
- **Outbox por status**: grava no banco (`PENDING`), publica no tópico da própria região, atualiza para `PUBLISHED` ou `PUBLISH_FAILED`. Se o Kafka cair, o relay (a cada 2 s) republica o backlog.
- **Replicação cross-region**: cada serviço consome o tópico da outra região e aplica com last-write-wins (`version` = timestamp µs, desempate pelo nome da região). Idempotente — duplicata não causa problema.
- **Health check**: `/health` retorna 503 quando o banco está fora. Com `HEALTH_REQUIRE_KAFKA=true` o Kafka local também derruba a região. O HAProxy usa isso para rotear.

---

## Fases

### Fase 1 · Ativo-ativo em regime normal

```
clients ──► HAProxy :8080
                ├──► service-a ──► postgres-a ──► redpanda-a ──► service-b (consome)
                └──► service-b ──► postgres-b ──► redpanda-b ──► service-a (consome)
```

**Objetivo:** entender a replicação assíncrona por eventos e o comportamento LWW.

```bash
make up && make load
# Grafana → "Replication visibility p95" — tempo até escrita de A aparecer em B
# Teste de conflito LWW:
curl -X POST http://localhost:18081/customers -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: same-id' -d '{"name":"A","address":"x","email":"a@a","nickname":"a"}'
curl -X POST http://localhost:18082/customers -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: same-id' -d '{"name":"B","address":"x","email":"b@b","nickname":"b"}'
# Quem venceu? Veja o campo `name` depois de alguns segundos em ambas as regiões.
```

---

### Fase 2 · Falha de componente

```
╔══ Kafka fora ══════════════════════════╗
║ service-a ──► postgres-a              ║
║      │                                ║
║      ▼ (PUBLISH_FAILED)               ║
║  relay drena quando Kafka volta        ║
╚════════════════════════════════════════╝

╔══ Banco fora ══════════════════════════╗
║ service-a ──► /health 503             ║
║ HAProxy remove A em ~2 s              ║
║ Todo tráfego vai para B               ║
╚════════════════════════════════════════╝
```

| Cenário | Comando | O que observar no Grafana |
|---|---|---|
| Kafka fora | `chaos.sh kafka-down a` | `PUBLISH_FAILED` sobe em A; B para de receber eventos de A |
| Kafka volta | `chaos.sh kafka-up a` | Relay drena backlog; lag de replicação pica e cai |
| Banco fora | `chaos.sh db-down a` | `dependency_up{db}` vai a 0; HAProxy tira A; erros no k6 só na janela de detecção |
| Banco volta | `chaos.sh db-up a` | A retorna ao pool após `rise` checks; relay e consumidor retomam |

---

### Fase 3 · Perda total de região e failback

```
╔══ Região A cai ══════════════════════╗       ╔══ Failback ══════════════════════════╗
║ HAProxy detecta (fall 2, ~2 s)       ║  ──►  ║ chaos.sh region-up a                ║
║ 100% do tráfego vai para B           ║       ║ A consome eventos de B (lag baixa)  ║
║ RPO = eventos ainda no Kafka de A    ║       ║ HAProxy reinsere A (rise 2)          ║
╚══════════════════════════════════════╝       ╚══════════════════════════════════════╝
```

```bash
chaos.sh region-down a    # derruba service-a + postgres-a + redpanda-a
# Grafana: erros k6 → janela de RTO; "replication timeouts" → RPO estimado

chaos.sh region-up a      # failback com dados preservados
# Grafana: lag de replicação baixa até zero conforme A consome o backlog de B
```

Failback controlado: em `http://localhost:8404/stats` coloque A em `DRAIN` (valide) e depois `READY`.

---

### Fase 4 · Partição de rede e perda de dados

```
╔══ Partição ══════════════════════════════════════════╗
║  região A ◄──── WAN cortada ────► região B          ║
║  Ambas aceitam escrita — dados divergem              ║
║                                                      ║
║  chaos.sh heal a → LWW reconcilia (skipped_stale)   ║
╚══════════════════════════════════════════════════════╝

╔══ Disaster (wipe + backfill) ════════════════════════╗
║  region-wipe a  →  A volta vazia                     ║
║  backfill a     →  cópia do banco de B para A        ║
║  reset-offsets a → replay do tópico de B             ║
╚══════════════════════════════════════════════════════╝
```

```bash
chaos.sh partition a      # corta WAN de A
# Escreva nas duas regiões; observe divergência no banco

chaos.sh heal a           # reconecta WAN
# Grafana: skipped_stale mostra quantas escritas foram descartadas pelo LWW

chaos.sh region-wipe a    # destrói A e seus volumes
chaos.sh region-up a
chaos.sh backfill a       # cópia do banco de B
chaos.sh reset-offsets a  # replay do tópico de B (dados originados em A não voltam)
```

---

### Fase 5 · Postgres streaming replication (ativo-passivo)

Arquitetura desta fase: **um único primário (postgres-a)**. postgres-b é réplica física que recebe WAL de A. Sem conflitos, sem LWW — só um lado escreve. Equivalente a Aurora Global Database.

```
              WAL stream (física)
postgres-a ──────────────────────────────► postgres-b (standby, read-only)
     │         pg_stat_replication              │
     │◄────────────────────────────────────────┘
     │
postgres-exporter-a ──► Prometheus ──► Grafana
                         (lag bytes · lag seconds · role · timeline)

service-a ──┐
            ├──► host=postgres-a,postgres-b  target_session_attrs=read-write
service-b ──┘         (pgx roteia para o primário automaticamente)
```

#### 5.1 · Replicação async — medir o RPO

```
escrita  ──►  postgres-a  ──WAL──►  postgres-b
                                    └── replay_lag_bytes / replay_lag_seconds
                                        (Grafana → Phase 5 → WAL replay lag)
```

```bash
make load
make pg-status   # pg_stat_replication ao vivo
```

RPO em bytes/segundos = o que seria perdido se postgres-a caísse agora.

---

#### 5.2 · Failover — derruba A, promova B

```
postgres-a  DOWN
                        pg_promote()
postgres-b ────────────────────────► postgres-b  PRIMARY  (timeline 2)
                                           │
                              pgx reconecta automaticamente
                         service-a e service-b passam a escrever em B
```

```bash
chaos.sh region-down a    # derruba service-a + postgres-a + redpanda-a
chaos.sh pg-failover b    # pg_promote() em postgres-b
# Grafana: postgres-b role → PRIMARY; timeline-b sobe para 2
```

---

#### 5.3 · Failback com pg_rewind

```
postgres-a  (divergida, timeline 1)
      │
      │   pg_rewind --source=postgres-b
      ▼
postgres-a  (rewound, sem divergência)  ──standby──►  postgres-b (primary)
```

```bash
chaos.sh region-up a      # sobe postgres-a (ainda divergida)
chaos.sh pg-rewind a      # desfaz divergência, A vira réplica de B
# Alternativa (mais lenta, reconstrói do zero):
chaos.sh pg-basebackup a
```

---

#### 5.4 · Switchover controlado — devolve liderança para A

```
postgres-b  PRIMARY (timeline 2)
      │
      ├── chaos.sh pg-rewind a    →  postgres-a vira réplica de B
      ├── chaos.sh pg-switchover a →  postgres-a promovida  (timeline 3)
      └── chaos.sh pg-rewind b    →  postgres-b vira réplica de A

resultado: postgres-a PRIMARY (timeline 3)  ·  postgres-b standby
           sem perda de dados
```

```bash
chaos.sh region-up a
chaos.sh pg-rewind a
chaos.sh pg-switchover a
chaos.sh pg-rewind b
```

---

#### 5.5 · Split brain — sem fencing

```
postgres-a  PRIMARY (timeline 1)  ◄── aceita escrita ──┐
                                                        │ dados divergem
postgres-b  PRIMARY (timeline 2)  ◄── aceita escrita ──┘

fix: chaos.sh pg-rewind a   OU   chaos.sh pg-basebackup a
```

```bash
chaos.sh pg-split-brain   # promove B com A ainda viva
# Grafana: dois primários com timelines diferentes
```

Sem fencing (STONITH, lease, epoch), o nó que "volta" pode sobrescrever dados do novo primário.

---

#### 5.6 · Sync replication — RPO = 0

```
                     commit só confirma quando B aplicou o WAL
client ──► postgres-a ──remote_apply──► postgres-b
                │                            │
                └── latência p95 de A sobe   │
                    A trava se B cair ───────┘
```

```bash
chaos.sh pg-sync-on     # synchronous_commit = remote_apply
# Grafana: WAL lag → 0; latência p95 de A sobe

chaos.sh db-down b      # postgres-b cai → escritas em A travam
chaos.sh db-up b
chaos.sh pg-sync-off    # volta para async
```

---

#### Comparativo Fases 1–4 vs Fase 5

| | Fases 1–4 | Fase 5 |
|---|---|---|
| Replicação | Kafka events + LWW | WAL streaming (física) |
| Conflitos | LWW resolve | Impossíveis (1 primário) |
| RPO async | lag do Kafka | bytes de WAL não replicados |
| RPO sync | — | 0 (`remote_apply`) |
| Failover | automático via HAProxy | manual (`pg_promote`) |
| Failback | automático (outbox relay) | `pg_rewind` ou `pg_basebackup` |
| Equivalente AWS | MSK Replicator + DynamoDB GT | Aurora Global Database |

---

## Mapa para a AWS

| Lab | AWS |
|---|---|
| HAProxy + health checks | Route53 (failover/latency routing) ou Global Accelerator |
| Postgres + eventos (fases 1–4) | DynamoDB Global Tables ou RDS + replicação própria |
| Postgres streaming replication (fase 5) | Aurora Global Database |
| Redpanda + consumidor cross-region | MSK + MSK Replicator |
| `chaos.sh region-down` | AWS FIS |
| Outbox por status | Transactional outbox + Lambda/Streams |

---

## Limites do laboratório

- A rede `wan` é Docker puro: sem latência nem perda de pacotes. Use Toxiproxy ou `tc netem` para simular WAN real.
- O relógio é o mesmo para as duas regiões (mesma máquina) — o LWW aqui é mais benigno que em produção com clock skew.
- O relay sem `SKIP LOCKED` publicaria em duplicata com múltiplas réplicas do serviço. Seguro (consumidor idempotente), mas não ideal.
