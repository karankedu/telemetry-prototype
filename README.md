# Telemetry Prototype Platform

A multi-stage architectural blueprint for high-throughput telemetry ingestion pipelines, demonstrated using a SEGA *Sonic Jump* mobile game as the workload. The repository evolves from a lightweight edge-caching model to a fully distributed stream lakehouse, with each phase production-ready and independently runnable.

---

## Architecture Overview

```
Phase 1 — Write-Behind Edge Cache
┌──────────┐   HTTP POST    ┌─────────────┐  LPUSH (fire & forget)  ┌───────┐
│ client.ts│ ─────────────► │ server.ts   │ ───────────────────────► │ Redis │
│ 5k players│               │ Fastify :3000│                          └───┬───┘
└──────────┘                └─────────────┘                              │ drain every 10s
                                                                          ▼
                                                                   NDJSON on disk

Phase 2 — Distributed Kappa Stream Lakehouse
┌──────────┐   HTTP POST    ┌─────────────┐   produce (fire & forget)  ┌────────────────────┐
│ client.ts│ ─────────────► │ server.ts   │ ──────────────────────────► │ Apache Kafka       │
│ 5k players│               │ Fastify :3001│                             │ telemetry-events   │
└──────────┘                └─────────────┘                             │ 3 partitions       │
                                                                         └─────────┬──────────┘
                                                                                   │ Structured Streaming
                                                                                   ▼
                                                                    ┌──────────────────────────┐
                                                                    │ Spark stream_to_delta.py  │
                                                                    │ micro-batch → Delta Lake  │
                                                                    └──────────────┬───────────┘
                                                                                   │
                                                            Airflow DAG (daily)    │
                                                            DockerOperator         ▼
                                                                    ┌──────────────────────────┐
                                                                    │ Spark batch_aggregation.py│
                                                                    │ 4 aggregations → task log │
                                                                    └──────────────────────────┘
```

---

## Projects

### Phase 1 — [`01-edge-ingress-redis-cache`](./01-edge-ingress-redis-cache)

| Concern | Choice |
|---|---|
| Ingress | Node.js / Fastify |
| Buffer | Redis (`LPUSH` fire-and-forget, 202 immediately) |
| Drain worker | TypeScript worker, `MULTI/EXEC LRANGE+DEL` every 10s |
| Storage | NDJSON appended to local disk |
| Infra | Docker Compose (Redis only) |

**Pattern:** Write-Behind Cache — the API never waits for a storage write. Redis absorbs bursts; the drain worker flushes to disk on a schedule. Simple, low-latency, and operationally lightweight.

**Limitation:** Redis `DEL` destroys the buffer on every drain. No replay, no fault tolerance, no schema enforcement.

---

### Phase 2 — [`02-distributed-kappa-stream-lakehouse`](./02-distributed-kappa-stream-lakehouse)

| Concern | Choice |
|---|---|
| Ingress | Node.js / Fastify |
| Log | Apache Kafka (KRaft, single broker, 3 partitions, keyed by `playerId`) |
| Stream processor | Apache Spark Structured Streaming → Delta Lake |
| Batch analytics | PySpark batch job, triggered by Apache Airflow `DockerOperator` |
| Storage | Delta Lake (ACID, columnar, Parquet + transaction log) |
| Infra | Docker Compose (Kafka + Spark + Airflow) |
| UIs | Spark UI `:4040` · Airflow UI `:8080` |

**Pattern:** Kappa Architecture — a single immutable Kafka log replaces both the Redis buffer and any separate batch tier. The same topic can be replayed from any offset to backfill, fix bugs, or feed multiple consumers simultaneously.

**Why Delta Lake over NDJSON:** ACID writes, schema enforcement at ingest time, columnar Parquet storage, and a transaction log that makes the table immediately queryable by engines like Dremio without a separate ETL step.

---

## Quick Start

### Phase 1

```bash
cd 01-edge-ingress-redis-cache
docker compose up -d        # Redis
npm install
npm run server              # Fastify on :3000
npm run client              # 5,000-player simulator
```

### Phase 2

```bash
cd 02-distributed-kappa-stream-lakehouse
cp .env.example .env        # set HOST_PROJECT_DIR to your absolute path
docker compose up -d        # Kafka + Spark + Airflow (~2 min first boot)

npm install
npm run server              # Fastify on :3001

# Start Spark Structured Streaming job
docker exec telemetry-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-jobs/stream_to_delta.py

npm run client              # generate load → events flow to Kafka → Delta Lake
```

Open **http://localhost:4040** for the Spark Streaming UI.  
Open **http://localhost:8080** for the Airflow UI (credentials in `docker logs telemetry-airflow | grep password`).  
Trigger the `telemetry_batch_aggregation` DAG to run the PySpark aggregation job.

---

## Repository Structure

```
telemetry/
├── README.md
├── 01-edge-ingress-redis-cache/
│   ├── docker-compose.yml          # Redis
│   ├── src/
│   │   ├── server.ts               # Fastify ingestion API
│   │   ├── client.ts               # 5,000-player simulator
│   │   └── worker.ts               # Redis drain worker
│   └── CLAUDE.md
└── 02-distributed-kappa-stream-lakehouse/
    ├── docker-compose.yml          # Kafka + Spark + Airflow
    ├── src/
    │   ├── server.ts               # Fastify ingestion API (Kafka producer)
    │   └── client.ts               # 5,000-player simulator
    ├── spark/
    │   ├── stream_to_delta.py      # Spark Structured Streaming job
    │   └── batch_aggregation.py    # PySpark batch aggregation job
    ├── airflow/
    │   └── dags/
    │       └── telemetry_batch_dag.py   # Airflow DAG (DockerOperator)
    └── CLAUDE.md
```

---

## Tech Stack

| Layer | Phase 1 | Phase 2 |
|---|---|---|
| Runtime | Node.js 22 + TypeScript | Node.js 22 + TypeScript |
| HTTP framework | Fastify | Fastify |
| Kafka client | — | KafkaJS |
| Message broker | Redis 7 | Apache Kafka 3.7 (KRaft) |
| Stream processing | — | Apache Spark 3.5.1 (PySpark) |
| Storage format | NDJSON | Delta Lake (Parquet) |
| Orchestration | — | Apache Airflow 2.9 |
| Infra | Docker Compose | Docker Compose |

---

## Performance (Phase 2, local Docker Desktop)

- **~1,500 HTTP req/s** sustained through the Fastify ingestion layer
- **5,000 concurrent simulated players**, 500-per-batch `Promise.allSettled`
- **0 failed requests** across all load test rounds
- Kafka topic partitioned by `playerId` for per-player ordering guarantees
- Spark writes Snappy-compressed Parquet files, one set per partition per micro-batch
