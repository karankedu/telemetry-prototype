# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Phase 2 of the telemetry blueprint: the same SEGA *Sonic Jump* telemetry workload as
[01-edge-ingress-redis-cache](../01-edge-ingress-redis-cache), re-architected as a **Kappa pipeline** —
an immutable, replayable Kafka log replaces the ephemeral Redis buffer, and Spark Structured Streaming
replaces the scheduled disk-drain worker, writing straight into a Delta Lake table.

Full architecture constraints are specified in `Requirements.MD` — read it before making structural changes.

---

## Current Build State (as of 2026-06-26)

### Completed and verified
- `docker-compose.yml` — Kafka (KRaft, 3 partitions) + Spark + Airflow
- `src/server.ts` — Fastify POST /api/telemetry/events, fire-and-forget KafkaJS produce, 202 Accepted
- `src/client.ts` — 5,000-player Sonic Jump simulator targeting port 3001
- `spark/stream_to_delta.py` — Spark Structured Streaming: Kafka → Delta Lake, checkpointed
- `spark/batch_aggregation.py` — PySpark batch job: reads Delta Lake, prints 4 aggregations
- `airflow/dags/telemetry_batch_dag.py` — Airflow DAG: DockerOperator triggers batch_aggregation.py
- `package.json`, `tsconfig.json`, `.gitignore`, `.env`

### End-to-end pipeline verified
- Client → Fastify (port 3001) → Kafka `telemetry-events` (3 partitions, keyed by playerId)
- Spark Structured Streaming → Delta Lake at `data/delta/telemetry_events/`
- Spark UI at http://localhost:4040
- Airflow UI at http://localhost:8080 (see credentials below)

### Not yet run
- Airflow `telemetry_batch_aggregation` DAG trigger (Airflow just came up — trigger manually)

---

## Key Decisions

### Docker Images
Bitnami pulled versioned free-tier tags from Docker Hub in late 2025. All services use official Apache images:
- `apache/kafka:3.7.0` — KRaft mode, `KAFKA_*` env prefix (not `KAFKA_CFG_*`)
- `apache/spark:3.5.1` — entrypoint overridden to `sleep infinity`; spark-submit run via `docker exec`
- `apache/airflow:2.9.0` — SequentialExecutor + SQLite (no extra DB container needed for demo)

### Spark `failOnDataLoss=false`
Added to the Kafka reader in `stream_to_delta.py`. Prevents job crash when Kafka broker restarts cause
offset divergence between the checkpoint and the broker's actual retained offsets.

### Checkpoint / Stale State
If `docker compose down -v` is run (deletes kafka_data volume), or the Kafka topic is recreated, delete
`data/checkpoints/` before restarting the streaming job — otherwise Spark will crash on offset mismatch.

### Airflow SequentialExecutor
`LocalExecutor` requires PostgreSQL/MySQL; `SequentialExecutor` works with the built-in SQLite DB. Fine
for a demo with a single-task DAG.

### Airflow DockerOperator — host path mounts
The DAG mounts `./spark` and `./data/delta` into the batch Spark container. These are host-absolute paths
read from the `HOST_PROJECT_DIR` env var (set in `.env`). If you move the project directory, update `.env`.

---

## How to Run the Full Stack

### 1. Start all infra
```bash
docker compose up -d
```
Kafka health-check passes (~15s), then Spark and Airflow start. Airflow takes ~2 min on first boot
(pip-installs `apache-airflow-providers-docker`).

### 2. Get Airflow admin password
```bash
docker logs telemetry-airflow | grep password
```
Open **http://localhost:8080** — login `admin` / `<password from logs>`.

### 3. Start the Fastify ingestion server (host terminal)
```bash
npm run server
```
Expected: `[Server] SEGA Telemetry Ingestion API — READY` on port 3001.

### 4. Run the client simulator
```bash
npm run client
```
Expected: `Round complete — sent: 5,000 | failed: 0`

### 5. Start the Spark Structured Streaming job
```bash
MSYS_NO_PATHCONV=1 docker exec telemetry-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-jobs/stream_to_delta.py
```
First run downloads JARs (~1 min). Spark UI: http://localhost:4040

### 6. Trigger the Airflow batch aggregation
- Open http://localhost:8080
- Find DAG `telemetry_batch_aggregation` → toggle ON → click **Trigger DAG ▶**
- Click the running task → **Log** tab → see the 4 aggregation tables printed to stdout

### 7. Teardown
```bash
docker compose down        # stops containers, retains kafka_data volume
docker compose down -v     # full reset — also deletes named volumes
```
If you run `down -v`, delete `data/checkpoints/` before restarting the streaming job.

---

## Commands Reference

```bash
docker compose up -d                 # start Kafka + Spark + Airflow
docker compose down                  # stop and remove containers
docker logs telemetry-airflow        # Airflow startup logs + admin password

npm install                          # install Node dependencies
npm run server                       # Fastify ingestion API on :3001
npm run client                       # 5,000-player simulator
npm run build                        # compile TypeScript → dist/
```

---

## Architecture

```
client.ts  --HTTP POST-->  server.ts  --produce-->  Kafka "telemetry-events" (3 partitions, keyed by playerId)
                                                              |
                                                              |  Structured Streaming micro-batches
                                                              v
                                          spark/stream_to_delta.py --append-->  ./data/delta/telemetry_events/
                                                                                          |
                                                              Airflow DAG (daily/@manual) |
                                                              DockerOperator              v
                                          spark/batch_aggregation.py  ----reads---->  Delta Lake
                                                              |
                                                              v
                                                    4 aggregations in Airflow task log
```

### Component summary
- **`src/client.ts`** — 5,000-player simulator, 500-per-batch, 2s cadence.
- **`src/server.ts`** — Fastify. Fire-and-forget `producer.send()`, 202 immediately. Keyed by `playerId`.
- **`spark/stream_to_delta.py`** — Reads `telemetry-events` (earliest, `failOnDataLoss=false`), parses
  JSON via `from_json` (null rows dropped), appends to Delta Lake with checkpoint for exactly-once restarts.
- **`spark/batch_aggregation.py`** — Batch reads the full Delta table; prints events-by-type,
  top-10-players-by-score, events-by-zone, rings-by-character.
- **`airflow/dags/telemetry_batch_dag.py`** — `@daily` DAG; one `DockerOperator` task that spins up
  `apache/spark:3.5.1`, bind-mounts `./spark` and `./data/delta`, runs spark-submit. Ivy JAR cache
  persisted in `ivy_cache` named volume so subsequent runs skip the download.

### Runtime notes
- Kafka reachable at `localhost:9094` from host (Node.js) and `telemetry-kafka:9092` from inside Docker (Spark).
- `data/delta/` and `data/checkpoints/` are bind-mounted into the Spark container at `/opt/spark-jobs/delta`
  and `/opt/spark-jobs/checkpoints`.
- `MSYS_NO_PATHCONV=1` is required on Windows/Git Bash before any `docker exec` with absolute Unix paths.
- `HOST_PROJECT_DIR` in `.env` must be the absolute Windows path using forward slashes
  (e.g. `C:/KK/code/telemetry/02-distributed-kappa-stream-lakehouse`).

### Why Kappa instead of Write-Behind Cache
Phase 1's Redis list is destroyed by the worker's `DEL` on every drain — data gone from the buffer once
drained. Kafka's topic is the system of record: nothing is deleted by a consumer, so the same log can be
replayed from any offset to backfill, recover from a bug in the Spark job, or feed a second consumer.

### Delta Lake vs. NDJSON drain file
Phase 1 appended raw NDJSON to a flat file — no schema enforcement, no compaction. Delta Lake's transaction
log gives ACID writes, the explicit schema in `stream_to_delta.py` rejects malformed rows at write time,
and the table is immediately queryable by columnar engines (e.g. Dremio) without a separate ETL.
