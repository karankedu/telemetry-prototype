# Functional Requirements: SEGA Mobile Game Telemetry Pipeline — Phase 2 (Distributed Kappa)

Act as an Expert Principal Data Platform Architect. Evolve the Phase 1 Write-Behind Redis pipeline into an
enterprise-scale **Kappa architecture**: a single immutable, replayable log replaces the Redis buffer, and a
stream processor replaces the disk-drain worker.

## System Architecture Requirements

### 1. Component 1: SEGA Game Client Simulator (`src/client.ts`)
- Identical simulator to Phase 1 (5,000 concurrent *Sonic Jump* players, same payload shape), retargeted at
  the Phase 2 ingestion server.

### 2. Component 2: Ingestion Backend API Server (`src/server.ts`)
- Fastify server exposing `POST /api/telemetry/events`, same non-blocking contract as Phase 1: validate,
  fire-and-forget the write, return `202 Accepted` immediately.
- Replace the Redis `LPUSH` with a Kafka **produce** to an immutable, partitioned topic (`telemetry-events`),
  keyed by `playerId` so a single player's events stay strictly ordered on one partition.
- There is no separate drain worker in this architecture — Kafka's own log retention and replication are the
  durability layer.

### 3. Component 3: Spark Structured Streaming Consumer (`spark/stream_to_delta.py`)
- A PySpark Structured Streaming job that subscribes to the `telemetry-events` Kafka topic from the earliest
  offset.
- Parse the JSON payload against an explicit schema; rows that fail schema validation must be dropped, not
  crash the job.
- Continuously micro-batch and append the validated stream into a **Delta Lake** table on local disk, using
  checkpointed offsets for exactly-once, restart-safe processing.
- This explicitly eliminates the "small files problem": Delta's transaction log and file compaction give
  ACID, queryable columnar storage instead of raw NDJSON dumps.

### 4. Infrastructure
- Kafka and Spark must run in Docker (`docker-compose.yml`), since neither is an npm-installable dependency
  like Redis was. Node.js client/server still run on the host via `npm`.

## Deliverables Needed
- `docker-compose.yml` bringing up a single-broker Kafka (KRaft mode) and a Spark container.
- Complete TypeScript source for `client.ts` and `server.ts` (kafkajs-based producer).
- Complete PySpark source for `stream_to_delta.py`.
- A concise architectural summary of how the Kappa pattern (single log, replayable, no separate batch layer)
  differs from Phase 1's Write-Behind Cache (ephemeral buffer, scheduled drain).
