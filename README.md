# Telemetry Prototype Platform: Architectural Evolution

This repository serves as a multi-stage architectural blueprint for scaling high-throughput telemetry ingestion pipelines (such as live mobile gaming or real-time streaming event monitoring). It transitions from a lightweight edge-caching model to a fully distributed cloud-scale data lakehouse.

## Projects Matrix

| Sub-Project | Architecture Pattern | Ingress Tier | Stream / Buffer Tier | Compute / Processing | Storage Target |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **[01-edge-ingress-redis-cache](./01-edge-ingress-redis-cache)** | Write-Behind Edge Cache | Node.js (Fastify) | Redis (In-Memory Hash) | TypeScript Worker | Local Storage / S3 |
| **[02-distributed-kappa-stream-lakehouse](./02-distributed-kappa-stream-lakehouse)** | Distributed Kappa Pipeline | Node.js (Fastify) | Apache Kafka (Distributed Log) | Apache Spark Streaming | Delta Lake (ACID Columnar) |

---

## Architectural Rationale

### Phase 1: Solving Network I/O Bottlenecks
The first project leverages the asynchronous, non-blocking single-threaded event loop of **Node.js (Fastify)** to maximize concurrent edge connections from client devices. By immediately buffering incoming payloads into **Redis**, we shield downstream layers from atomic write overhead and decouple network ingestion from storage engine physics.

### Phase 2: Scaling for Enterprise Resiliency & Analytics
The second project evolves the architecture to handle enterprise-scale durability and stateful analytics. By replacing Redis with **Apache Kafka**, we create an immutable, fault-tolerant message backbone. 

**Apache Spark Structured Streaming** continuously processes these streams in micro-batches, enforcing data schemas and writing them into **Delta Lake** formats. This explicitly eliminates the "small files problem" that traditionally chokes analytical workloads, paving the way for low-latency semantic query virtualization via engines like **Dremio**.
