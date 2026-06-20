# Functional Requirements: SEGA Mobile Game Telemetry Pipeline (e.g., Sonic Jump)

Act as an Expert Principal Game Backend Architect. I need you to build a complete, production-ready proof-of-concept for a high-throughput gaming telemetry ingestion pipeline using TypeScript and Node.js. 

Please architect this across a clean multi-file workspace directory structure using modern ES Modules syntax (`import/export`) and clean `async/await` patterns.

## System Architecture Requirements

### 1. Component 1: SEGA Game Client Simulator (`src/client.ts`)
- Act as a standalone simulation script generating real-time game telemetry load from 5,000 active concurrent players playing a SEGA title like *Sonic Jump*.
- Continuously fire concurrent JSON telemetry payloads via HTTP POST to the backend endpoint representing in-game actions (e.g., leveling up, collecting rings, or dying).
- **Gaming Payload Structure:**
  - `playerId`: UUID
  - `timestamp`: ISO string
  - `gameTitle`: "Sonic Jump"
  - `character`: String (e.g., "Sonic", "Tails", "Knuckles")
  - `zone`: String (e.g., "Green Hill Zone", "Marble Zone")
  - `ringsCollected`: Integer (randomized per event)
  - `currentScore`: Integer (incrementing over time)
  - `eventType`: String (e.g., "GAME_START", "RING_COLLECTION", "BOSS_BATTLE", "GAME_OVER")
- Use an efficient asynchronous looping mechanism (e.g., an unblocked async interval loop) to scale to massive concurrent traffic hits without bottlenecking memory on the client side.

### 2. Component 2: Ingestion Backend API Server (`src/server.ts`)
- Create a lightweight, high-performance HTTP server using Express or Fastify running a single-threaded Event Loop.
- Expose a POST endpoint at `/api/telemetry/events`.
- **Crucial Non-Blocking Performance Rule:** The endpoint route handler must be completely non-blocking to handle heavy concurrent spike loads (e.g., millions of players playing simultaneously). It should receive the incoming gameplay JSON payload, immediately execute an asynchronous push into a Redis List or Stream buffer using a performance-tuned Redis client (like `redis` or `ioredis`), and instantly return a `202 Accepted` status code to the client. It must NOT wait for disk operations or heavy database calculations before responding.

### 3. Component 3: Asynchronous Storage Drainer Worker (`src/worker.ts`)
- Create an independent, asynchronous background loop that triggers strictly every 10 seconds.
- It must execute atomic batch commands (like a pipeline executing `LRANGE` + `LTRIM` or utilizing a transaction `MULTI` block) to pull and flush all accumulated gaming events from the Redis cache buffer simultaneously. This ensures zero data loss during high-volume ingestion spikes.
- Stream and append the drained gameplay JSON batch into a local disk file (e.g., `data/game_telemetry_drain.json`) using Node’s asynchronous filesystem promises (`fs.promises.appendFile`). This represents archiving raw game data before batch-loading it into an analytics data lake.

## Deliverables Needed
- A clear workspace directory tree visualization.
- A fully functional `package.json` detailing modern dependencies (`typescript`, `@types/node`, `ts-node`, `redis` or `ioredis`, etc.).
- Complete, production-grade TypeScript source files for `client.ts`, `server.ts`, and `worker.ts`.
- A concise architectural summary highlighting how this specific "Write-Behind Caching" pattern perfectly isolates Node.js's main execution thread from heavy disk I/O bottlenecks during live gaming events.