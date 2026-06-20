# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A proof-of-concept high-throughput telemetry ingestion pipeline for a SEGA mobile game (Sonic Jump), demonstrating the **Write-Behind Caching** pattern: an HTTP server accepts events and buffers them in Redis without blocking on disk I/O, while a separate worker process drains that buffer to disk on a fixed schedule.

Full architecture constraints are specified in `Requirements.MD` — read it before making structural changes, since the three components (client/server/worker) were built to satisfy specific requirements there (exact payload shape, 10-second drain cadence, 202 Accepted non-blocking response, etc.).

## Commands

```bash
npm install                # install dependencies

npm run server              # start the Fastify ingestion API (src/server.ts) on :3000
npm run worker              # start the Redis -> disk drainer (src/worker.ts)
npm run client               # start the 5,000-player simulator (src/client.ts)

npm run build                # compile TypeScript to dist/ via tsc
npm run start:server         # run compiled dist/server.js
npm run start:worker         # run compiled dist/worker.js
npm run start:client         # run compiled dist/client.js
```

There is no test suite, lint config, or single-test command in this project.

Dev scripts run TypeScript directly via `tsx` (not `ts-node`, despite `ts-node` being listed as a devDependency per the original requirements — `tsx` is what's actually used because `ts-node`'s ESM support is unreliable).

### Runtime dependencies for local execution

- **Redis** must be running and reachable at `REDIS_HOST`/`REDIS_PORT` (default `localhost:6379`) before starting `server` or `worker`. Quickest way: `docker run -d --name telemetry-redis -p 6379:6379 redis:7-alpine`.
- **Node.js 18+** is required (native `fetch` is used in `client.ts`).
- On Windows, if `node`/`npm` aren't found in a PowerShell session right after install, the session's `PATH` needs a manual reload from the registry — it isn't always picked up automatically.

## Architecture

Three independent Node.js processes, decoupled through a Redis list, not through direct calls to each other:

```
client.ts  --HTTP POST-->  server.ts  --LPUSH-->  Redis list "telemetry:events"
                                                          |
                                                          |  MULTI/EXEC every 10s
                                                          v
                                              worker.ts --appendFile-->  data/game_telemetry_drain.json
```

- **`src/client.ts`** — Simulates 5,000 concurrent players. Player state objects (UUID, character, zone, running score) are pre-allocated once at startup to avoid GC churn under load. Each round chunks all 5,000 players into batches of 500 concurrent requests (`Promise.allSettled`) rather than firing all 5,000 at once, to cap in-flight sockets. A round-in-progress guard (`roundInProgress`) skips a tick rather than overlapping rounds if the server is slow.

- **`src/server.ts`** — Fastify server exposing `POST /api/telemetry/events`. The handler's defining property: it calls `redis.lpush(...)` **without awaiting it**, then immediately returns `202 Accepted`. This fire-and-forget is the core of the Write-Behind pattern — the event loop is never held up by Redis or disk latency. JSON Schema validation on the route (via Fastify's built-in ajv integration) is the only synchronous work done in the handler. `GET /health` exists for liveness checks.

- **`src/worker.ts`** — Runs on its own `setInterval` (10s), fully decoupled from the server process. Each cycle issues a Redis `MULTI` transaction combining `LRANGE telemetry:events 0 -1` (read everything) and `DEL telemetry:events` (clear it) so the read+clear is atomic — no events pushed mid-transaction are lost, since `EXEC` is the only point where both commands run, and anything pushed after `EXEC` starts simply lands in the next cycle's read. Drained events are appended as NDJSON (one JSON object per line, not a JSON array) to `data/game_telemetry_drain.json`, matching the line-oriented format typical analytics/data-lake batch loaders expect.

Because the three processes only communicate through the Redis list, they can be started/stopped/restarted independently and in any order — the list buffers whatever arrives while the worker isn't running, and the server doesn't care whether the worker is up.

### Why fire-and-forget instead of awaiting the Redis push

Awaiting `lpush` would still be fast (Redis is in-memory), but the explicit non-blocking pattern here is intentional per `Requirements.MD`'s framing: the rule is that the request handler must not wait on anything other than the instant enqueue, so that even a transient Redis slowdown doesn't add latency to the client-facing response. Errors from the fire-and-forget push are caught and logged, not surfaced to the client.

### Data format note

`data/game_telemetry_drain.json` despite the `.json` extension is NDJSON (newline-delimited), not a single parseable JSON document — each line is independently `JSON.parse`-able, the file as a whole is not.
