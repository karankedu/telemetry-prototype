/**
 * Component 3 — Asynchronous Storage Drainer Worker
 *
 * Runs an independent async loop that fires every 10 seconds.
 * Uses a Redis MULTI/EXEC transaction to atomically:
 *   1. LRANGE — read every event currently buffered in the list
 *   2. DEL    — atomically clear the list in the same round-trip
 * Because EXEC holds the Redis lock during both commands, zero events are
 * lost or double-counted even under concurrent LPUSH traffic from the server.
 *
 * Drained events are appended to disk as NDJSON (one JSON object per line),
 * mirroring the format expected by analytics data-lake batch loaders.
 */

import Redis from 'ioredis';
import { promises as fs } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_FILE = join(__dirname, '..', 'data', 'game_telemetry_drain.json');
const REDIS_KEY = 'telemetry:events';
const DRAIN_INTERVAL_MS = 10_000; // 10-second drain cycle

const redis = new Redis({
  host: process.env.REDIS_HOST ?? 'localhost',
  port: Number(process.env.REDIS_PORT ?? 6379),
  lazyConnect: true,
  maxRetriesPerRequest: 3,
  enableReadyCheck: true,
});

redis.on('connect', () => console.log('[Worker] Redis connected'));
redis.on('error', (err) => console.error('[Worker] Redis error:', err.message));

async function ensureOutputDir(): Promise<void> {
  await fs.mkdir(dirname(OUTPUT_FILE), { recursive: true });
}

async function drainBuffer(): Promise<void> {
  // MULTI/EXEC: both commands execute atomically on the Redis server.
  // Any LPUSH from the server that arrives after EXEC starts is NOT included —
  // it lands in a fresh list and is picked up by the next drain cycle.
  const results = await redis
    .multi()
    .lrange(REDIS_KEY, 0, -1)   // Read entire event list
    .del(REDIS_KEY)              // Atomically clear it
    .exec();

  if (!results) {
    // null means a WATCH was broken — extremely unlikely without WATCH, but guard it
    console.warn('[Worker] MULTI/EXEC returned null — skipping this cycle');
    return;
  }

  const [lrangeResult, delResult] = results as [
    [Error | null, string[]],
    [Error | null, number],
  ];

  if (lrangeResult[0]) {
    console.error('[Worker] LRANGE error:', lrangeResult[0].message);
    return;
  }
  if (delResult[0]) {
    console.error('[Worker] DEL error:', delResult[0].message);
    return;
  }

  const events = lrangeResult[1];

  if (!events || events.length === 0) {
    console.log('[Worker] Buffer empty — nothing to drain');
    return;
  }

  // NDJSON: one JSON event per line. Supports streaming reads by analytics tools
  // without loading the entire file into memory (unlike a JSON array).
  const ndjsonBatch = events.join('\n') + '\n';

  await fs.appendFile(OUTPUT_FILE, ndjsonBatch, 'utf-8');

  const kb = (Buffer.byteLength(ndjsonBatch, 'utf-8') / 1024).toFixed(1);
  console.log(
    `[Worker] Drained ${events.length.toLocaleString()} events (${kb} KB) → ${OUTPUT_FILE}`,
  );
}

async function start(): Promise<void> {
  await redis.connect();
  await ensureOutputDir();

  console.log(`\n[Worker] ─────────────────────────────────────────────`);
  console.log(`[Worker]  Storage Drainer — ACTIVE`);
  console.log(`[Worker]  Source : Redis key "${REDIS_KEY}"`);
  console.log(`[Worker]  Sink   : ${OUTPUT_FILE}`);
  console.log(`[Worker]  Cycle  : every ${DRAIN_INTERVAL_MS / 1_000}s`);
  console.log(`[Worker] ─────────────────────────────────────────────\n`);

  // Drain immediately on startup to clear any backlog from previous runs
  await drainBuffer();
  setInterval(drainBuffer, DRAIN_INTERVAL_MS);
}

process.on('SIGTERM', async () => {
  console.log('[Worker] SIGTERM — running final drain before exit');
  await drainBuffer();
  redis.disconnect();
  process.exit(0);
});

start().catch((err) => {
  console.error('[Worker] Fatal startup error:', err);
  process.exit(1);
});
