/**
 * Component 2 — Ingestion Backend API Server
 *
 * Write-Behind Caching: the POST handler fires a Redis LPUSH without awaiting
 * it, then instantly returns 202 Accepted. The event loop is never blocked by
 * disk I/O or slow DB queries — only by the tiny in-memory Redis enqueue.
 */

import Fastify from 'fastify';
import Redis from 'ioredis';

const REDIS_KEY = 'telemetry:events';
const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? '0.0.0.0';

interface TelemetryEvent {
  playerId: string;
  timestamp: string;
  gameTitle: string;
  character: string;
  zone: string;
  ringsCollected: number;
  currentScore: number;
  eventType: string;
}

const redis = new Redis({
  host: process.env.REDIS_HOST ?? 'localhost',
  port: Number(process.env.REDIS_PORT ?? 6379),
  lazyConnect: true,
  maxRetriesPerRequest: 3,
  enableReadyCheck: true,
});

redis.on('connect', () => console.log('[Server] Redis connected'));
redis.on('error', (err) => console.error('[Server] Redis error:', err.message));

const fastify = Fastify({ logger: false });

// Fastify JSON schema for input validation (uses ajv under the hood — zero overhead)
const telemetrySchema = {
  body: {
    type: 'object',
    required: ['playerId', 'timestamp', 'eventType'],
    additionalProperties: true,
    properties: {
      playerId: { type: 'string' },
      timestamp: { type: 'string' },
      gameTitle: { type: 'string' },
      character: { type: 'string' },
      zone: { type: 'string' },
      ringsCollected: { type: 'integer', minimum: 0 },
      currentScore: { type: 'integer', minimum: 0 },
      eventType: { type: 'string' },
    },
  },
} as const;

fastify.get('/health', async (_req, reply) => {
  return reply.code(200).send({ status: 'ok', uptime: process.uptime() });
});

fastify.post<{ Body: TelemetryEvent }>(
  '/api/telemetry/events',
  { schema: telemetrySchema },
  async (request, reply) => {
    // Non-blocking fire-and-forget: the LPUSH promise is not awaited.
    // Node.js returns to the event loop immediately after this line,
    // ready to accept the next request — this is the core of the
    // Write-Behind Cache pattern's throughput advantage.
    redis.lpush(REDIS_KEY, JSON.stringify(request.body)).catch((err: Error) =>
      console.error('[Server] Redis LPUSH failed:', err.message),
    );

    return reply.code(202).send({ status: 'accepted' });
  },
);

const start = async (): Promise<void> => {
  await redis.connect();
  await fastify.listen({ port: PORT, host: HOST });

  console.log(`\n[Server] ─────────────────────────────────────────────`);
  console.log(`[Server]  SEGA Telemetry Ingestion API — READY`);
  console.log(`[Server]  POST http://${HOST}:${PORT}/api/telemetry/events`);
  console.log(`[Server]  GET  http://${HOST}:${PORT}/health`);
  console.log(`[Server]  Write-Behind Cache → Redis key: "${REDIS_KEY}"`);
  console.log(`[Server] ─────────────────────────────────────────────\n`);
};

process.on('SIGTERM', async () => {
  console.log('[Server] SIGTERM — shutting down gracefully');
  await fastify.close();
  redis.disconnect();
  process.exit(0);
});

start().catch((err) => {
  console.error('[Server] Fatal startup error:', err);
  process.exit(1);
});
