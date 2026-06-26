/**
 * Component 2 — Ingestion Backend API Server
 *
 * Same non-blocking ingress shape as the Phase 1 (Redis) pipeline, but the
 * sink is now an immutable Kafka log instead of an in-memory list: the POST
 * handler fires a Kafka produce without awaiting it, then instantly returns
 * 202 Accepted. Kafka's own replication/durability guarantees replace the
 * worker.ts drain step entirely — there is no separate flush process in the
 * Kappa architecture, Spark reads directly off the topic.
 */

import Fastify from 'fastify';
import { Kafka, Partitioners, type Producer } from 'kafkajs';

const KAFKA_TOPIC = 'telemetry-events';
const PORT = Number(process.env.PORT ?? 3001);
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

const kafka = new Kafka({
  clientId: 'telemetry-ingress-server',
  brokers: [process.env.KAFKA_BROKER ?? 'localhost:9094'],
  retry: { retries: 5 },
});

const producer: Producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  allowAutoTopicCreation: true,
});

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
    // Non-blocking fire-and-forget: the produce promise is not awaited.
    // Partitioning on playerId keeps every event for a given player in
    // strict order on the same partition, which Spark relies on downstream
    // for any per-player windowed aggregation.
    producer
      .send({
        topic: KAFKA_TOPIC,
        messages: [{ key: request.body.playerId, value: JSON.stringify(request.body) }],
      })
      .catch((err: Error) => console.error('[Server] Kafka produce failed:', err.message));

    return reply.code(202).send({ status: 'accepted' });
  },
);

const start = async (): Promise<void> => {
  await producer.connect();
  await fastify.listen({ port: PORT, host: HOST });

  console.log(`\n[Server] ─────────────────────────────────────────────`);
  console.log(`[Server]  SEGA Telemetry Ingestion API — READY`);
  console.log(`[Server]  POST http://${HOST}:${PORT}/api/telemetry/events`);
  console.log(`[Server]  GET  http://${HOST}:${PORT}/health`);
  console.log(`[Server]  Kappa Pipeline → Kafka topic: "${KAFKA_TOPIC}"`);
  console.log(`[Server] ─────────────────────────────────────────────\n`);
};

process.on('SIGTERM', async () => {
  console.log('[Server] SIGTERM — shutting down gracefully');
  await fastify.close();
  await producer.disconnect();
  process.exit(0);
});

start().catch((err) => {
  console.error('[Server] Fatal startup error:', err);
  process.exit(1);
});
