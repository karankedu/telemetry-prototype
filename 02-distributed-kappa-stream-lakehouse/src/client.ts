/**
 * Component 1 — SEGA Game Client Simulator
 *
 * Identical load-generation shape to the Phase 1 simulator: 5,000
 * pre-allocated players firing batched concurrent requests every 2 seconds.
 * Only the target URL (port 3001, the Kappa pipeline's server) differs.
 */

import { randomUUID } from 'crypto';

const SERVER_URL = process.env.SERVER_URL ?? 'http://localhost:3001/api/telemetry/events';
const PLAYER_COUNT = 5_000;
const BATCH_SIZE = 500;         // Max concurrent HTTP requests in flight at once
const FIRE_INTERVAL_MS = 2_000; // Fire a full round every 2 seconds

const CHARACTERS = ['Sonic', 'Tails', 'Knuckles', 'Amy', 'Shadow'] as const;
const ZONES = [
  'Green Hill Zone',
  'Marble Zone',
  'Spring Yard Zone',
  'Labyrinth Zone',
  'Star Light Zone',
  'Scrap Brain Zone',
] as const;
const EVENT_TYPES = [
  'GAME_START',
  'RING_COLLECTION',
  'BOSS_BATTLE',
  'GAME_OVER',
] as const;

interface PlayerState {
  playerId: string;
  character: string;
  zone: string;
  currentScore: number;
}

type TelemetryPayload = {
  playerId: string;
  timestamp: string;
  gameTitle: 'Sonic Jump';
  character: string;
  zone: string;
  ringsCollected: number;
  currentScore: number;
  eventType: string;
};

const pick = <T>(arr: readonly T[]): T =>
  arr[Math.floor(Math.random() * arr.length)];

// Pre-allocate all 5,000 player state objects once at startup.
// Avoids repeated heap allocations and GC pauses during sustained load.
const players: PlayerState[] = Array.from({ length: PLAYER_COUNT }, () => ({
  playerId: randomUUID(),
  character: pick(CHARACTERS),
  zone: pick(ZONES),
  currentScore: 0,
}));

function buildPayload(player: PlayerState): TelemetryPayload {
  const ringsCollected = Math.floor(Math.random() * 100);
  player.currentScore += ringsCollected * 10;

  // 5% chance of zone transition per event — simulates level progression
  if (Math.random() < 0.05) player.zone = pick(ZONES);

  return {
    playerId: player.playerId,
    timestamp: new Date().toISOString(),
    gameTitle: 'Sonic Jump',
    character: player.character,
    zone: player.zone,
    ringsCollected,
    currentScore: player.currentScore,
    eventType: pick(EVENT_TYPES),
  };
}

async function sendEvent(player: PlayerState): Promise<void> {
  const res = await fetch(SERVER_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(buildPayload(player)),
  });

  if (res.status !== 202) {
    console.warn(`[Client] Unexpected status ${res.status} for player ${player.playerId}`);
  }
}

async function fireRound(): Promise<{ sent: number; failed: number }> {
  let sent = 0;
  let failed = 0;

  // Chunk the full player list into BATCH_SIZE slices.
  // Each slice fires concurrently; we await before advancing to the next slice.
  // This caps peak in-flight requests and protects the client's socket pool.
  for (let i = 0; i < players.length; i += BATCH_SIZE) {
    const batch = players.slice(i, i + BATCH_SIZE);
    const results = await Promise.allSettled(batch.map(sendEvent));

    for (const r of results) {
      if (r.status === 'fulfilled') {
        sent++;
      } else {
        failed++;
        // Server may still be starting on first few rounds — suppress noise
      }
    }
  }

  return { sent, failed };
}

let roundInProgress = false;

async function scheduleRound(): Promise<void> {
  if (roundInProgress) {
    console.warn('[Client] Previous round still in flight — skipping tick');
    return;
  }

  roundInProgress = true;
  const t0 = performance.now();

  try {
    const { sent, failed } = await fireRound();
    const elapsedMs = performance.now() - t0;
    const rps = Math.round(sent / (elapsedMs / 1_000));

    console.log(
      `[Client] Round complete — sent: ${sent.toLocaleString()} | ` +
        `failed: ${failed} | elapsed: ${elapsedMs.toFixed(0)}ms | ~${rps.toLocaleString()} req/s`,
    );
  } finally {
    roundInProgress = false;
  }
}

console.log(`\n[Client] ─────────────────────────────────────────────`);
console.log(`[Client]  Sonic Jump Telemetry Simulator — STARTING`);
console.log(`[Client]  Players : ${PLAYER_COUNT.toLocaleString()} concurrent sessions`);
console.log(`[Client]  Target  : ${SERVER_URL}`);
console.log(`[Client]  Cadence : every ${FIRE_INTERVAL_MS / 1_000}s | batch size: ${BATCH_SIZE}`);
console.log(`[Client] ─────────────────────────────────────────────\n`);

// Fire the first round immediately, then every FIRE_INTERVAL_MS
scheduleRound();
setInterval(scheduleRound, FIRE_INTERVAL_MS);
