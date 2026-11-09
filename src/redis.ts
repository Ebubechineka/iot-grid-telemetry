import IORedis from 'ioredis';
import { config } from './config.js';
import { logger } from './logger.js';

/**
 * Redis holds the *live* reading for each node — the value the dashboard
 * reads on every refresh. History lives in Postgres.
 *
 * Two behaviours the original code lacked:
 *  - a TTL on every live key, so a node that stops reporting disappears
 *    instead of showing a frozen voltage forever;
 *  - reconnect handling, so a Redis restart doesn't kill ingestion.
 */
export const redis = new IORedis(config.redis.url, {
  maxRetriesPerRequest: 3,
  enableReadyCheck: true,
  retryStrategy(times) {
    const delay = Math.min(times * 200, 5000);
    logger.warn({ attempt: times, delayMs: delay }, 'redis reconnecting');
    return delay;
  },
  reconnectOnError(err) {
    // Reconnect on READONLY (failover) and on connection resets.
    return /READONLY|ECONNRESET|ETIMEDOUT/.test(err.message);
  },
  lazyConnect: false,
});

redis.on('connect', () => logger.info('redis: connected'));
redis.on('ready', () => logger.info('redis: ready'));
redis.on('error', (err) => logger.error({ err }, 'redis error'));
redis.on('close', () => logger.warn('redis: connection closed'));
redis.on('reconnecting', () => logger.warn('redis: reconnecting'));

export const liveKey = (nodeId: string): string => `node:live:${nodeId}`;

export interface LiveReading {
  nodeId: string;
  voltage: number;
  currentAmps: number;
  timestamp: number;
}

/**
 * Write the live reading with a TTL. HSET + EXPIRE in one round trip via
 * a pipeline — the original did a bare hmset with no expiry, so stale
 * nodes lingered indefinitely.
 */
export async function setLiveReading(reading: LiveReading): Promise<void> {
  const key = liveKey(reading.nodeId);
  await redis
    .multi()
    .hset(key, {
      v: reading.voltage,
      a: reading.currentAmps,
      t: reading.timestamp,
    })
    .expire(key, config.redis.liveTtlSeconds)
    .exec();
}

/** Read every live node in one pipeline. */
export async function getLiveReadings(nodeIds: string[]): Promise<Map<string, LiveReading>> {
  const out = new Map<string, LiveReading>();
  if (nodeIds.length === 0) return out;

  const pipe = redis.pipeline();
  for (const id of nodeIds) pipe.hgetall(liveKey(id));
  const results = await pipe.exec();

  if (!results) return out;
  results.forEach(([err, value], i) => {
    const nodeId = nodeIds[i];
    if (err || !nodeId || !value || typeof value !== 'object') return;
    const h = value as Record<string, string>;
    if (!h.v || !h.a || !h.t) return;
    out.set(nodeId, {
      nodeId,
      voltage: Number(h.v),
      currentAmps: Number(h.a),
      timestamp: Number(h.t),
    });
  });
  return out;
}

export async function checkRedis(): Promise<boolean> {
  try {
    const pong = await redis.ping();
    return pong === 'PONG';
  } catch (err) {
    logger.error({ err }, 'redis health check failed');
    return false;
  }
}

export async function closeRedis(): Promise<void> {
  await redis.quit().catch(() => redis.disconnect());
  logger.info('redis connection closed');
}
