import pg from 'pg';
import { config } from './config.js';
import { logger } from './logger.js';

const { Pool } = pg;

/**
 * A Pool, not a single Client.
 *
 * The original code used `new Client(...)` and called `.connect()` once at
 * module load. That gives you exactly one connection for the whole process:
 * every concurrent insert queues behind the previous one, and if the
 * connection drops the service is dead until restart. A Pool reconnects,
 * scales to PG_POOL_MAX, and surfaces errors per-checkout instead of
 * crashing the process.
 */
export const pool = new Pool({
  connectionString: config.postgres.connectionString,
  max: config.postgres.max,
  connectionTimeoutMillis: config.postgres.connectionTimeoutMillis,
  idleTimeoutMillis: config.postgres.idleTimeoutMillis,
  application_name: 'iot-grid-telemetry',
});

// An idle client erroring (network blip, server restart) must never take
// the process down — pg emits it on the pool, not on a query.
pool.on('error', (err) => {
  logger.error({ err }, 'idle postgres client error — pool will recover');
});

pool.on('connect', () => {
  logger.debug('postgres: new client connected');
});

/** Verify the database is reachable at boot. */
export async function checkDatabase(): Promise<boolean> {
  try {
    await pool.query('SELECT 1');
    return true;
  } catch (err) {
    logger.error({ err }, 'postgres health check failed');
    return false;
  }
}

export async function closePool(): Promise<void> {
  await pool.end();
  logger.info('postgres pool closed');
}
