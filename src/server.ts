import { config } from './config.js';
import { logger } from './logger.js';
import { checkDatabase, closePool } from './db.js';
import { checkRedis, closeRedis } from './redis.js';
import { TelemetryWriter } from './writer.js';
import { AlertManager } from './alerts.js';
import { Ingestor } from './ingestor.js';
import { buildServer } from './api.js';

/**
 * Process entrypoint.
 *
 * Boot order matters: verify Postgres and Redis before subscribing to MQTT,
 * otherwise the first messages arrive with nowhere to go. If either is down
 * we still start (so /health can report *why*), but we say so loudly.
 */
async function main(): Promise<void> {
  logger.info(
    { env: config.env, node: process.version },
    'starting IoT grid telemetry service',
  );

  const [dbOk, redisOk] = await Promise.all([checkDatabase(), checkRedis()]);
  if (!dbOk) {
    logger.error('postgres is not reachable — history writes will fail until it recovers');
  }
  if (!redisOk) {
    logger.error('redis is not reachable — live readings will be unavailable until it recovers');
  }

  const writer = new TelemetryWriter();
  const alerts = new AlertManager();
  const ingestor = new Ingestor({ writer, alerts });

  writer.start();
  ingestor.start();

  const app = buildServer({ writer, alerts, ingestor });

  await app.listen({ port: config.http.port, host: config.http.host });
  logger.info(
    { url: `http://localhost:${config.http.port}` },
    'dashboard and API listening',
  );

  // ── Graceful shutdown ───────────────────────────────────────────
  // Order: stop accepting new messages, flush what we have, then close
  // the HTTP server and the datastores. Anything else risks losing the
  // last batch of readings.
  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutdown requested');

    const hardStop = setTimeout(() => {
      logger.error('graceful shutdown timed out — forcing exit');
      process.exit(1);
    }, 15_000);
    hardStop.unref?.();

    try {
      await ingestor.stop();
      await writer.shutdown();
      await app.close();
      await closeRedis();
      await closePool();
      logger.info('shutdown complete');
      clearTimeout(hardStop);
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'error during shutdown');
      process.exit(1);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  // A stray rejection must be logged, not silently swallowed.
  process.on('unhandledRejection', (reason) => {
    logger.error({ reason }, 'unhandled promise rejection');
  });
  process.on('uncaughtException', (err) => {
    logger.fatal({ err }, 'uncaught exception — exiting');
    void shutdown('uncaughtException');
  });
}

main().catch((err) => {
  logger.fatal({ err }, 'failed to start');
  process.exit(1);
});
