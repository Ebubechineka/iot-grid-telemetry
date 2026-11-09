import { readdir, readFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { pool, closePool } from './db.js';
import { logger } from './logger.js';

const here = dirname(fileURLToPath(import.meta.url));
const sqlDir = join(here, '..', 'sql');

/**
 * Minimal forward-only migration runner.
 * Applies every .sql file in sql/ in filename order, inside a transaction,
 * and records what it applied in schema_migrations.
 */
async function main(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename   TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  const files = (await readdir(sqlDir)).filter((f) => f.endsWith('.sql')).sort();
  const { rows } = await pool.query<{ filename: string }>('SELECT filename FROM schema_migrations');
  const done = new Set(rows.map((r) => r.filename));

  for (const file of files) {
    if (done.has(file)) {
      logger.info({ file }, 'migration already applied, skipping');
      continue;
    }
    const sql = await readFile(join(sqlDir, file), 'utf8');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
      await client.query('COMMIT');
      logger.info({ file }, 'migration applied');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      logger.error({ err, file }, 'migration failed — rolled back');
      throw err;
    } finally {
      client.release();
    }
  }

  // Report what we ended up with, so a failed TimescaleDB upgrade is visible.
  const ext = await pool.query<{ extname: string }>(
    "SELECT extname FROM pg_extension WHERE extname = 'timescaledb'",
  );
  const hypertable = await pool.query<{ count: string }>(
    `SELECT count(*)::text AS count
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'grid_telemetry_logs' AND c.relkind = 'r'`,
  );
  logger.info(
    {
      timescale: ext.rowCount ? 'enabled' : 'not installed (plain Postgres mode)',
      telemetryTable: hypertable.rows[0]?.count === '1' ? 'present' : 'missing',
    },
    'schema ready',
  );

  await closePool();
}

main().catch(async (err) => {
  logger.error({ err }, 'migration run failed');
  await closePool().catch(() => {});
  process.exit(1);
});
