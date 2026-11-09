-- ═══════════════════════════════════════════════════════════════════
--  IoT Grid Telemetry — schema
--  Works on plain PostgreSQL 13+ and upgrades itself to a TimescaleDB
--  hypertable when the extension is available.
-- ═══════════════════════════════════════════════════════════════════

-- TimescaleDB is optional. On a plain Postgres image the CREATE EXTENSION
-- fails; we swallow that and fall back to a regular partitioned-friendly
-- table with a BRIN index, which is still fast for time-series scans.
DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
  RAISE NOTICE 'TimescaleDB extension available';
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'TimescaleDB not available (%), continuing with plain PostgreSQL', SQLERRM;
END
$$;

-- ── Raw telemetry log ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS grid_telemetry_logs (
  id           BIGSERIAL,
  node_id      TEXT             NOT NULL,
  voltage      DOUBLE PRECISION NOT NULL,
  current_amps DOUBLE PRECISION NOT NULL,
  -- Power in watts, derived at write time so dashboards don't recompute.
  power_watts  DOUBLE PRECISION GENERATED ALWAYS AS (voltage * current_amps) STORED,
  logged_at    TIMESTAMPTZ      NOT NULL,
  ingested_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
  CONSTRAINT grid_telemetry_logs_node_time_uniq UNIQUE (node_id, logged_at)
);

-- The UNIQUE (node_id, logged_at) constraint is what makes duplicate and
-- out-of-order protection survive a restart: even if the in-memory guard
-- is cold, the database refuses a second row for the same reading.

-- ── Indexes ─────────────────────────────────────────────────────────
-- "latest reading per node" and "history for one node, newest first"
CREATE INDEX IF NOT EXISTS idx_telemetry_node_time
  ON grid_telemetry_logs (node_id, logged_at DESC);

-- Time-range scans across all nodes (dashboards, exports)
CREATE INDEX IF NOT EXISTS idx_telemetry_time
  ON grid_telemetry_logs (logged_at DESC);

-- BRIN is tiny and ideal for append-only time data on plain Postgres.
CREATE INDEX IF NOT EXISTS idx_telemetry_time_brin
  ON grid_telemetry_logs USING BRIN (logged_at) WITH (pages_per_range = 32);

-- ── Hypertable upgrade (TimescaleDB only) ───────────────────────────
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    PERFORM create_hypertable(
      'grid_telemetry_logs',
      'logged_at',
      chunk_time_interval => INTERVAL '1 day',
      if_not_exists => TRUE,
      migrate_data => TRUE
    );
    RAISE NOTICE 'grid_telemetry_logs is now a TimescaleDB hypertable';
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Hypertable conversion skipped: %', SQLERRM;
END
$$;

-- ── Retention (TimescaleDB only) ────────────────────────────────────
-- Keep 90 days of raw telemetry. On plain Postgres this is a no-op and
-- you would schedule a DELETE job instead.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    PERFORM add_retention_policy('grid_telemetry_logs', INTERVAL '90 days', if_not_exists => TRUE);
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Retention policy skipped: %', SQLERRM;
END
$$;

-- ── Continuous aggregate: 1-minute rollups (TimescaleDB only) ───────
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    CREATE MATERIALIZED VIEW IF NOT EXISTS grid_telemetry_1m
    WITH (timescaledb.continuous) AS
    SELECT
      time_bucket('1 minute', logged_at) AS bucket,
      node_id,
      avg(voltage)      AS avg_voltage,
      min(voltage)      AS min_voltage,
      max(voltage)      AS max_voltage,
      avg(current_amps) AS avg_current,
      avg(power_watts)  AS avg_power,
      count(*)          AS samples
    FROM grid_telemetry_logs
    GROUP BY bucket, node_id
    WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('grid_telemetry_1m',
      start_offset => INTERVAL '3 hours',
      end_offset   => INTERVAL '1 minute',
      schedule_interval => INTERVAL '1 minute',
      if_not_exists => TRUE);
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Continuous aggregate skipped: %', SQLERRM;
END
$$;

-- ── Node registry ───────────────────────────────────────────────────
-- Optional metadata so the dashboard can show a human label and location
-- instead of a bare node id. Ingestion auto-registers unknown nodes.
CREATE TABLE IF NOT EXISTS grid_nodes (
  node_id     TEXT PRIMARY KEY,
  label       TEXT,
  location    TEXT,
  first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Alert log ───────────────────────────────────────────────────────
-- Every alert that survives de-duplication is recorded here, so the
-- dashboard can show a recent-events feed and you keep an audit trail.
CREATE TABLE IF NOT EXISTS grid_alerts (
  id          BIGSERIAL PRIMARY KEY,
  node_id     TEXT NOT NULL,
  kind        TEXT NOT NULL,          -- 'low_voltage' | 'over_voltage'
  voltage     DOUBLE PRECISION NOT NULL,
  message     TEXT NOT NULL,
  raised_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_raised
  ON grid_alerts (raised_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_node_kind_time
  ON grid_alerts (node_id, kind, raised_at DESC);
