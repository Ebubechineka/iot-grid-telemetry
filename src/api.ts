import Fastify from 'fastify';
import { readFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { logger } from './logger.js';
import { pool } from './db.js';
import { redis, getLiveReadings } from './redis.js';
import type { TelemetryWriter } from './writer.js';
import type { AlertManager } from './alerts.js';
import type { Ingestor } from './ingestor.js';

const here = dirname(fileURLToPath(import.meta.url));
const publicDir = join(here, '..', 'public');

export interface ApiDeps {
  writer: TelemetryWriter;
  alerts: AlertManager;
  ingestor: Ingestor;
}

interface NodeRow {
  node_id: string;
  label: string | null;
  location: string | null;
  last_seen: string;
}

interface HistoryRow {
  node_id: string;
  voltage: number;
  current_amps: number;
  power_watts: number;
  logged_at: string;
}

export function buildServer(deps: ApiDeps) {
  const app = Fastify({
    loggerInstance: logger,
    disableRequestLogging: true,
    trustProxy: true,
  });

  // ── CORS: the dashboard is served from the same origin, but allow
  //    embedding/other origins so the API is usable from anywhere.
  app.addHook('onRequest', async (req, reply) => {
    reply.header('Access-Control-Allow-Origin', '*');
    reply.header('Access-Control-Allow-Methods', 'GET,OPTIONS');
    reply.header('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') {
      reply.code(204).send();
    }
  });

  // ── Static dashboard ────────────────────────────────────────────
  app.get('/', async (_req, reply) => {
    const html = await readFile(join(publicDir, 'index.html'), 'utf8');
    reply.type('text/html; charset=utf-8').send(html);
  });

  // ── Health ──────────────────────────────────────────────────────
  app.get('/health', async (_req, reply) => {
    const [dbOk, redisOk] = await Promise.all([
      pool
        .query('SELECT 1')
        .then(() => true)
        .catch(() => false),
      redis
        .ping()
        .then((p) => p === 'PONG')
        .catch(() => false),
    ]);

    const mqttOk = deps.ingestor.isConnected;
    const healthy = dbOk && redisOk && mqttOk;

    reply.code(healthy ? 200 : 503).send({
      status: healthy ? 'ok' : 'degraded',
      uptimeSeconds: Math.round(process.uptime()),
      checks: {
        postgres: dbOk ? 'up' : 'down',
        redis: redisOk ? 'up' : 'down',
        mqtt: mqttOk ? 'up' : 'down',
      },
      pipeline: {
        mqtt: deps.ingestor.stats,
        writer: { ...deps.writer.stats, bufferSize: deps.writer.bufferSize },
        alerts: deps.alerts.stats,
      },
    });
  });

  // ── Nodes with their latest reading ─────────────────────────────
  app.get('/api/nodes', async (_req, reply) => {
    // Latest reading per node, straight from the time-series table.
    // DISTINCT ON is the idiomatic Postgres way to do this and uses
    // idx_telemetry_node_time.
    const { rows } = await pool.query<
      NodeRow & { voltage: number; current_amps: number; power_watts: number; logged_at: string }
    >(`
      SELECT DISTINCT ON (t.node_id)
             t.node_id,
             t.voltage,
             t.current_amps,
             t.power_watts,
             t.logged_at,
             n.label,
             n.location,
             n.last_seen
        FROM grid_telemetry_logs t
        LEFT JOIN grid_nodes n ON n.node_id = t.node_id
       ORDER BY t.node_id, t.logged_at DESC
    `);

    const nodeIds = rows.map((r) => r.node_id);
    const live = await getLiveReadings(nodeIds);
    const now = Date.now();

    const nodes = rows.map((r) => {
      const liveReading = live.get(r.node_id);
      const loggedAt = new Date(r.logged_at).getTime();
      const ageSeconds = Math.max(0, Math.round((now - loggedAt) / 1000));
      const stale = ageSeconds > config.api.staleAfterSeconds;

      const voltage = liveReading?.voltage ?? r.voltage;
      const currentAmps = liveReading?.currentAmps ?? r.current_amps;

      let status: 'normal' | 'low_voltage' | 'over_voltage' | 'stale';
      if (stale) status = 'stale';
      else if (voltage < config.alerts.voltageMin) status = 'low_voltage';
      else if (voltage > config.alerts.voltageMax) status = 'over_voltage';
      else status = 'normal';

      return {
        nodeId: r.node_id,
        label: r.label ?? r.node_id,
        location: r.location,
        voltage: Number(voltage.toFixed(2)),
        currentAmps: Number(currentAmps.toFixed(2)),
        powerWatts: Number((voltage * currentAmps).toFixed(1)),
        status,
        ageSeconds,
        loggedAt: r.logged_at,
        live: Boolean(liveReading),
      };
    });

    nodes.sort((a, b) => a.nodeId.localeCompare(b.nodeId));

    reply.send({
      count: nodes.length,
      thresholds: {
        voltageMin: config.alerts.voltageMin,
        voltageMax: config.alerts.voltageMax,
        staleAfterSeconds: config.api.staleAfterSeconds,
      },
      nodes,
    });
  });

  // ── One node's history ──────────────────────────────────────────
  app.get<{ Params: { nodeId: string }; Querystring: { limit?: string; minutes?: string } }>(
    '/api/nodes/:nodeId/history',
    async (req, reply) => {
      const { nodeId } = req.params;
      if (!/^[A-Za-z0-9._:-]{1,64}$/.test(nodeId)) {
        return reply.code(400).send({ error: 'invalid nodeId' });
      }

      const limit = Math.min(
        Math.max(Number(req.query.limit) || config.api.historyDefaultLimit, 1),
        config.api.historyMaxLimit,
      );
      const minutes = Number(req.query.minutes) || null;

      const params: unknown[] = [nodeId];
      let timeFilter = '';
      if (minutes && minutes > 0) {
        params.push(minutes);
        timeFilter = `AND logged_at >= now() - ($2 || ' minutes')::interval`;
      }
      params.push(limit);

      const { rows } = await pool.query<HistoryRow>(
        `SELECT node_id, voltage, current_amps, power_watts, logged_at
           FROM grid_telemetry_logs
          WHERE node_id = $1 ${timeFilter}
          ORDER BY logged_at DESC
          LIMIT $${params.length}`,
        params,
      );

      if (rows.length === 0) {
        return reply.code(404).send({ error: 'no readings for this node', nodeId });
      }

      // Return oldest-first so a chart can plot it directly.
      const points = rows
        .map((r) => ({
          t: r.logged_at,
          voltage: Number(r.voltage.toFixed(2)),
          currentAmps: Number(r.current_amps.toFixed(2)),
          powerWatts: Number(r.power_watts.toFixed(1)),
        }))
        .reverse();

      reply.send({
        nodeId,
        count: points.length,
        thresholds: {
          voltageMin: config.alerts.voltageMin,
          voltageMax: config.alerts.voltageMax,
        },
        points,
      });
    },
  );

  // ── Recent alerts ───────────────────────────────────────────────
  app.get<{ Querystring: { limit?: string } }>('/api/alerts', async (req, reply) => {
    const limit = Math.min(Math.max(Number(req.query.limit) || 20, 1), 200);
    const { rows } = await pool.query(
      `SELECT id, node_id, kind, voltage, message, raised_at
         FROM grid_alerts
        ORDER BY raised_at DESC
        LIMIT $1`,
      [limit],
    );
    reply.send({ count: rows.length, alerts: rows });
  });

  // ── 404 ─────────────────────────────────────────────────────────
  app.setNotFoundHandler((req, reply) => {
    reply.code(404).send({ error: 'not found', path: req.url });
  });

  return app;
}
