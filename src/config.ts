import 'dotenv/config';
import { z } from 'zod';

/**
 * Every tunable in the system lives here. Nothing is hard-coded in the
 * ingestion path — the 180V threshold, TTLs, batch sizes and dedupe
 * windows are all environment-driven so the service can be retuned
 * without a code change.
 */
const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z
    .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
    .default('info'),
  LOG_PRETTY: z
    .string()
    .default('true')
    .transform((v) => v.toLowerCase() === 'true'),

  HTTP_PORT: z.coerce.number().int().positive().default(3000),
  HTTP_HOST: z.string().default('0.0.0.0'),

  DATABASE_URL: z
    .string()
    .min(1, 'DATABASE_URL is required (e.g. postgres://user:pass@host:5432/grid)'),
  PG_POOL_MAX: z.coerce.number().int().positive().default(10),
  PG_CONNECT_TIMEOUT_MS: z.coerce.number().int().positive().default(5000),
  PG_IDLE_TIMEOUT_MS: z.coerce.number().int().positive().default(30_000),

  REDIS_URL: z.string().default('redis://127.0.0.1:6379'),
  REDIS_LIVE_TTL_SECONDS: z.coerce.number().int().positive().default(120),

  MQTT_BROKER_URL: z.string().default('mqtt://127.0.0.1:1883'),
  MQTT_USERNAME: z.string().optional(),
  MQTT_PASSWORD: z.string().optional(),
  MQTT_CLIENT_ID: z.string().default('grid-ingest-core'),
  MQTT_TOPIC: z.string().default('grid/+/telemetry'),
  MQTT_QOS: z.coerce.number().int().min(0).max(2).default(1),
  MQTT_RECONNECT_PERIOD_MS: z.coerce.number().int().positive().default(2000),
  MQTT_CONNECT_TIMEOUT_MS: z.coerce.number().int().positive().default(10_000),

  BATCH_MAX_ROWS: z.coerce.number().int().positive().default(500),
  BATCH_FLUSH_INTERVAL_MS: z.coerce.number().int().positive().default(1000),
  BATCH_MAX_BUFFER: z.coerce.number().int().positive().default(20_000),

  ALERT_VOLTAGE_MIN: z.coerce.number().default(180),
  ALERT_VOLTAGE_MAX: z.coerce.number().default(260),
  ALERT_DEDUPE_WINDOW_SECONDS: z.coerce.number().int().positive().default(300),

  API_HISTORY_DEFAULT_LIMIT: z.coerce.number().int().positive().default(120),
  API_HISTORY_MAX_LIMIT: z.coerce.number().int().positive().default(2000),
  API_STALE_AFTER_SECONDS: z.coerce.number().int().positive().default(120),
});

const parsed = EnvSchema.safeParse(process.env);

if (!parsed.success) {
  // Fail fast and loudly: a misconfigured service that starts anyway is
  // far more dangerous than one that refuses to boot.
  const issues = parsed.error.issues
    .map((i) => `  • ${i.path.join('.') || '(root)'}: ${i.message}`)
    .join('\n');
  console.error(`\n✖ Invalid environment configuration:\n${issues}\n`);
  console.error('  Copy .env.example to .env and fill in the values.\n');
  process.exit(1);
}

const env = parsed.data;

export const config = {
  env: env.NODE_ENV,
  isProd: env.NODE_ENV === 'production',
  log: {
    level: env.LOG_LEVEL,
    pretty: env.LOG_PRETTY,
  },
  http: {
    port: env.HTTP_PORT,
    host: env.HTTP_HOST,
  },
  postgres: {
    connectionString: env.DATABASE_URL,
    max: env.PG_POOL_MAX,
    connectionTimeoutMillis: env.PG_CONNECT_TIMEOUT_MS,
    idleTimeoutMillis: env.PG_IDLE_TIMEOUT_MS,
  },
  redis: {
    url: env.REDIS_URL,
    liveTtlSeconds: env.REDIS_LIVE_TTL_SECONDS,
  },
  mqtt: {
    url: env.MQTT_BROKER_URL,
    username: env.MQTT_USERNAME || undefined,
    password: env.MQTT_PASSWORD || undefined,
    clientId: env.MQTT_CLIENT_ID,
    topic: env.MQTT_TOPIC,
    qos: env.MQTT_QOS as 0 | 1 | 2,
    reconnectPeriodMs: env.MQTT_RECONNECT_PERIOD_MS,
    connectTimeoutMs: env.MQTT_CONNECT_TIMEOUT_MS,
  },
  batch: {
    maxRows: env.BATCH_MAX_ROWS,
    flushIntervalMs: env.BATCH_FLUSH_INTERVAL_MS,
    maxBuffer: env.BATCH_MAX_BUFFER,
  },
  alerts: {
    voltageMin: env.ALERT_VOLTAGE_MIN,
    voltageMax: env.ALERT_VOLTAGE_MAX,
    dedupeWindowSeconds: env.ALERT_DEDUPE_WINDOW_SECONDS,
  },
  api: {
    historyDefaultLimit: env.API_HISTORY_DEFAULT_LIMIT,
    historyMaxLimit: env.API_HISTORY_MAX_LIMIT,
    staleAfterSeconds: env.API_STALE_AFTER_SECONDS,
  },
} as const;

export type Config = typeof config;
