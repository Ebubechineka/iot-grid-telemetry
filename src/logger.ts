import pino from 'pino';
import { config } from './config.js';

/**
 * Structured JSON logging. In development we pipe through pino-pretty for
 * readability; in production we emit raw JSON so a log shipper can parse it.
 */
export const logger = pino({
  level: config.log.level,
  base: { service: 'iot-grid-telemetry' },
  timestamp: pino.stdTimeFunctions.isoTime,
  redact: {
    paths: ['password', '*.password', 'DATABASE_URL', 'REDIS_URL', 'MQTT_PASSWORD'],
    censor: '[redacted]',
  },
  ...(config.log.pretty
    ? {
        transport: {
          target: 'pino-pretty',
          options: {
            colorize: true,
            translateTime: 'HH:MM:ss.l',
            ignore: 'pid,hostname,service',
            singleLine: false,
          },
        },
      }
    : {}),
});

export type Logger = typeof logger;
