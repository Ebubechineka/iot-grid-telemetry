import mqtt, { type MqttClient } from 'mqtt';
import { config } from './config.js';
import { logger } from './logger.js';
import { parseTopic, validateMessage } from './validation.js';
import { setLiveReading } from './redis.js';
import { TelemetryWriter } from './writer.js';
import { AlertManager } from './alerts.js';

export interface IngestorDeps {
  writer: TelemetryWriter;
  alerts: AlertManager;
}

/**
 * MQTT ingestion core.
 *
 * Responsibilities, in order:
 *  1. subscribe to the configured topic filter
 *  2. validate the topic shape AND that the topic's nodeId matches the body
 *  3. validate the payload schema
 *  4. write the live reading to Redis (with TTL)
 *  5. evaluate alerts (de-duplicated)
 *  6. hand the reading to the batched writer
 *
 * Every step is individually guarded: one bad message can never take down
 * the consumer, and every rejection is counted and logged with a reason.
 */
export class Ingestor {
  private client: MqttClient | null = null;
  private connected = false;
  private stopping = false;

  readonly stats = {
    messages: 0,
    badTopic: 0,
    nodeMismatch: 0,
    invalidPayload: 0,
    redisErrors: 0,
    connectedAt: null as string | null,
    lastMessageAt: null as string | null,
  };

  constructor(private readonly deps: IngestorDeps) {}

  start(): void {
    const { writer, alerts } = this.deps;

    this.client = mqtt.connect(config.mqtt.url, {
      clientId: `${config.mqtt.clientId}-${process.pid}`,
      username: config.mqtt.username,
      password: config.mqtt.password,
      reconnectPeriod: config.mqtt.reconnectPeriodMs,
      connectTimeout: config.mqtt.connectTimeoutMs,
      clean: true,
      // Let the broker queue QoS1 messages while we're briefly away.
      resubscribe: true,
    });

    this.client.on('connect', () => {
      this.connected = true;
      this.stats.connectedAt = new Date().toISOString();
      logger.info({ broker: config.mqtt.url, topic: config.mqtt.topic }, 'mqtt: connected');

      this.client?.subscribe(config.mqtt.topic, { qos: config.mqtt.qos }, (err, granted) => {
        if (err) {
          logger.error({ err, topic: config.mqtt.topic }, 'mqtt: subscribe failed');
          return;
        }
        logger.info(
          { granted: granted?.map((g) => `${g.topic} (qos ${g.qos})`) },
          'System Ingestion Core Active. Awaiting IoT Payloads...',
        );
      });
    });

    this.client.on('reconnect', () => logger.warn('mqtt: reconnecting'));
    this.client.on('close', () => {
      this.connected = false;
      if (!this.stopping) logger.warn('mqtt: connection closed');
    });
    this.client.on('offline', () => logger.warn('mqtt: offline'));
    this.client.on('error', (err) => logger.error({ err }, 'mqtt error'));

    this.client.on('message', (topic, message) => {
      void this.handleMessage(topic, message, writer, alerts);
    });
  }

  private async handleMessage(
    topic: string,
    message: Buffer,
    writer: TelemetryWriter,
    alerts: AlertManager,
  ): Promise<void> {
    this.stats.messages++;
    this.stats.lastMessageAt = new Date().toISOString();

    // ── 1. Topic validation ────────────────────────────────────────
    const topicInfo = parseTopic(topic);
    if (!topicInfo) {
      this.stats.badTopic++;
      logger.warn({ topic }, 'rejected: topic does not match grid/<nodeId>/telemetry');
      return;
    }

    // ── 2. Payload validation ──────────────────────────────────────
    const result = validateMessage(message);
    if (!result.ok) {
      this.stats.invalidPayload++;
      writer.stats.rejectedInvalid++;
      logger.warn(
        { topic, reason: result.reason, detail: result.detail },
        'rejected: invalid telemetry payload',
      );
      return;
    }
    const payload = result.payload;

    // ── 3. Topic/body identity check ───────────────────────────────
    // A device publishing to grid/node-a/telemetry with nodeId "node-b"
    // is either misconfigured or spoofing. Refuse it.
    if (payload.nodeId !== topicInfo.nodeId) {
      this.stats.nodeMismatch++;
      logger.warn(
        { topic, topicNode: topicInfo.nodeId, payloadNode: payload.nodeId },
        'rejected: nodeId in payload does not match topic',
      );
      return;
    }

    // ── 4. Live cache (Redis, with TTL) ────────────────────────────
    try {
      await setLiveReading({
        nodeId: payload.nodeId,
        voltage: payload.voltage,
        currentAmps: payload.currentAmps,
        timestamp: payload.timestamp,
      });
    } catch (err) {
      // Redis being down must not stop history from being written.
      this.stats.redisErrors++;
      logger.error({ err, nodeId: payload.nodeId }, 'redis live-cache write failed');
    }

    // ── 5. Alerts (de-duplicated) ──────────────────────────────────
    const alert = alerts.evaluate(payload.nodeId, payload.voltage);
    if (alert) {
      void alerts.record(payload.nodeId, alert.kind, payload.voltage, alert.message);
    } else if (
      payload.voltage >= config.alerts.voltageMin &&
      payload.voltage <= config.alerts.voltageMax
    ) {
      // Back in range — allow the next excursion to alert immediately.
      alerts.clear(payload.nodeId);
    }

    // ── 6. Batched history write ───────────────────────────────────
    const enq = writer.enqueue(payload);
    if (!enq.accepted) {
      logger.debug(
        { nodeId: payload.nodeId, reason: enq.reason, ts: payload.timestamp },
        'reading not buffered',
      );
    }
  }

  get isConnected(): boolean {
    return this.connected;
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (!this.client) return;
    await new Promise<void>((resolve) => {
      this.client?.end(false, {}, () => resolve());
      // Never hang shutdown on a wedged socket.
      setTimeout(resolve, 3000).unref?.();
    });
    logger.info('mqtt: disconnected');
  }
}
