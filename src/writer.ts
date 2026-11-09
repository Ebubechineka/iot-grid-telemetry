import { pool } from './db.js';
import { config } from './config.js';
import { logger } from './logger.js';
import type { TelemetryPayload } from './validation.js';

export interface BufferedReading {
  nodeId: string;
  voltage: number;
  currentAmps: number;
  timestamp: number;
}

/**
 * Batched, de-duplicated writer.
 *
 * The original code ran one INSERT per MQTT message. At a few hundred
 * messages/second that is a few hundred round trips per second, and the
 * event loop spends its life waiting on Postgres. Here readings accumulate
 * in memory and are flushed either when the buffer reaches BATCH_MAX_ROWS
 * or every BATCH_FLUSH_INTERVAL_MS, whichever comes first — one multi-row
 * INSERT per flush.
 *
 * Duplicate / out-of-order protection works at two levels:
 *  1. In memory: `lastTimestampByNode` rejects anything not strictly newer
 *     than the last accepted reading for that node. Cheap, catches the
 *     common case of a device replaying its buffer after a reconnect.
 *  2. In the database: UNIQUE (node_id, logged_at) + ON CONFLICT DO NOTHING.
 *     Survives restarts and multi-instance deployments.
 */
export class TelemetryWriter {
  private buffer: BufferedReading[] = [];
  private timer: NodeJS.Timeout | null = null;
  private flushing = false;
  private shuttingDown = false;

  /** Highest accepted timestamp per node — the in-memory ordering guard. */
  private lastTimestampByNode = new Map<string, number>();

  /** Counters surfaced on /health so you can see the pipeline working. */
  readonly stats = {
    received: 0,
    accepted: 0,
    rejectedDuplicate: 0,
    rejectedOutOfOrder: 0,
    rejectedInvalid: 0,
    droppedOnOverflow: 0,
    rowsWritten: 0,
    flushErrors: 0,
    lastFlushAt: null as string | null,
  };

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.flush();
    }, config.batch.flushIntervalMs);
    // Don't hold the process open just for the flush timer.
    this.timer.unref?.();
    logger.info(
      { maxRows: config.batch.maxRows, intervalMs: config.batch.flushIntervalMs },
      'telemetry writer started',
    );
  }

  /**
   * Accept a validated reading. Returns whether it was buffered, and why not
   * if it wasn't — the caller logs the reason.
   */
  enqueue(payload: TelemetryPayload): { accepted: boolean; reason?: string } {
    this.stats.received++;

    const last = this.lastTimestampByNode.get(payload.nodeId);

    if (last !== undefined) {
      if (payload.timestamp === last) {
        this.stats.rejectedDuplicate++;
        return { accepted: false, reason: 'duplicate_timestamp' };
      }
      if (payload.timestamp < last) {
        this.stats.rejectedOutOfOrder++;
        return { accepted: false, reason: 'out_of_order' };
      }
    }

    // Back-pressure: never let the buffer grow without bound. If Postgres is
    // down for a long time we drop the OLDEST readings (they are the least
    // useful) rather than the newest, and we say so loudly.
    if (this.buffer.length >= config.batch.maxBuffer) {
      const dropped = this.buffer.shift();
      this.stats.droppedOnOverflow++;
      if (this.stats.droppedOnOverflow % 100 === 1) {
        logger.warn(
          { droppedNode: dropped?.nodeId, bufferSize: this.buffer.length },
          'buffer full — dropping oldest reading (is Postgres reachable?)',
        );
      }
    }

    this.buffer.push({
      nodeId: payload.nodeId,
      voltage: payload.voltage,
      currentAmps: payload.currentAmps,
      timestamp: payload.timestamp,
    });
    this.lastTimestampByNode.set(payload.nodeId, payload.timestamp);
    this.stats.accepted++;

    if (this.buffer.length >= config.batch.maxRows) {
      void this.flush();
    }
    return { accepted: true };
  }

  /** Flush the buffer to Postgres in a single multi-row INSERT. */
  async flush(): Promise<void> {
    if (this.flushing || this.buffer.length === 0) return;
    this.flushing = true;

    const batch = this.buffer.splice(0, this.buffer.length);

    try {
      // Build ($1,$2,$3,to_timestamp($4)),($5,...) ... placeholders.
      const values: unknown[] = [];
      const tuples: string[] = [];
      batch.forEach((r, i) => {
        const b = i * 4;
        tuples.push(`($${b + 1}, $${b + 2}, $${b + 3}, to_timestamp($${b + 4}))`);
        values.push(r.nodeId, r.voltage, r.currentAmps, r.timestamp / 1000);
      });

      const sql = `
        INSERT INTO grid_telemetry_logs (node_id, voltage, current_amps, logged_at)
        VALUES ${tuples.join(',')}
        ON CONFLICT (node_id, logged_at) DO NOTHING
      `;

      const res = await pool.query(sql, values);
      this.stats.rowsWritten += res.rowCount ?? 0;
      this.stats.lastFlushAt = new Date().toISOString();

      // Auto-register nodes we haven't seen, and bump last_seen. One
      // statement for the whole batch.
      const nodeIds = [...new Set(batch.map((r) => r.nodeId))];
      await pool.query(
        `INSERT INTO grid_nodes (node_id, last_seen)
         VALUES ${nodeIds.map((_, i) => `($${i + 1}, now())`).join(',')}
         ON CONFLICT (node_id) DO UPDATE SET last_seen = now()`,
        nodeIds,
      );

      logger.debug(
        { rows: res.rowCount, buffered: batch.length, remaining: this.buffer.length },
        'batch flushed',
      );
    } catch (err) {
      this.stats.flushErrors++;
      // Put the batch back at the FRONT so nothing is lost, but respect the
      // buffer cap so a permanently broken DB can't exhaust memory.
      this.buffer.unshift(...batch);
      if (this.buffer.length > config.batch.maxBuffer) {
        const overflow = this.buffer.length - config.batch.maxBuffer;
        this.buffer.splice(0, overflow);
        this.stats.droppedOnOverflow += overflow;
      }
      logger.error({ err, batchSize: batch.length }, 'batch flush failed — readings requeued');
    } finally {
      this.flushing = false;
    }
  }

  /** Flush what's left and stop the timer. */
  async shutdown(): Promise<void> {
    this.shuttingDown = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    // Drain in a loop: flush() may requeue on error, and we want a bounded
    // number of attempts rather than an infinite loop at shutdown.
    for (let attempt = 0; attempt < 3 && this.buffer.length > 0; attempt++) {
      await this.flush();
    }
    if (this.buffer.length > 0) {
      logger.warn({ remaining: this.buffer.length }, 'shutdown with unflushed readings');
    }
    logger.info(this.stats, 'telemetry writer stopped');
  }

  get bufferSize(): number {
    return this.buffer.length;
  }

  get isShuttingDown(): boolean {
    return this.shuttingDown;
  }
}
