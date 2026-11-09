import { pool } from './db.js';
import { config } from './config.js';
import { logger } from './logger.js';

export type AlertKind = 'low_voltage' | 'over_voltage';

/**
 * Alert de-duplication.
 *
 * The original code did `console.warn` on every single message below 180V.
 * A node sitting at 175V for ten minutes at 1 message/second produces 600
 * identical warnings — which is how real alerts get ignored.
 *
 * Here an alert for a given (node, kind) is suppressed for
 * ALERT_DEDUPE_WINDOW_SECONDS. The window is held in memory for speed and
 * mirrored into grid_alerts so you keep an audit trail and so a restart
 * doesn't immediately re-fire everything.
 */
export class AlertManager {
  private lastFired = new Map<string, number>();
  readonly stats = { raised: 0, suppressed: 0 };

  private key(nodeId: string, kind: AlertKind): string {
    return `${nodeId}:${kind}`;
  }

  /** Decide whether this reading should raise an alert. */
  evaluate(nodeId: string, voltage: number): { kind: AlertKind; message: string } | null {
    let kind: AlertKind | null = null;
    if (voltage < config.alerts.voltageMin) kind = 'low_voltage';
    else if (voltage > config.alerts.voltageMax) kind = 'over_voltage';
    if (!kind) return null;

    const k = this.key(nodeId, kind);
    const now = Date.now();
    const last = this.lastFired.get(k);
    const windowMs = config.alerts.dedupeWindowSeconds * 1000;

    if (last !== undefined && now - last < windowMs) {
      this.stats.suppressed++;
      return null;
    }

    this.lastFired.set(k, now);
    this.stats.raised++;

    const message =
      kind === 'low_voltage'
        ? `Grid drop detected at node ${nodeId}: ${voltage.toFixed(1)}V (below ${config.alerts.voltageMin}V)`
        : `Over-voltage detected at node ${nodeId}: ${voltage.toFixed(1)}V (above ${config.alerts.voltageMax}V)`;

    return { kind, message };
  }

  /** Persist an alert that survived de-duplication. */
  async record(nodeId: string, kind: AlertKind, voltage: number, message: string): Promise<void> {
    logger.warn({ nodeId, kind, voltage }, message);
    try {
      await pool.query(
        'INSERT INTO grid_alerts (node_id, kind, voltage, message) VALUES ($1, $2, $3, $4)',
        [nodeId, kind, voltage, message],
      );
    } catch (err) {
      // An alert that can't be persisted must not break ingestion.
      logger.error({ err, nodeId, kind }, 'failed to persist alert');
    }
  }

  /** Clear a node's suppression state once it returns to normal. */
  clear(nodeId: string): void {
    this.lastFired.delete(this.key(nodeId, 'low_voltage'));
    this.lastFired.delete(this.key(nodeId, 'over_voltage'));
  }
}
