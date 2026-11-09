import mqtt from 'mqtt';
import { config } from './config.js';
import { logger } from './logger.js';

/**
 * Telemetry simulator.
 *
 * Publishes realistic readings for a handful of fake grid nodes so the whole
 * pipeline can be demoed with no hardware. Each node has its own baseline
 * voltage, load profile and failure personality:
 *
 *   node-north-01   healthy, steady ~230V
 *   node-east-02    healthy, busier load, more current
 *   node-south-03   sags periodically into a low-voltage alert
 *   node-west-04    occasionally spikes (over-voltage)
 *   node-central-05 goes offline for a while, then returns
 *
 * Usage:
 *   npm run simulate                 # 5 nodes, 1 msg/sec each, forever
 *   npm run simulate -- --rate 4     # 4 messages/sec per node
 *   npm run simulate -- --duration 60
 */

interface NodeProfile {
  id: string;
  label: string;
  baseVoltage: number;
  baseCurrent: number;
  /** Probability per tick of entering a sag/spike episode. */
  sagChance: number;
  spikeChance: number;
  /** Probability per tick of going offline for a few ticks. */
  offlineChance: number;
}

const PROFILES: NodeProfile[] = [
  { id: 'node-north-01', label: 'North Feeder', baseVoltage: 231, baseCurrent: 12.4, sagChance: 0.0, spikeChance: 0.0, offlineChance: 0.0 },
  { id: 'node-east-02', label: 'East Feeder', baseVoltage: 228, baseCurrent: 24.8, sagChance: 0.0, spikeChance: 0.0, offlineChance: 0.0 },
  { id: 'node-south-03', label: 'South Feeder', baseVoltage: 224, baseCurrent: 18.2, sagChance: 0.06, spikeChance: 0.0, offlineChance: 0.0 },
  { id: 'node-west-04', label: 'West Feeder', baseVoltage: 236, baseCurrent: 9.6, sagChance: 0.0, spikeChance: 0.03, offlineChance: 0.0 },
  { id: 'node-central-05', label: 'Central Substation', baseVoltage: 233, baseCurrent: 31.5, sagChance: 0.0, spikeChance: 0.0, offlineChance: 0.02 },
];

interface NodeState {
  profile: NodeProfile;
  /** Remaining ticks of a sag/spike episode. */
  episodeTicks: number;
  episodeKind: 'sag' | 'spike' | null;
  offlineTicks: number;
  phase: number;
}

function parseArgs(): { rate: number; duration: number | null } {
  const args = process.argv.slice(2);
  const get = (flag: string): string | undefined => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : undefined;
  };
  const rate = Number(get('--rate')) || 1;
  const durationRaw = get('--duration');
  const duration = durationRaw ? Number(durationRaw) : null;
  return { rate: Math.max(0.1, rate), duration: duration && duration > 0 ? duration : null };
}

/** Smooth pseudo-random walk so the chart looks like a real grid, not noise. */
function jitter(amplitude: number): number {
  return (Math.random() - 0.5) * 2 * amplitude;
}

function nextReading(state: NodeState): { voltage: number; currentAmps: number } | null {
  const { profile } = state;

  // Offline handling: a node that drops out stops publishing entirely,
  // which is what exercises the Redis TTL / "stale" path on the dashboard.
  if (state.offlineTicks > 0) {
    state.offlineTicks--;
    return null;
  }
  if (Math.random() < profile.offlineChance) {
    state.offlineTicks = 8 + Math.floor(Math.random() * 12);
    logger.info({ node: profile.id }, 'simulator: node going offline');
    return null;
  }

  // Episode handling: a sag or spike lasts a few ticks.
  if (state.episodeTicks > 0) {
    state.episodeTicks--;
  } else if (Math.random() < profile.sagChance) {
    state.episodeTicks = 6 + Math.floor(Math.random() * 10);
    state.episodeKind = 'sag';
    logger.info({ node: profile.id }, 'simulator: low-voltage episode starting');
  } else if (Math.random() < profile.spikeChance) {
    state.episodeTicks = 3 + Math.floor(Math.random() * 5);
    state.episodeKind = 'spike';
    logger.info({ node: profile.id }, 'simulator: over-voltage episode starting');
  } else {
    state.episodeKind = null;
  }

  state.phase += 0.15;

  // Slow sinusoidal drift + small noise = believable load curve.
  let voltage = profile.baseVoltage + Math.sin(state.phase) * 3.5 + jitter(1.2);
  let currentAmps = profile.baseCurrent + Math.sin(state.phase * 0.7) * 2.2 + jitter(0.6);

  if (state.episodeKind === 'sag') {
    voltage = 168 + Math.random() * 10; // clearly below the 180V threshold
    currentAmps = profile.baseCurrent * 1.35;
  } else if (state.episodeKind === 'spike') {
    voltage = 268 + Math.random() * 12; // clearly above the 260V threshold
    currentAmps = profile.baseCurrent * 0.8;
  }

  return {
    voltage: Number(Math.max(0, voltage).toFixed(2)),
    currentAmps: Number(Math.max(0, currentAmps).toFixed(2)),
  };
}

async function main(): Promise<void> {
  const { rate, duration } = parseArgs();
  const intervalMs = Math.round(1000 / rate);

  logger.info(
    { broker: config.mqtt.url, nodes: PROFILES.length, rate, duration },
    'simulator starting',
  );

  const client = mqtt.connect(config.mqtt.url, {
    clientId: `grid-simulator-${process.pid}`,
    reconnectPeriod: 2000,
    connectTimeout: 10_000,
  });

  const states: NodeState[] = PROFILES.map((profile) => ({
    profile,
    episodeTicks: 0,
    episodeKind: null,
    offlineTicks: 0,
    phase: Math.random() * Math.PI * 2,
  }));

  let published = 0;
  let timer: NodeJS.Timeout | null = null;

  await new Promise<void>((resolve, reject) => {
    client.once('connect', () => {
      logger.info('simulator: connected to broker');
      resolve();
    });
    client.once('error', reject);
  });

  const tick = (): void => {
    const now = Date.now();
    for (const state of states) {
      const reading = nextReading(state);
      if (!reading) continue;

      const payload = {
        nodeId: state.profile.id,
        voltage: reading.voltage,
        currentAmps: reading.currentAmps,
        timestamp: now,
      };

      client.publish(
        `grid/${state.profile.id}/telemetry`,
        JSON.stringify(payload),
        { qos: config.mqtt.qos },
        (err) => {
          if (err) logger.error({ err, node: state.profile.id }, 'simulator: publish failed');
        },
      );
      published++;
    }
  };

  tick();
  timer = setInterval(tick, intervalMs);

  const stop = (): void => {
    if (timer) clearInterval(timer);
    timer = null;
    logger.info({ published }, 'simulator stopping');
    client.end(false, {}, () => process.exit(0));
    setTimeout(() => process.exit(0), 2000).unref?.();
  };

  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);

  if (duration) {
    setTimeout(stop, duration * 1000);
  }

  // Periodic progress line so you can see it's alive.
  setInterval(() => {
    logger.info({ published }, 'simulator: still publishing');
  }, 15_000).unref?.();
}

main().catch((err) => {
  logger.fatal({ err }, 'simulator failed');
  process.exit(1);
});
