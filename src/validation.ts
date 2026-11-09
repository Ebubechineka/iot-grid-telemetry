import { z } from 'zod';

/**
 * Payload contract for a telemetry message.
 *
 * The original code did `JSON.parse(...)` and trusted the result. A single
 * malformed message (missing nodeId, voltage as a string, NaN, a timestamp
 * in seconds instead of milliseconds) would either throw deep inside the
 * insert or silently write garbage into the time-series table.
 *
 * Rules enforced here:
 *  - nodeId: non-empty, safe charset, bounded length (it becomes a Redis
 *    key and a DB column, so it must not be attacker-controlled free text)
 *  - voltage / currentAmps: finite numbers in a physically plausible range
 *  - timestamp: epoch MILLISECONDS, not in the future beyond a small skew
 */
export const TelemetryPayloadSchema = z.object({
  nodeId: z
    .string()
    .min(1, 'nodeId is required')
    .max(64, 'nodeId too long')
    .regex(/^[A-Za-z0-9._:-]+$/, 'nodeId may only contain letters, digits, . _ : -'),

  voltage: z
    .number({ message: 'voltage must be a number' })
    .finite('voltage must be finite')
    .min(0, 'voltage cannot be negative')
    .max(1000, 'voltage above 1000V is not plausible for this grid'),

  currentAmps: z
    .number({ message: 'currentAmps must be a number' })
    .finite('currentAmps must be finite')
    .min(0, 'currentAmps cannot be negative')
    .max(10_000, 'currentAmps above 10000A is not plausible'),

  timestamp: z
    .number({ message: 'timestamp must be a number' })
    .int('timestamp must be an integer')
    .positive('timestamp must be positive')
    // Epoch ms: anything before 2001 or more than 5 minutes in the future
    // is a clock bug, not a reading.
    .min(1_000_000_000_000, 'timestamp must be epoch milliseconds')
    .refine((t) => t <= Date.now() + 5 * 60_000, {
      message: 'timestamp is more than 5 minutes in the future',
    }),
});

export type TelemetryPayload = z.infer<typeof TelemetryPayloadSchema>;

/**
 * Topic contract: `grid/<nodeId>/telemetry`.
 * We validate the topic as well as the body — the original subscribed to
 * `grid/+/telemetry` but never checked that the nodeId in the topic matched
 * the nodeId in the payload, so a misconfigured device could write readings
 * under another node's identity.
 */
export const TOPIC_PATTERN = /^grid\/([A-Za-z0-9._:-]{1,64})\/telemetry$/;

export function parseTopic(topic: string): { nodeId: string } | null {
  const m = TOPIC_PATTERN.exec(topic);
  if (!m || !m[1]) return null;
  return { nodeId: m[1] };
}

export type ValidationResult =
  | { ok: true; payload: TelemetryPayload }
  | { ok: false; reason: string; detail?: unknown };

/** Parse + validate a raw MQTT message body. Never throws. */
export function validateMessage(raw: Buffer | string): ValidationResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw.toString());
  } catch {
    return { ok: false, reason: 'invalid_json' };
  }

  const result = TelemetryPayloadSchema.safeParse(parsed);
  if (!result.success) {
    return {
      ok: false,
      reason: 'schema_invalid',
      detail: result.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`),
    };
  }
  return { ok: true, payload: result.data };
}
