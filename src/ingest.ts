// ingest.ts — the hot path. Decode → R2 put → minute-rollup upsert → enqueue.
// Target <50ms CPU; NEVER runs analytics inline.

import type { Context } from 'hono'
import { decodeBatch, hexToBytes } from 'openstrap-protocol/ts/live'
import { rollupMinutes } from './rollup'
import { perMinuteSignals } from './ingest_signals'
import { writeBatch } from './minute_store'

// ── Rate limiting: per-user token bucket in a D1 row (RESILIENCE §7). ──
// Refill RATE tokens/sec, cap BURST. One ingest = one token. Over budget → 429.
const RL_BURST = 30          // allow short bursts (bulk reconnect upload)
const RL_REFILL = 0.5        // 0.5 tokens/sec ≈ 30 batches/min sustained

export async function checkRateLimit(db: D1Database, userId: string): Promise<boolean> {
  const now = Math.floor(Date.now() / 1000)
  const row = await db.prepare(
    'SELECT tokens, updated_at FROM rate_limit WHERE user_id = ?',
  ).bind(userId).first<{ tokens: number; updated_at: number }>()

  let tokens = RL_BURST
  if (row) {
    const elapsed = Math.max(0, now - row.updated_at)
    tokens = Math.min(RL_BURST, row.tokens + elapsed * RL_REFILL)
  }
  if (tokens < 1) {
    // Persist the (refilled-but-still-empty) state so the clock keeps ticking.
    await db.prepare(
      'INSERT INTO rate_limit (user_id, tokens, updated_at) VALUES (?,?,?) ' +
      'ON CONFLICT(user_id) DO UPDATE SET tokens=excluded.tokens, updated_at=excluded.updated_at',
    ).bind(userId, tokens, now).run()
    return false
  }
  tokens -= 1
  await db.prepare(
    'INSERT INTO rate_limit (user_id, tokens, updated_at) VALUES (?,?,?) ' +
    'ON CONFLICT(user_id) DO UPDATE SET tokens=excluded.tokens, updated_at=excluded.updated_at',
  ).bind(userId, tokens, now).run()
  return true
}

interface IngestEnv {
  DB: D1Database
  RAW_BUCKET: R2Bucket
  ANALYTICS_Q?: Queue
}

// Mark the user dirty so the NEXT 30-min cron sweep enqueues exactly ONE analytics
// job for them (deduped). We deliberately do NOT enqueue per ingest batch: at scale
// that storms the queue (~240 msgs/user/day) and re-derives the same day hundreds of
// times. A cheap dirty flag here + the cron as the single enqueue point makes it one
// sweep per user per 30 min. Freshness is bounded by the sweep interval (fine — and
// recovery still fires promptly on wake via the sleep-detection trigger).
async function markUserDirty(env: IngestEnv, userId: string) {
  await env.DB.prepare(
    'INSERT INTO analytics_cursor (user_id, last_min_ts, dirty) VALUES (?,0,1) ' +
    'ON CONFLICT(user_id) DO UPDATE SET dirty = 1',
  ).bind(userId).run()
}

// POST /ingest/batch (JWT) { device_id, records: [hex,…] }
export async function ingestBatch(c: Context<{ Bindings: IngestEnv; Variables: { userId: string } }>) {
  const userId = c.get('userId')
  const { device_id, records } = await c.req.json<{ device_id: string; records: string[] }>()
  if (!device_id || !Array.isArray(records)) return c.json({ error: 'Invalid payload' }, 400)

  // 1. Rate limit.
  if (!(await checkRateLimit(c.env.DB, userId))) {
    return c.json({ error: 'Rate limit exceeded' }, 429)
  }

  // 2. Decode.
  const samples = decodeBatch(records)

  // 3. (Raw R2 archive removed.) Decoders are validated, and every surfaced signal —
  //    HR/RR/HRV, steps, SpO₂/temp, and PPG-resp (via the R21 green RIIV proxy) — is now
  //    derived at ingest and stored in the D1 minute blob, so there is nothing to
  //    re-decode from raw later. RAW_BUCKET is retained ONLY for the hot/seal tier
  //    (sealOldDays moves aged minute_day blobs to R2); no per-POST raw object is written.

  // 4. Rollup → day-packed minute store (ONE row per day, read-merge-write). This is
    // the cost lever: ~1 row written/day instead of ~1,440. The merge inside writeBatch
    // mirrors the old per-minute ON CONFLICT exactly (additive sums, CASE hr_min, MAX
    // hr_max, steps add, rr longer-wins). Idempotent IFF the edge dedupes by hex (it does).
    // RR (ms, R24 + live 0x28/R10) rides the blob as number[] — HRV folds into the
    // wake-close with zero R2. (Storage: gzipped blob via bound param — well under D1's
    // 2 MB value cap; serialized outside the 100 KB SQL-statement limit.)
  const buckets = rollupMinutes(samples)
  let minutesWritten = 0
  if (buckets.length > 0) {
    const sig = perMinuteSignals(records)
    await writeBatch(c.env, userId, buckets, sig, Math.floor(Date.now() / 1000))
    minutesWritten = buckets.length
  }

  // 5. Events → events (INSERT OR IGNORE). Records that look like events still
  //    flow through here only if the caller mixes them; the dedicated
  //    /ingest/events route handles event-only payloads. Here we skip.

  // 6. Enqueue analytics (or mark dirty). Never run inline.
  if (minutesWritten > 0) await markUserDirty(c.env, userId)

  // 7. Respond. `received` = records accepted (a 2xx means we decoded + rolled them into
  //    the D1 minute store); `decoded` = records that yielded a surfaceable sample;
  //    `minutes_written` = per-minute rollups touched. The client shows `received` so the
  //    count is honest, not 0.
  return c.json({
    ok: true,
    received: records.length,
    decoded: samples.length,
    minutes_written: minutesWritten,
  })
}

// POST /ingest/events (JWT) { device_id, events: [hex,…] }
export async function ingestEvents(c: Context<{ Bindings: IngestEnv; Variables: { userId: string } }>) {
  const userId = c.get('userId')
  const { device_id, events } = await c.req.json<{ device_id: string; events: string[] }>()
  if (!device_id || !Array.isArray(events)) return c.json({ error: 'Invalid payload' }, 400)

  const stmt = c.env.DB.prepare(
    'INSERT OR IGNORE INTO events (user_id, device_id, hex, event_id, ts) VALUES (?,?,?,?,?)')
  const batch = []
  for (const hex of events) {
    try {
      const b = hexToBytes(hex)
      if (b.length < 8) continue
      const view = new DataView(b.buffer, b.byteOffset, b.byteLength)
      batch.push(stmt.bind(userId, device_id, hex, view.getUint16(2, true), view.getUint32(4, true)))
    } catch { /* skip malformed */ }
  }
  if (batch.length > 0) await c.env.DB.batch(batch)
  return c.json({ total: events.length, stored: batch.length })
}
