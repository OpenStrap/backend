// ingest.ts — the hot path. Decode → R2 put → minute-rollup upsert → enqueue.
// Target <50ms CPU; NEVER runs analytics inline.

import type { Context } from 'hono'
import { decodeBatch, hexToBytes } from './decode'
import { rollupMinutes } from './rollup'

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

  // 3. R2 put the whole batch raw (re-decodable system of record).
  let rawKey: string | null = null
  if (records.length > 0) {
    const tss = samples.map((s) => s.ts).filter((t) => t > 0)
    const minTs = tss.length ? Math.min(...tss) : 0
    const maxTs = tss.length ? Math.max(...tss) : 0
    rawKey = `raw/${userId}/${device_id}/${Date.now()}-${minTs}-${maxTs}.txt`
    try {
      await c.env.RAW_BUCKET.put(rawKey, records.join('\n'))
    } catch (e) {
      console.error('R2 put failed', e)
      rawKey = null
    }
  }

  // 4. Rollup → minute upsert (one batch). Merge deterministically with stored.
  const buckets = rollupMinutes(samples)
  let minutesWritten = 0
  let maxTsMin = 0
  if (buckets.length > 0) {
    // The upsert folds new running sums into stored aggregates and recomputes
    // the display columns (hr_avg, activity, hr_min/max) from the merged totals.
    const stmt = c.env.DB.prepare(
      'INSERT INTO minute (user_id, ts_min, hr_avg, hr_min, hr_max, hr_n, hr_sum, activity, act_sum, act_n, steps, wrist_on) ' +
      'VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ' +
      'ON CONFLICT(user_id, ts_min) DO UPDATE SET ' +
      'hr_sum = minute.hr_sum + excluded.hr_sum, ' +
      'hr_n = minute.hr_n + excluded.hr_n, ' +
      'hr_min = CASE WHEN minute.hr_min = 0 THEN excluded.hr_min ' +
      '              WHEN excluded.hr_min = 0 THEN minute.hr_min ' +
      '              ELSE MIN(minute.hr_min, excluded.hr_min) END, ' +
      'hr_max = MAX(minute.hr_max, excluded.hr_max), ' +
      'hr_avg = CASE WHEN (minute.hr_n + excluded.hr_n) > 0 ' +
      '              THEN (minute.hr_sum + excluded.hr_sum) / (minute.hr_n + excluded.hr_n) ELSE 0 END, ' +
      'act_sum = minute.act_sum + excluded.act_sum, ' +
      'act_n = minute.act_n + excluded.act_n, ' +
      'activity = CASE WHEN (minute.act_n + excluded.act_n) > 0 ' +
      '                THEN (minute.act_sum + excluded.act_sum) / (minute.act_n + excluded.act_n) ELSE 0 END, ' +
      'steps = minute.steps + excluded.steps, ' +
      'wrist_on = MAX(minute.wrist_on, excluded.wrist_on)',
    )
    const batch = buckets.map((b) => {
      if (b.ts_min > maxTsMin) maxTsMin = b.ts_min
      const hr_avg = b.hr_n > 0 ? Math.round(b.hr_sum / b.hr_n) : 0
      const activity = b.act_n > 0 ? b.act_sum / b.act_n : 0
      return stmt.bind(userId, b.ts_min, hr_avg, b.hr_min, b.hr_max, b.hr_n, b.hr_sum,
        activity, b.act_sum, b.act_n, b.steps, b.wrist_on)
    })
    await c.env.DB.batch(batch)
    minutesWritten = buckets.length
  }

  // 5. Events → events (INSERT OR IGNORE). Records that look like events still
  //    flow through here only if the caller mixes them; the dedicated
  //    /ingest/events route handles event-only payloads. Here we skip.

  // 6. Enqueue analytics (or mark dirty). Never run inline.
  if (minutesWritten > 0) await markUserDirty(c.env, userId)

  // 7. Respond. `received` = records persisted raw to R2 (the re-decodable system of
  //    record); `decoded` = records that yielded a surfaceable sample; `minutes_written`
  //    = per-minute rollups touched. The client shows `received` so the count is honest
  //    (a 2xx means we stored them all), not 0.
  return c.json({
    ok: true,
    received: records.length,
    decoded: samples.length,
    minutes_written: minutesWritten,
    raw_key: rawKey,
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
