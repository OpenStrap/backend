// steps_imu.ts — the STEPS RUNNER (I/O only). The band has no pedometer field, so
// steps are derived from the wrist accelerometer. This file does the heavy,
// binding-dependent work — list + read the raw IMU frames from R2, dedup, order,
// group per minute — then hands the per-minute magnitude signals to the PURE
// AN-2554 pedometer in openstrap-analytics (calcSteps) and persists the result.
//
// Mirrors the runBiometrics / runRespRate pattern: decode (decode.ts) + math
// (analytics) live elsewhere; this owns only R2 reads and the daily.steps write.
//
// Heavy (R2 reads) → cron / admin only, NEVER inline ingest. Owns daily.steps
// (written AFTER analytics in the cron so it's authoritative).

import { calcSteps } from 'openstrap-analytics'
import { frameAccel, type ImuFrame } from './decode'

const DAY = 86400

interface StepsEnv { DB: D1Database; RAW_BUCKET: R2Bucket }

async function rawKeysInWindow(bucket: R2Bucket, userId: string, from: number, to: number): Promise<string[]> {
  const out: string[] = []
  let cursor: string | undefined
  do {
    const listing = await bucket.list({ prefix: `raw/${userId}/`, cursor, limit: 1000 })
    for (const o of listing.objects) {
      const m = o.key.match(/-(\d+)-(\d+)\.txt$/)
      if (!m) { out.push(o.key); continue }
      if (parseInt(m[2]) >= from && parseInt(m[1]) <= to) out.push(o.key)
    }
    cursor = listing.truncated ? listing.cursor : undefined
  } while (cursor)
  return out
}

// Steps over an arbitrary [from, to) window: assemble per-minute accel (ordered by
// ts,idx), then run the pure AN-2554 pedometer (analytics calcSteps). calcSteps is
// per-minute-independent (it sums pedometer(minute) × GAIN), so the count over a
// window is exactly the sum of the counts over any partition of it into whole
// minutes — which is what makes incremental accumulation exact.
async function computeWindowSteps(env: StepsEnv, userId: string, from: number, to: number): Promise<number> {
  const keys = await rawKeysInWindow(env.RAW_BUCKET, userId, from, to)
  // DEDUP by (ts, idx): R2 upload windows overlap, so the same frame appears in
  // multiple objects — without this, samples (and steps) are double-counted.
  const seen = new Map<string, ImuFrame>()
  for (const key of keys) {
    const obj = await env.RAW_BUCKET.get(key)
    if (!obj) continue
    const text = await obj.text()
    for (const line of text.split('\n')) {
      if (!line) continue
      const f = frameAccel(line)
      if (!f || f.ts < from || f.ts >= to) continue
      seen.set(`${f.ts}:${f.idx}`, f)
    }
  }
  // Group frames per minute, ordered by (ts, idx), into one contiguous signal each.
  const byMin = new Map<number, ImuFrame[]>()
  for (const f of seen.values()) {
    const m = Math.floor(f.ts / 60) * 60
    const arr = byMin.get(m); if (arr) arr.push(f); else byMin.set(m, [f])
  }
  const minuteSignals: number[][] = []
  for (const frames of byMin.values()) {
    frames.sort((a, b) => a.ts - b.ts || a.idx - b.idx)
    const sig: number[] = []
    for (const f of frames) for (const v of f.mags) sig.push(v)
    minuteSignals.push(sig)
  }
  return calcSteps(minuteSignals)
}

// Minutes are considered "settled" (safe to count incrementally) once they're this
// old, giving late/bursty band syncs time to land. Anything later is caught by the
// nightly full recompute. 30 min matches the sweep cadence.
const SETTLE_GRACE = 1800

function ymd(dayStart: number): string {
  return new Date(dayStart * 1000).toISOString().slice(0, 10)
}

/**
 * runStepsIncremental — the 30-min-sweep path. Counts only the minutes that have
 * newly SETTLED since the last run (a few R2 objects) and accumulates into
 * daily.steps via a per-user cursor. Exact for settled data (per-minute pedometer);
 * the nightly full recompute trues up any late-arriving frames. Cheap: bounded R2
 * reads per run regardless of how long the day is or how many users.
 */
export async function runStepsIncremental(env: StepsEnv, userId: string): Promise<{ added: number }> {
  const now = Math.floor(Date.now() / 1000)
  const todayStart = Math.floor(now / DAY) * DAY
  const date = ymd(todayStart)
  const settledCutoff = Math.floor((now - SETTLE_GRACE) / 60) * 60 // last fully-settled minute boundary

  const cur = await env.DB.prepare(
    'SELECT steps_cursor_ts, steps_cursor_day FROM analytics_cursor WHERE user_id = ?',
  ).bind(userId).first<{ steps_cursor_ts: number | null; steps_cursor_day: string | null }>()
  const sameDay = !!cur && cur.steps_cursor_day === date && cur.steps_cursor_ts != null
  const cursor = sameDay ? cur!.steps_cursor_ts! : todayStart

  const setCursor = env.DB.prepare(
    'INSERT INTO analytics_cursor (user_id, steps_cursor_ts, steps_cursor_day) VALUES (?,?,?) ' +
    'ON CONFLICT(user_id) DO UPDATE SET steps_cursor_ts=excluded.steps_cursor_ts, steps_cursor_day=excluded.steps_cursor_day',
  )

  if (settledCutoff <= cursor) {
    // Nothing newly settled. On a fresh day, still seed the row at 0 + set cursor.
    if (!sameDay) {
      await env.DB.batch([
        env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?,?)').bind(userId, date),
        env.DB.prepare('UPDATE daily SET steps = 0 WHERE user_id = ? AND date = ?').bind(userId, date),
        setCursor.bind(userId, todayStart, date),
      ])
    }
    return { added: 0 }
  }

  const chunk = await computeWindowSteps(env, userId, cursor, settledCutoff)
  if (sameDay) {
    // Compare-and-swap on the cursor: apply this chunk AND advance the cursor only
    // if no concurrent writer moved the cursor since we read it (line above). A
    // nightly full recompute (runStepsImu, the 'steps_full' job) SETs today's steps
    // and the cursor to settledCutoff; it and this incremental 'sweep' are distinct
    // queue jobs that aren't deduped against each other, so they CAN run together
    // (both are enqueued by the 3:30 cron). Without the guard, a full landing
    // between our cursor read and our slow R2 compute would let us add a stale chunk
    // on top of the full count → double-count for the day. If the cursor moved, our
    // chunk is stale: both statements no-op and the next sweep recomputes from the
    // new cursor. D1 batch is one serialized transaction, so the guard is reliable.
    await env.DB.batch([
      env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?,?)').bind(userId, date),
      env.DB.prepare(
        'UPDATE daily SET steps = COALESCE(steps,0) + ? WHERE user_id = ? AND date = ? ' +
        'AND (SELECT steps_cursor_ts FROM analytics_cursor WHERE user_id = ?) = ?',
      ).bind(chunk, userId, date, userId, cursor),
      env.DB.prepare(
        'UPDATE analytics_cursor SET steps_cursor_ts = ? WHERE user_id = ? AND steps_cursor_day = ? AND steps_cursor_ts = ?',
      ).bind(settledCutoff, userId, date, cursor),
    ])
  } else {
    // Fresh day: REPLACE (not accumulate) with the full settled count, which is
    // idempotent with a concurrent full recompute (both write the same window).
    await env.DB.batch([
      env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?,?)').bind(userId, date),
      env.DB.prepare('UPDATE daily SET steps = ? WHERE user_id = ? AND date = ?').bind(chunk, userId, date),
      setCursor.bind(userId, settledCutoff, date),
    ])
  }
  return { added: chunk }
}

/**
 * runStepsImu — FULL recompute for the last `days` UTC days from R2 IMU. The
 * authoritative true-up: catches late-arriving frames the incremental path may
 * have missed. For TODAY it also realigns the incremental cursor so the sweep
 * continues from the trued-up baseline without double-counting.
 */
export async function runStepsImu(env: StepsEnv, userId: string, days = 1, onlyDate?: string): Promise<{ days: number; total: number }> {
  const now = Math.floor(Date.now() / 1000)
  const todayStart = Math.floor(now / DAY) * DAY
  const settledCutoff = Math.floor((now - SETTLE_GRACE) / 60) * 60
  let grand = 0
  // Per-(user,day) fan-out: onlyDate → just that day; else the trailing `days`.
  const dayStarts = onlyDate
    ? [Math.floor(Date.parse(`${onlyDate}T00:00:00Z`) / 1000)]
    : Array.from({ length: days }, (_, d) => Math.floor((now - d * DAY) / DAY) * DAY)
  for (const dayStart of dayStarts) {
    const date = ymd(dayStart)
    if (dayStart === todayStart) {
      // Today: count only settled minutes (consistent with the incremental path)
      // and realign the cursor so the next sweep appends from here.
      const steps = await computeWindowSteps(env, userId, todayStart, settledCutoff)
      await env.DB.batch([
        env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?,?)').bind(userId, date),
        env.DB.prepare('UPDATE daily SET steps = ? WHERE user_id = ? AND date = ?').bind(steps, userId, date),
        env.DB.prepare(
          'INSERT INTO analytics_cursor (user_id, steps_cursor_ts, steps_cursor_day) VALUES (?,?,?) ' +
          'ON CONFLICT(user_id) DO UPDATE SET steps_cursor_ts=excluded.steps_cursor_ts, steps_cursor_day=excluded.steps_cursor_day',
        ).bind(userId, settledCutoff, date),
      ])
      grand += steps
    } else {
      // Past days are fully settled → count the whole day.
      const steps = await computeWindowSteps(env, userId, dayStart, dayStart + DAY)
      await env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?,?)').bind(userId, date).run()
      await env.DB.prepare('UPDATE daily SET steps = ? WHERE user_id = ? AND date = ?')
        .bind(steps, userId, date).run()
      grand += steps
    }
  }
  return { days, total: grand }
}
