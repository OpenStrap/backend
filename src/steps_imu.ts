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

// Steps for one UTC day: assemble per-minute accel (ordered by ts,idx), then run
// the pure AN-2554 pedometer (analytics calcSteps) over the per-minute signals.
async function computeDaySteps(env: StepsEnv, userId: string, dayStart: number): Promise<number> {
  const from = dayStart, to = dayStart + DAY
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

/**
 * runStepsImu — recompute steps for the last `days` UTC days from R2 IMU and store
 * on daily.steps. Authoritative source of steps (analytics no longer needs to be).
 */
export async function runStepsImu(env: StepsEnv, userId: string, days = 1): Promise<{ days: number; total: number }> {
  const now = Math.floor(Date.now() / 1000)
  let grand = 0
  for (let d = 0; d < days; d++) {
    const dayStart = Math.floor((now - d * DAY) / DAY) * DAY
    const date = new Date(dayStart * 1000).toISOString().slice(0, 10)
    const steps = await computeDaySteps(env, userId, dayStart)
    await env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?,?)').bind(userId, date).run()
    await env.DB.prepare('UPDATE daily SET steps = ? WHERE user_id = ? AND date = ?')
      .bind(steps, userId, date).run()
    grand += steps
  }
  return { days, total: grand }
}
