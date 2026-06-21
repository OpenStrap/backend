// minute_store.ts — [feat/wake-trigger] THE single minute-storage layer.
//
// Storage model: ONE row per (user, ymd) in `minute_day`, value = a gzipped JSON
// array of MinuteRec (one entry per touched minute, keyed by ts_min). This replaces
// the old row-per-minute `minute` table — so ingest writes ~1 row/day instead of
// ~1,440, and the 10-day prune deletes ~1 row/day instead of ~1,440 (the big D1
// write-cost cut). Reads unpack the blob to the same MinuteRec[] every caller used.
//
// Tiering: hot days live in D1 `minute_day`; days older than HOT_DAYS are sealed —
// the SAME gzipped blob is moved to a gzipped R2 object and the D1 row dropped.
// readDay/readMinutes fall back to R2 for sealed days.
//
// D1 limits respected: blob is gzipped (~150–250 KB worst-case day, << the 2 MB
// value cap) and ALWAYS written via a bound parameter (the 100 KB SQL-statement
// limit is for SQL text, not bound-param values).
//
// Concurrency: writeBatch is read-merge-write, NOT atomic. Safe because a user has
// ONE band syncing serially (the edge uploader awaits each batch). Two concurrent
// writers for the same (user, ymd) could lose an update — not a real scenario here.

import type { MinuteBucket } from './rollup'

export const HOT_DAYS = 3
const DAY = 86400
const ymd = (ts: number): string => new Date(ts * 1000).toISOString().slice(0, 10)
const dayStart = (d: string): number => Math.floor(Date.parse(`${d}T00:00:00Z`) / 1000)
const objKey = (userId: string, date: string): string => `minute/${userId}/${date}.json.gz`

/** One minute's stored aggregate (running sums kept so merges stay exact). */
export interface MinuteRec {
  ts_min: number
  hr_avg: number; hr_min: number; hr_max: number; hr_n: number; hr_sum: number
  activity: number; act_sum: number; act_n: number
  steps: number; wrist_on: number
  rr: number[]
  // Optical aggregates (wrist-on R24 only) — running sums + count so merges stay
  // exact and re-uploads can't double-count (edge dedupes by hex). RELATIVE raw ADCs;
  // SpO₂/°C are derived in the close path, never on-band. Optional (older blobs lack them).
  opt_n?: number; red_sum?: number; ir_sum?: number; temp_sum?: number
  // PPG RIIV proxy: per-second mean of the R21 green channel (the only value
  // estimateResp consumes). Present only during live optical sessions (R21 is
  // live-stream-only), so usually empty. Replaces the R2 raw store for resp —
  // resp is computed from this series at the wake-close. Optional (older blobs lack it).
  green?: number[]
  // Dominant HAR activity class for this minute, classified at ingest from the live
  // high-rate accel (Mannini, see openstrap-analytics/har). One tiny enum string — NOT
  // the raw samples. Present only for live-streamed minutes (flash R24 is 1 Hz → none).
  // Feeds workout typing + segmentation in detectSessions. Optional (older blobs lack it).
  act_class?: string
}

export interface StoreEnv { DB: D1Database; RAW_BUCKET?: R2Bucket }

// ── gzip via Workers CompressionStream ────────────────────────────────────────
async function gzip(s: string): Promise<Uint8Array> {
  const cs = new CompressionStream('gzip')
  const w = cs.writable.getWriter(); void w.write(new TextEncoder().encode(s)); void w.close()
  return new Uint8Array(await new Response(cs.readable).arrayBuffer())
}
async function gunzip(b: ArrayBuffer): Promise<string> {
  const ds = new DecompressionStream('gzip')
  const w = ds.writable.getWriter(); void w.write(new Uint8Array(b)); void w.close()
  return new TextDecoder().decode(await new Response(ds.readable).arrayBuffer())
}

function zeroRec(ts_min: number): MinuteRec {
  return { ts_min, hr_avg: 0, hr_min: 0, hr_max: 0, hr_n: 0, hr_sum: 0, activity: 0, act_sum: 0, act_n: 0, steps: 0, wrist_on: 0, rr: [] }
}

// pack/unpack — pure, lossless JSON.
export function packDay(recs: MinuteRec[]): MinuteRec[] { return recs }
function serialize(map: Map<number, MinuteRec>): string {
  return JSON.stringify([...map.values()].sort((a, b) => a.ts_min - b.ts_min))
}
function deserialize(json: string): Map<number, MinuteRec> {
  const out = new Map<number, MinuteRec>()
  try { const arr = JSON.parse(json); if (Array.isArray(arr)) for (const r of arr) if (r && typeof r.ts_min === 'number') out.set(r.ts_min, r as MinuteRec) } catch { /* corrupt → empty */ }
  return out
}

/** Read ONE day's minutes as a map: D1 minute_day (hot) → else R2 sealed → empty. */
export async function readDay(env: StoreEnv, userId: string, date: string): Promise<Map<number, MinuteRec>> {
  const row = await env.DB.prepare('SELECT blob FROM minute_day WHERE user_id = ? AND ymd = ?')
    .bind(userId, date).first<{ blob: ArrayBuffer | null }>()
  if (row?.blob) return deserialize(await gunzip(row.blob))
  if (env.RAW_BUCKET) {
    const obj = await env.RAW_BUCKET.get(objKey(userId, date))
    if (obj) { try { return deserialize(await gunzip(await obj.arrayBuffer())) } catch { /* fall through */ } }
  }
  return new Map()
}

/** Write ONE day's map back to D1 minute_day (gzip + bound param). */
async function writeDay(env: StoreEnv, userId: string, date: string, map: Map<number, MinuteRec>, now: number): Promise<void> {
  if (map.size === 0) return
  const gz = await gzip(serialize(map))
  await env.DB.prepare(
    'INSERT INTO minute_day (user_id, ymd, blob, updated_at) VALUES (?,?,?,?) ' +
    'ON CONFLICT(user_id, ymd) DO UPDATE SET blob = excluded.blob, updated_at = excluded.updated_at',
  ).bind(userId, date, gz, now).run()
}

/** Read minutes over [from, to): unpack each day in range (D1 hot + R2 sealed). */
export async function readMinutes(env: StoreEnv, userId: string, from: number, to: number): Promise<MinuteRec[]> {
  const out: MinuteRec[] = []
  for (let t = Math.floor(from / DAY) * DAY; t < to; t += DAY) {
    const day = await readDay(env, userId, ymd(t))
    for (const r of day.values()) if (r.ts_min >= from && r.ts_min < to) out.push(r)
  }
  out.sort((a, b) => a.ts_min - b.ts_min)
  return out
}

/** Latest minute at/after `sinceTs` (for "current HR"): scans today's + sinceTs's day. */
export async function latestMinute(env: StoreEnv, userId: string, sinceTs: number, now: number): Promise<MinuteRec | null> {
  let best: MinuteRec | null = null
  for (let t = Math.floor(sinceTs / DAY) * DAY; t <= now; t += DAY) {
    for (const r of (await readDay(env, userId, ymd(t))).values()) {
      if (r.ts_min >= sinceTs && (!best || r.ts_min > best.ts_min)) best = r
    }
  }
  return best
}

/**
 * Fold a batch's per-minute buckets (+ steps/rr signals) into the day blob(s),
 * RMW per touched day. Merge mirrors the old `minute` ON CONFLICT EXACTLY:
 * hr/act sums + counts add, hr_min CASE-guarded for 0, hr_max MAX, steps add,
 * rr keeps the longer (fuller) array, wrist_on MAX; hr_avg/activity recomputed.
 * Idempotent for re-uploaded identical batches IFF the edge dedupes by hex (it does).
 */
export async function writeBatch(
  env: StoreEnv, userId: string,
  buckets: MinuteBucket[],
  signals: Map<number, { steps: number; rr: number[]; opt_n?: number; red_sum?: number; ir_sum?: number; temp_sum?: number; green?: number[]; act_class?: string }>,
  now: number,
): Promise<number> {
  if (buckets.length === 0) return 0
  // group buckets by ymd
  const byDay = new Map<string, MinuteBucket[]>()
  for (const b of buckets) { const d = ymd(b.ts_min); (byDay.get(d) ?? byDay.set(d, []).get(d)!).push(b) }
  let daysWritten = 0
  for (const [date, dayBuckets] of byDay) {
    const map = await readDay(env, userId, date) // hot (D1) or, rare re-drain, sealed (R2)
    for (const b of dayBuckets) {
      const rec = map.get(b.ts_min) ?? zeroRec(b.ts_min)
      rec.hr_sum += b.hr_sum
      rec.hr_n += b.hr_n
      rec.hr_min = rec.hr_min === 0 ? b.hr_min : (b.hr_min === 0 ? rec.hr_min : Math.min(rec.hr_min, b.hr_min))
      rec.hr_max = Math.max(rec.hr_max, b.hr_max)
      rec.act_sum += b.act_sum
      rec.act_n += b.act_n
      rec.wrist_on = Math.max(rec.wrist_on, b.wrist_on)
      // Steps: accumulate the AN-2554 per-minute count from the ingest signals — the
      // SINGLE step source now (the per-record r10Motion heuristic and the R2 steps_imu
      // recompute are both gone). Additive across batches; idempotent because the edge
      // dedupes by raw hex, so a minute's frames are never counted twice.
      const sig = signals.get(b.ts_min)
      rec.steps += sig?.steps ?? 0
      const newRr = sig?.rr ?? []
      if (newRr.length >= rec.rr.length) rec.rr = newRr
      // PPG green RIIV proxy: same "keep the fuller array" idempotency as rr.
      const newGreen = sig?.green ?? []
      if (newGreen.length >= (rec.green?.length ?? 0)) rec.green = newGreen
      // HAR activity class (live minutes only): keep when present. Recompute on re-upload
      // is deterministic, so a re-drained batch yields the same label.
      if (sig?.act_class) rec.act_class = sig.act_class
      // Optical: additive sums + count (same idempotency basis as steps — edge hex-dedup).
      if (sig?.opt_n) {
        rec.opt_n = (rec.opt_n ?? 0) + sig.opt_n
        rec.red_sum = (rec.red_sum ?? 0) + (sig.red_sum ?? 0)
        rec.ir_sum = (rec.ir_sum ?? 0) + (sig.ir_sum ?? 0)
        rec.temp_sum = (rec.temp_sum ?? 0) + (sig.temp_sum ?? 0)
      }
      rec.hr_avg = rec.hr_n > 0 ? Math.round(rec.hr_sum / rec.hr_n) : 0
      rec.activity = rec.act_n > 0 ? rec.act_sum / rec.act_n : 0
      map.set(b.ts_min, rec)
    }
    await writeDay(env, userId, date, map, now)
    daysWritten++
  }
  return daysWritten
}

/** Seal days older than HOT_DAYS: move the D1 day blob → gzipped R2 object, drop the
 *  D1 row (the blob is already in the seal format). Bounded per call. */
export async function sealOldDays(env: StoreEnv, now = Math.floor(Date.now() / 1000), limit = 1000): Promise<{ sealed: number }> {
  if (!env.RAW_BUCKET) return { sealed: 0 }
  const cutoff = ymd(now - HOT_DAYS * DAY)
  const { results } = await env.DB.prepare(
    'SELECT user_id, ymd, blob FROM minute_day WHERE ymd < ? LIMIT ?',
  ).bind(cutoff, limit).all<{ user_id: string; ymd: string; blob: ArrayBuffer }>()
  let sealed = 0
  for (const r of results ?? []) {
    try {
      await env.RAW_BUCKET.put(objKey(r.user_id, r.ymd), r.blob, { httpMetadata: { contentEncoding: 'gzip' } })
      await env.DB.prepare('DELETE FROM minute_day WHERE user_id = ? AND ymd = ?').bind(r.user_id, r.ymd).run()
      sealed++
    } catch (e) { console.error('sealDay failed', r.user_id, r.ymd, e) }
  }
  return { sealed }
}

/** Seed/admin: write MinuteRec[] straight into the day-packed store (groups by ymd). */
export async function putDay(env: StoreEnv, userId: string, recs: MinuteRec[], now: number): Promise<void> {
  const byDay = new Map<string, Map<number, MinuteRec>>()
  for (const r of recs) {
    const d = ymd(r.ts_min)
    let m = byDay.get(d); if (!m) { m = new Map(); byDay.set(d, m) }
    m.set(r.ts_min, r)
  }
  for (const [d, m] of byDay) await writeDay(env, userId, d, m, now)
}

/** Backstop retention delete: drop any minute_day rows older than `days` still in D1. */
export async function pruneMinuteDays(env: StoreEnv, now: number, days: number): Promise<void> {
  await env.DB.prepare('DELETE FROM minute_day WHERE ymd < ?').bind(ymd(now - days * DAY)).run()
}
