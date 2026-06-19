// minute_store.ts — [feat/wake-trigger] D1-hot / R2-sealed tiered minute storage.
//
// Hot days (≤ HOT_DAYS old) live row-per-minute in D1 — the ingest write path and
// processUser/wake read path are UNCHANGED (their windows are always within the hot
// days), so the corruption-sensitive paths never touch this module. Older "sealed"
// days are packed into ONE gzipped R2 object and removed from D1, which:
//   • cuts D1 minute storage (only ~3 days resident) and dodges the 10 GB-per-DB cap,
//   • replaces ~1440 per-row prune-DELETE writes/day with ONE R2 PUT (cheap),
//   • extends drill-down history to the R2 window (14 d) instead of the D1 prune (10 d).
// Reads for older days fall back to R2 (only the day-detail endpoints request them).
//
// SAFETY: sealDay PUTs to R2 and only DELETEs the D1 rows after the put succeeds
// (raw-first / never-drop-before-persist). pack/unpack are pure JSON → lossless.

import { decodeRr } from './ingest_signals'

export const HOT_DAYS = 3
const DAY = 86400
const ymd = (ts: number): string => new Date(ts * 1000).toISOString().slice(0, 10)
const dayStart = (d: string): number => Math.floor(Date.parse(`${d}T00:00:00Z`) / 1000)
const objKey = (userId: string, date: string): string => `minute/${userId}/${date}.json.gz`

export interface StoredMin {
  ts_min: number
  hr_avg: number | null; hr_min: number | null; hr_max: number | null; hr_n: number | null
  hr_sum?: number | null
  activity: number | null; act_sum?: number | null; act_n?: number | null
  steps: number | null; wrist_on: number | null
  rr?: number[] | null
}

interface StoreEnv { DB: D1Database; RAW_BUCKET?: R2Bucket }

const SELECT_COLS = 'ts_min, hr_avg, hr_min, hr_max, hr_n, hr_sum, activity, act_sum, act_n, steps, wrist_on, rr'

// ── gzip via the Workers CompressionStream (no extra deps) ────────────────────
async function gzip(s: string): Promise<Uint8Array> {
  const cs = new CompressionStream('gzip')
  const w = cs.writable.getWriter()
  void w.write(new TextEncoder().encode(s)); void w.close()
  return new Uint8Array(await new Response(cs.readable).arrayBuffer())
}
async function gunzip(b: ArrayBuffer): Promise<string> {
  const ds = new DecompressionStream('gzip')
  const w = ds.writable.getWriter()
  void w.write(new Uint8Array(b)); void w.close()
  return new TextDecoder().decode(await new Response(ds.readable).arrayBuffer())
}

// pack/unpack — pure, lossless, unit-testable.
export function packDay(mins: StoredMin[]): string { return JSON.stringify(mins) }
export function unpackDay(json: string): StoredMin[] {
  const v = JSON.parse(json)
  return Array.isArray(v) ? v as StoredMin[] : []
}

function rowToStored(r: any): StoredMin {
  return {
    ts_min: r.ts_min, hr_avg: r.hr_avg, hr_min: r.hr_min, hr_max: r.hr_max, hr_n: r.hr_n,
    hr_sum: r.hr_sum, activity: r.activity, act_sum: r.act_sum, act_n: r.act_n,
    steps: r.steps, wrist_on: r.wrist_on, rr: r.rr ? decodeRr(r.rr) : null,
  }
}

/** Seal ONE day: pack D1 rows → gzipped R2 object, then (only on success) drop the
 *  D1 rows. Idempotent: re-sealing an already-empty day is a no-op. */
export async function sealDay(env: StoreEnv, userId: string, date: string): Promise<{ sealed: boolean; n: number }> {
  if (!env.RAW_BUCKET) return { sealed: false, n: 0 }
  const start = dayStart(date)
  const { results } = await env.DB.prepare(
    `SELECT ${SELECT_COLS} FROM minute WHERE user_id = ? AND ts_min >= ? AND ts_min < ? ORDER BY ts_min ASC`,
  ).bind(userId, start, start + DAY).all<any>()
  const rows = results ?? []
  if (!rows.length) return { sealed: false, n: 0 }
  const gz = await gzip(packDay(rows.map(rowToStored)))
  await env.RAW_BUCKET.put(objKey(userId, date), gz, { httpMetadata: { contentEncoding: 'gzip' } })
  // R2 put confirmed → safe to drop the D1 rows.
  await env.DB.prepare('DELETE FROM minute WHERE user_id = ? AND ts_min >= ? AND ts_min < ?')
    .bind(userId, start, start + DAY).run()
  return { sealed: true, n: rows.length }
}

/** Read one sealed day from R2 (empty if not sealed / no bucket). */
export async function readSealedDay(env: StoreEnv, userId: string, date: string): Promise<StoredMin[]> {
  if (!env.RAW_BUCKET) return []
  const obj = await env.RAW_BUCKET.get(objKey(userId, date))
  if (!obj) return []
  try { return unpackDay(await gunzip(await obj.arrayBuffer())) } catch { return [] }
}

/** Read minutes over [from,to): D1 first, then R2 for any whole day in range that
 *  has no D1 rows (i.e. already sealed). Used by the day-detail read path. */
export async function readMinutes(env: StoreEnv, userId: string, from: number, to: number): Promise<StoredMin[]> {
  const { results } = await env.DB.prepare(
    `SELECT ${SELECT_COLS} FROM minute WHERE user_id = ? AND ts_min >= ? AND ts_min < ? ORDER BY ts_min ASC`,
  ).bind(userId, from, to).all<any>()
  const out = (results ?? []).map(rowToStored)
  const haveDays = new Set(out.map((m) => ymd(m.ts_min)))
  for (let t = Math.floor(from / DAY) * DAY; t < to; t += DAY) {
    const d = ymd(t)
    if (haveDays.has(d)) continue
    const sealed = await readSealedDay(env, userId, d)
    for (const m of sealed) if (m.ts_min >= from && m.ts_min < to) out.push(m)
  }
  out.sort((a, b) => a.ts_min - b.ts_min)
  return out
}

/** Seal every day older than HOT_DAYS that still has D1 rows (bounded per call).
 *  Runs in the nightly maintenance cron — replaces the blind prune-delete. */
export async function sealOldDays(env: StoreEnv, now = Math.floor(Date.now() / 1000), limit = 500): Promise<{ users: number; sealed: number }> {
  if (!env.RAW_BUCKET) return { users: 0, sealed: 0 }
  const cutoff = now - HOT_DAYS * DAY
  const { results } = await env.DB.prepare(
    "SELECT DISTINCT user_id, strftime('%Y-%m-%d', ts_min, 'unixepoch') AS d FROM minute WHERE ts_min < ? LIMIT ?",
  ).bind(cutoff, limit).all<{ user_id: string; d: string }>()
  let sealed = 0
  const users = new Set<string>()
  for (const r of results ?? []) {
    try { const res = await sealDay(env, r.user_id, r.d); if (res.sealed) { sealed++; users.add(r.user_id) } }
    catch (e) { console.error('sealDay failed', r.user_id, r.d, e) }
  }
  return { users: users.size, sealed }
}
