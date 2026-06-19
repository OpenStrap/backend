// dayseries.ts — minimal D1-only RR loader for the HRV-from-minute path.
//
// [feat/wake-trigger] Deliberately the *minimal* version: reads beat-to-beat RR
// straight from the D1 `minute.rr` BLOB column (populated at ingest by
// ingest_signals). NO R2 series cache / re-decode (that v3 machinery is left
// behind on purpose). HRV/recovery therefore costs zero R2 ops — it folds into the
// wake-triggered day-close. Returns a per-minute map keyed by ts_min.

import { decodeRr } from './ingest_signals'

interface DSEnv { DB: D1Database; RAW_BUCKET?: R2Bucket; MINUTE_SOURCE?: string }

/** RR (ms) per minute over [from, to] from D1 minute.rr. Empty map if none. */
export async function loadDayRr(
  env: DSEnv, userId: string, from: number, to: number,
): Promise<Map<number, number[]>> {
  const fromMin = Math.floor(from / 60) * 60
  const { results } = await env.DB.prepare(
    'SELECT ts_min, rr FROM minute WHERE user_id = ? AND ts_min >= ? AND ts_min <= ? AND rr IS NOT NULL ORDER BY ts_min',
  ).bind(userId, fromMin, to).all<{ ts_min: number; rr: ArrayBuffer | null }>()
  const map = new Map<number, number[]>()
  for (const r of results ?? []) {
    const rr = decodeRr(r.rr)
    if (rr.length) map.set(r.ts_min, rr)
  }
  return map
}
