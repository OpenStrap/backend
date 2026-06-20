// dayseries.ts — RR loader for the HRV-from-minute path.
//
// [feat/wake-trigger] RR now rides the day-packed minute blob (minute_store) as a
// plain number[] per minute — no separate column, no per-blob decode. Reads via the
// tiered store (D1 hot + R2 sealed), so HRV at the wake-close still costs zero extra
// R2 for hot nights. Returns RR (ms) per minute over [from, to].

import { readMinutes, type StoreEnv } from './minute_store'

/** RR (ms) per minute over [from, to] from the day-packed store. Empty if none. */
export async function loadDayRr(
  env: StoreEnv, userId: string, from: number, to: number,
): Promise<Map<number, number[]>> {
  const map = new Map<number, number[]>()
  for (const m of await readMinutes(env, userId, from, to)) {
    if (m.rr && m.rr.length) map.set(m.ts_min, m.rr)
  }
  return map
}
