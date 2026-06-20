// cache.ts — [feat/wake-trigger] simple TTL read-cache for Tier 1/2 on-read metrics.
//
// NO watermark. Invalidation is purely time-based: "today" gets a short TTL (~60s,
// the 1-min rollup granularity, so it stays near-live without recomputing on every
// request), a completed past day is effectively immutable until its minutes prune.
// This sidesteps the v2 day_version watermark that re-fired every minute as 1 Hz
// data rolled in. Tier 3/4 are NOT cached here — they're served from `daily`.

/** Serve `key` from read_cache if fresh (< ttlSec old); else compute, store, return. */
export async function cached<T>(
  db: D1Database, userId: string, key: string, ttlSec: number, compute: () => Promise<T>,
): Promise<T> {
  const now = Math.floor(Date.now() / 1000)
  const row = await db.prepare(
    'SELECT payload, computed_at FROM read_cache WHERE user_id = ? AND key = ?',
  ).bind(userId, key).first<{ payload: string; computed_at: number }>()
  if (row && now - row.computed_at < ttlSec) {
    try { return JSON.parse(row.payload) as T } catch { /* corrupt → recompute */ }
  }
  const val = await compute()
  await db.prepare(
    'INSERT INTO read_cache (user_id, key, payload, computed_at) VALUES (?,?,?,?) ' +
    'ON CONFLICT(user_id, key) DO UPDATE SET payload = excluded.payload, computed_at = excluded.computed_at',
  ).bind(userId, key, JSON.stringify(val), now).run()
  return val
}

/** TTL by date: today (UTC) is live (60s); a past day is immutable-until-prune. */
export function ttlForDate(ymd: string, nowSec = Math.floor(Date.now() / 1000)): number {
  const today = new Date(nowSec * 1000).toISOString().slice(0, 10)
  return ymd >= today ? 60 : 86400
}

/** Invalidate a day's cached Tier-2 entries (called by the day-close). */
export async function invalidateDay(db: D1Database, userId: string, ymd: string): Promise<void> {
  await db.prepare("DELETE FROM read_cache WHERE user_id = ? AND key LIKE ?").bind(userId, `%:${ymd}`).run()
}
