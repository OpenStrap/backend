// wake_cron.ts — [feat/wake-trigger] the frequent cron's ONLY job: detect each
// user's real wake and fire close_day once per physiological day. Designed to be
// O(cheap) per tick so it's ~free even at */10:
//
//   ladder step 1  awake & already closed today  → cursor-only skip (no minute read)
//   ladder step 2  else                          → cheap peek of the last ~30 min
//   ladder step 3  peek says awake               → ONE full ensemble over 36h, enqueue
//
// The expensive ensemble + 36h read runs at most ONCE per user per day (at the wake);
// every other tick is a tiny cursor/30-min read. No analytics, no prune here.

import { detectWakeState, peekRecentState } from 'openstrap-analytics'
import { loadMinutes, loadBaseline } from './analytics'
import { loadDayRr } from './dayseries'
import { closeDay, type AnalyticsMessage } from './queue'

interface WakeEnv { DB: D1Database; RAW_BUCKET?: R2Bucket; ANALYTICS_Q?: Queue<AnalyticsMessage> }

const DAY = 86400
const ymd = (ts: number): string => new Date(ts * 1000).toISOString().slice(0, 10)

export async function runWakeLadder(
  env: WakeEnv, now = Math.floor(Date.now() / 1000),
): Promise<{ checked: number; closed: number }> {
  const today = ymd(now)
  // Candidates = users with fresh data (dirty). Awake-and-closed ones are filtered
  // by step 1 below at ~zero cost; the close_day job clears dirty when it runs.
  const { results } = await env.DB.prepare(
    'SELECT user_id, last_close_date FROM analytics_cursor WHERE dirty = 1 LIMIT 1000',
  ).all<{ user_id: string; last_close_date: string | null }>()

  let checked = 0, closed = 0
  for (const u of results ?? []) {
    // Step 1 — got this user's wake already today → skip (cursor-only, cheapest path).
    if (u.last_close_date === today) continue
    checked++

    const baseline = await loadBaseline(env.DB, u.user_id)

    // Step 2 — cheap, liberal peek over the last ~30 min (high recall on purpose).
    const recent = await loadMinutes(env.DB, u.user_id, now - 30 * 60, now + 60)
    const peek = peekRecentState(recent, baseline)
    if (peek !== 'awake') {
      // still asleep/unknown → mark phase ONLY on transition (no-op write = 0 rows billed).
      await env.DB.prepare(
        "UPDATE analytics_cursor SET sleep_phase = 'asleep', phase_since = ? WHERE user_id = ? AND COALESCE(sleep_phase,'') <> 'asleep'",
      ).bind(now, u.user_id).run()
      continue
    }

    // Step 3 — candidate wake confirmed by the cheap peek → run the FULL ensemble ONCE
    // over a generous 36h window (so the circadian fit isn't starved). RR enables the
    // cardiac arm; missing RR just drops that voter.
    const win = await loadMinutes(env.DB, u.user_id, now - 36 * 3600, now + 60)
    if (win.length < 60) continue
    const rrByMin = await loadDayRr(env, u.user_id, now - 36 * 3600, now + 60)
    const ws = detectWakeState({ minutes: win, baseline, rrByMin, now })
    if (!ws.wake_ts || ws.state !== 'awake') continue

    const dayLabel = ymd(ws.wake_ts)
    if (dayLabel === u.last_close_date) continue // already closed this physiological day

    // Set last_close_date NOW to dedupe re-enqueue; close_day clears dirty when done.
    await env.DB.prepare(
      "INSERT INTO analytics_cursor (user_id, last_close_date, sleep_phase, phase_since) VALUES (?,?,'awake',?) " +
      "ON CONFLICT(user_id) DO UPDATE SET last_close_date = excluded.last_close_date, sleep_phase = 'awake', phase_since = excluded.phase_since",
    ).bind(u.user_id, dayLabel, ws.wake_ts).run()

    if (env.ANALYTICS_Q) {
      await env.ANALYTICS_Q.send({
        user_id: u.user_id, job: 'close_day', day: dayLabel,
        onset_ts: ws.onset_ts ?? undefined, wake_ts: ws.wake_ts,
      })
    } else {
      // [free-tier] No queue bound (Workers Free): derive the day INLINE. At most one
      // user is closed per tick, so this stays well under the 1000 Cloudflare-subrequest
      // cap. Without this the day would be marked closed but never actually computed.
      try { await closeDay({ DB: env.DB, RAW_BUCKET: env.RAW_BUCKET as R2Bucket }, u.user_id, dayLabel, ws.onset_ts ?? undefined, ws.wake_ts) }
      catch (e) { console.error('inline close_day failed', u.user_id, dayLabel, e) }
    }
    closed++
  }
  return { checked, closed }
}

// Retry-net for the nightly maintenance tick: users with fresh data whose last close
// is stale (>~28h) — re-attempt a close so a missed/failed wake self-heals.
export async function retryStaleCloses(env: WakeEnv, now = Math.floor(Date.now() / 1000)): Promise<number> {
  const cutoff = ymd(now - 1 * DAY)
  const { results } = await env.DB.prepare(
    "SELECT user_id FROM analytics_cursor WHERE dirty = 1 AND (last_close_date IS NULL OR last_close_date < ?) LIMIT 1000",
  ).bind(cutoff).all<{ user_id: string }>()
  let n = 0
  for (const u of results ?? []) {
    if (env.ANALYTICS_Q) { await env.ANALYTICS_Q.send({ user_id: u.user_id, job: 'sweep' }); n++ }
    else {
      // [free-tier] No queue: re-derive inline as the nightly retry-net backstop.
      try { await closeDay({ DB: env.DB, RAW_BUCKET: env.RAW_BUCKET as R2Bucket }, u.user_id) } catch (e) { console.error('inline retry close failed', u.user_id, e) }
      n++
    }
  }
  return n
}
