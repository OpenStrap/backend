// queue.ts — analytics queue consumer. Each message is one (user, job) UNIT of
// work, so every consumer invocation does a single bounded job for a single user
// and stays well under the per-invocation subrequest cap (1000 on Paid). This is
// what lets the heavy R2-re-decode jobs (biometrics/resp/steps) scale: the cron
// just enqueues; the consumer fans them out, one bounded unit per invocation.

import { processUser } from './analytics'
import { runBiometrics } from './biometrics'
import { runBiometricsMinute } from './biometrics_minute'
import { runRespRate } from './resp'
import { runStepsImu, runStepsIncremental } from './steps_imu'
import { invalidateDay } from './cache'

export type AnalyticsJob = 'sweep' | 'biometrics' | 'resp' | 'steps_full' | 'close_day'

export interface AnalyticsMessage {
  user_id: string
  upto?: number
  job?: AnalyticsJob // default 'sweep'
  day?: string       // YYYY-MM-DD — per-(user,day) fan-out for heavy R2 jobs
  onset_ts?: number  // [wake-trigger] close_day: sleep onset (RR window start)
  wake_ts?: number   // [wake-trigger] close_day: wake (RR window end)
}

interface QueueEnv {
  DB: D1Database
  RAW_BUCKET: R2Bucket
  ANALYTICS_Q?: Queue<AnalyticsMessage>
}

// After a sweep, if a fresh night just finished (sleep with wake_ts, settled, for a
// date we haven't scored yet), enqueue biometrics + resp for it. This makes
// recovery/HRV appear shortly after each user WAKES — no timezone needed — instead
// of waiting for the fixed nightly run. Fires once per night (bio_last_date guard).
async function maybeTriggerBiometrics(env: QueueEnv, userId: string): Promise<void> {
  if (!env.ANALYTICS_Q) return
  const sleep = await env.DB.prepare(
    'SELECT date, wake_ts FROM sleep WHERE user_id = ? AND onset_ts IS NOT NULL AND wake_ts IS NOT NULL ORDER BY date DESC LIMIT 1',
  ).bind(userId).first<{ date: string; wake_ts: number }>()
  if (!sleep) return
  const now = Math.floor(Date.now() / 1000)
  if (sleep.wake_ts > now - 600) return // woke <10 min ago → let the night settle; next sweep gets it
  // Already have HRV for this night (nightly backstop or an earlier fire computed
  // it)? Nothing to do — never re-decode R2 for a night we've already scored.
  const have = await env.DB.prepare(
    'SELECT 1 FROM daily WHERE user_id = ? AND date = ? AND hrv_rmssd IS NOT NULL',
  ).bind(userId, sleep.date).first()
  if (have) return
  const cur = await env.DB.prepare('SELECT bio_last_date FROM analytics_cursor WHERE user_id = ?')
    .bind(userId).first<{ bio_last_date: string | null }>()
  // Cost cap: fire the heavy R2 decode at most ONCE per night from the event path.
  // If that fire yields null (partial/late R2), the nightly cron retries missing
  // nights — so we never spam the decode every 30-min sweep.
  if (cur?.bio_last_date && cur.bio_last_date >= sleep.date) return // already fired for this night
  await env.ANALYTICS_Q.send({ user_id: userId, job: 'biometrics', day: sleep.date })
  await env.ANALYTICS_Q.send({ user_id: userId, job: 'resp', day: sleep.date })
  await env.DB.prepare(
    'INSERT INTO analytics_cursor (user_id, bio_last_date) VALUES (?,?) ' +
    'ON CONFLICT(user_id) DO UPDATE SET bio_last_date = excluded.bio_last_date',
  ).bind(userId, sleep.date).run()
}

// Run one bounded unit of work. Each branch is sized to fit in a single
// invocation's budget (one user, one job, a few days of R2 at most).
async function runJob(env: QueueEnv, userId: string, job: AnalyticsJob, day?: string, onset_ts?: number, wake_ts?: number): Promise<void> {
  switch (job) {
    case 'close_day':
      // [wake-trigger] fired ONCE per physiological day when the user wakes. Derives
      // the day (sleep/naps/strain/sessions/baselines/coach via processUser) and folds
      // in HRV/recovery from D1 minute.rr — zero R2. Then invalidates the day's Tier-2
      // cache and clears the dirty flag (daytime ingests re-set it; the cron skips
      // awake-and-closed users by last_close_date, so no churn).
      await processUser(env.DB, userId, { historyDays: 3 })
      await runStepsIncremental(env, userId)
      if (day && wake_ts) {
        const from = onset_ts ?? (wake_ts - 8 * 3600)
        try { await runBiometricsMinute({ DB: env.DB, RAW_BUCKET: env.RAW_BUCKET }, userId, day, from, wake_ts + 60) } catch (e) { console.error('biometrics_minute failed', userId, day, e) }
        await invalidateDay(env.DB, userId, day)
      }
      await env.DB.prepare('UPDATE analytics_cursor SET dirty = 0 WHERE user_id = ?').bind(userId).run()
      break
    case 'biometrics':
      // HRV/temp/SpO₂ from RR (R2) for ONE day (day set) or the trailing window.
      await runBiometrics(env, userId, 3, day)
      // Legacy multi-day mode re-runs analytics so the coach picks up recovery; in
      // per-day mode the next 'sweep' does that (recovery is written either way).
      if (!day) await processUser(env.DB, userId, { historyDays: 2 })
      break
    case 'resp':
      await runRespRate(env, userId, 2, day)
      break
    case 'steps_full':
      // Full AN-2554 true-up for late-arriving frames (realigns the incremental cursor).
      await runStepsImu(env, userId, 2, day)
      break
    case 'sweep':
    default:
      // The frequent path: derive daily/sleep/strain (D1) + incremental steps (bounded R2).
      await processUser(env.DB, userId, { historyDays: 3 })
      await runStepsIncremental(env, userId)
      // Event-driven: if this sweep just finished a night, fire biometrics for it.
      await maybeTriggerBiometrics(env, userId)
      break
  }
}

export async function handleQueueBatch(
  batch: MessageBatch<AnalyticsMessage>,
  env: QueueEnv,
): Promise<void> {
  // Dedup identical (user, job) units within the batch.
  const groups = new Map<string, Message<AnalyticsMessage>[]>()
  for (const msg of batch.messages) {
    const uid = msg.body.user_id
    if (!uid) { msg.ack(); continue }
    const key = `${uid}::${msg.body.job ?? 'sweep'}::${msg.body.day ?? ''}`
    const arr = groups.get(key) ?? []
    arr.push(msg)
    groups.set(key, arr)
  }

  for (const [, msgs] of groups) {
    const uid = msgs[0].body.user_id
    const job = msgs[0].body.job ?? 'sweep'
    const day = msgs[0].body.day
    const { onset_ts, wake_ts } = msgs[0].body
    try {
      await runJob(env, uid, job, day, onset_ts, wake_ts)
      for (const m of msgs) m.ack()
    } catch (e) {
      console.error('queue: job failed', uid, job, day, e)
      // Retry the group (Cloudflare re-delivers; max_retries → DLQ).
      for (const m of msgs) m.retry()
    }
  }
}
