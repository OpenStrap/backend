// queue.ts — analytics queue consumer. Each message is one (user, job) UNIT of
// work, so every consumer invocation does a single bounded job for a single user
// and stays well under the per-invocation subrequest cap (1000 on Paid). This is
// what lets the heavy R2-re-decode jobs (biometrics/resp/steps) scale: the cron
// just enqueues; the consumer fans them out, one bounded unit per invocation.

import { processUser } from './analytics'
import { runBiometricsMinute } from './biometrics_minute'
import { invalidateDay } from './cache'

// [drop-r2] All HRV/resp/optical now derive from the D1 minute store at the wake-close
// (runBiometricsMinute). The old R2 re-decode jobs ('biometrics'/'resp') and their
// fallback trigger are gone. Steps are AN-2554, counted at ingest into the minute blob.
export type AnalyticsJob = 'sweep' | 'close_day'

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

// [free-tier] Close one physiological day for one user. Exported so the cron can run
// this INLINE when no queue is bound (Cloudflare Queues require Workers Paid). All work
// is D1 + (optional) R2 reads — well under the free plan's 1000 Cloudflare-subrequest
// cap for a single user. Mirrors the 'close_day' queue branch exactly.
export async function closeDay(env: QueueEnv, userId: string, day?: string, onset_ts?: number, wake_ts?: number): Promise<void> {
  // [wake-trigger] fired ONCE per physiological day when the user wakes. Derives
  // the day (sleep/naps/strain/sessions/baselines/coach via processUser) and folds
  // in HRV/recovery from D1 minute.rr — zero R2. Then invalidates the day's Tier-2
  // cache and clears the dirty flag (daytime ingests re-set it; the cron skips
  // awake-and-closed users by last_close_date, so no churn).
  // processUser derives the day AND folds daily.steps = SUM(minute.steps) (AN-2554
  // counted at ingest) — no separate step job.
  await processUser(env.DB, userId, { historyDays: 3 })
  if (day && wake_ts) {
    const from = onset_ts ?? (wake_ts - 8 * 3600)
    try { await runBiometricsMinute({ DB: env.DB, RAW_BUCKET: env.RAW_BUCKET }, userId, day, from, wake_ts + 60) } catch (e) { console.error('biometrics_minute failed', userId, day, e) }
    await invalidateDay(env.DB, userId, day)
  }
  await env.DB.prepare('UPDATE analytics_cursor SET dirty = 0 WHERE user_id = ?').bind(userId).run()
}

// Run one bounded unit of work. Each branch is sized to fit in a single
// invocation's budget (one user, one job, a few days of R2 at most).
async function runJob(env: QueueEnv, userId: string, job: AnalyticsJob, day?: string, onset_ts?: number, wake_ts?: number): Promise<void> {
  switch (job) {
    case 'close_day':
      await closeDay(env, userId, day, onset_ts, wake_ts)
      break
    case 'sweep':
    default:
      // The frequent path: derive daily/sleep/strain (D1); steps folded in by processUser.
      // HRV/resp/optical are derived at the wake-close (close_day → runBiometricsMinute),
      // not here — no R2 re-decode jobs anymore.
      await processUser(env.DB, userId, { historyDays: 3 })
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
