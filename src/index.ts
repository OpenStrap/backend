import { Hono } from 'hono'
import {
  signJwt, verifyJwt, sha256Hex, generateOtp, randomToken, uuid, sendOtpEmail,
} from './auth'
import { runAnalytics, processUser } from './analytics'
import { ingestBatch, ingestEvents } from './ingest'
import { handleQueueBatch, type AnalyticsMessage, type AnalyticsJob } from './queue'
import { getToday, getSleep, getSleepV2, getStrain, getSessions, getTrends, getChart } from './query'
import { getHistory } from './history'
import { postJournal, getJournal, getJournalInsights } from './journal'
import { getDayStrain, getDaySleep, getDaySleepV2, getDayTimeline, getDayStress, getDayHeart, getDayLungs, getDayWear } from './daydetail'
import { getTrend } from './trend'
import { runStepsImu, runStepsIncremental } from './steps_imu'
import { workoutStart, workoutEnd, listWorkouts, getWorkout, deleteWorkout, autoCloseStaleWorkouts } from './workouts'
import { getRecords } from './records'
import { getNotifications, markNotificationsRead } from './notifications'
import { runRespRate } from './resp'
import { runBiometrics } from './biometrics'
import { getAppStatus, adminGetConfig, adminSetConfig } from './appconfig'
import { seedInit, seedMinutes, seedAnalytics } from './seed'

type Bindings = {
  DB: D1Database
  RAW_BUCKET: R2Bucket
  ANALYTICS_Q?: Queue<AnalyticsMessage>
  JWT_SECRET: string
  ADMIN_TOKEN?: string
  BREVO_API_KEY?: string
  RESEND_API_KEY?: string
  EMAIL_FROM?: string
  EMAIL_FROM_NAME?: string
  RESEND_FROM?: string
}

type Vars = { userId: string }

const ACCESS_TTL = 24 * 60 * 60          // 24h
const REFRESH_TTL = 30 * 24 * 60 * 60    // 30d
const OTP_TTL = 10 * 60                   // 10m
const OTP_MAX_ATTEMPTS = 5
const DAY = 86400
// Per-minute rollups are kept this long. The app shows minute-level detail
// (hypnogram, 24h timelines, wear, workout HR curve) for the last 7 days; this +3
// buffer avoids a boundary race at the edge of that window. Derived metrics
// (daily/sleep/sessions) are permanent — pruning minutes never affects them.
const MINUTE_RETENTION_DAYS = 10

const app = new Hono<{ Bindings: Bindings; Variables: Vars }>()

// ---------- middleware ----------
const requireJwt = async (c: any, next: any) => {
  const auth = c.req.header('Authorization')
  if (!auth?.startsWith('Bearer ')) return c.json({ error: 'Unauthorized' }, 401)
  const payload = await verifyJwt(auth.slice(7), c.env.JWT_SECRET)
  if (!payload || payload.typ === 'refresh') return c.json({ error: 'Unauthorized' }, 401)
  c.set('userId', payload.sub)
  await next()
}

const requireAdmin = async (c: any, next: any) => {
  const auth = c.req.header('Authorization')
  if (!c.env.ADMIN_TOKEN || auth !== `Bearer ${c.env.ADMIN_TOKEN}`) {
    return c.json({ error: 'Unauthorized' }, 401)
  }
  await next()
}

app.use('/ingest/*', requireJwt)
app.use('/profile', requireJwt)
app.use('/today', requireJwt)
app.use('/sleep', requireJwt)
app.use('/sleep/v2', requireJwt)
app.use('/strain', requireJwt)
app.use('/sessions', requireJwt)
app.use('/trends', requireJwt)
app.use('/chart', requireJwt)
app.use('/history', requireJwt)
app.use('/journal', requireJwt)
app.use('/journal/*', requireJwt)
app.use('/day/*', requireJwt)
app.use('/trend/*', requireJwt)
app.use('/workout/*', requireJwt)
app.use('/workouts', requireJwt)
app.use('/records', requireJwt)
app.use('/notifications', requireJwt)
app.use('/notifications/*', requireJwt)
app.use('/admin/*', requireAdmin)

app.get('/', (c) => c.json({ ok: true, service: 'openstrap-backend', ts: Math.floor(Date.now() / 1000) }))

// Public app status (OTA update pointer + admin alert banner). No JWT: the update
// prompt and a "service down" notice must reach clients even with an expired session.
app.get('/app/status', getAppStatus)

// ========================= AUTH =========================

app.post('/auth/register', async (c) => {
  const { name, email, age, height_cm, weight_kg, sex } =
    await c.req.json<{ name?: string; email: string; age?: number; height_cm?: number; weight_kg?: number; sex?: string }>()
  if (!email || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
    return c.json({ error: 'Valid email required' }, 400)
  }
  const e = email.toLowerCase().trim()
  const sexVal = sex === 'm' || sex === 'f' ? sex : null

  // Atomic dedupe-by-email: existing email ALWAYS keeps its user_id (the uuid
  // below is only used on a genuine first insert). Prevents re-registration
  // from ever minting a new id and orphaning a user's data. email is UNIQUE.
  await c.env.DB.prepare(
    'INSERT INTO users (id, email, name, age, height_cm, weight_kg, sex, created_at) ' +
    'VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(email) DO UPDATE SET ' +
    'name=COALESCE(excluded.name, users.name), age=COALESCE(excluded.age, users.age), ' +
    'height_cm=COALESCE(excluded.height_cm, users.height_cm), ' +
    'weight_kg=COALESCE(excluded.weight_kg, users.weight_kg), ' +
    'sex=COALESCE(excluded.sex, users.sex)',
  ).bind(uuid(), e, name ?? null, age ?? null, height_cm ?? null, weight_kg ?? null, sexVal,
    Math.floor(Date.now() / 1000)).run()
  return issueOtp(c, e)
})

app.post('/auth/request-otp', async (c) => {
  const { email } = await c.req.json<{ email: string }>()
  if (!email) return c.json({ error: 'Email required' }, 400)
  const e = email.toLowerCase().trim()
  const user = await c.env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(e).first()
  if (!user) return c.json({ error: 'No account for that email' }, 404)
  return issueOtp(c, e)
})

async function issueOtp(c: any, email: string) {
  const code = generateOtp()
  const codeHash = await sha256Hex(code)
  const expires = Math.floor(Date.now() / 1000) + OTP_TTL
  await c.env.DB.prepare(
    'INSERT INTO otps (email, code_hash, expires_at, attempts) VALUES (?,?,?,0) ' +
    'ON CONFLICT(email) DO UPDATE SET code_hash=excluded.code_hash, expires_at=excluded.expires_at, attempts=0',
  ).bind(email, codeHash, expires).run()
  const sent = await sendOtpEmail(c.env, email, code)
  return c.json({ ok: true, ...(sent.dev_code ? { dev_code: sent.dev_code } : {}) })
}

app.post('/auth/verify-otp', async (c) => {
  const { email, code } = await c.req.json<{ email: string; code: string }>()
  if (!email || !code) return c.json({ error: 'Email and code required' }, 400)
  const e = email.toLowerCase().trim()

  const row = await c.env.DB.prepare(
    'SELECT code_hash, expires_at, attempts FROM otps WHERE email = ?',
  ).bind(e).first<{ code_hash: string; expires_at: number; attempts: number }>()
  if (!row) return c.json({ error: 'No pending code' }, 400)
  if (row.attempts >= OTP_MAX_ATTEMPTS) return c.json({ error: 'Too many attempts' }, 429)
  if (row.expires_at < Math.floor(Date.now() / 1000)) return c.json({ error: 'Code expired' }, 400)

  if ((await sha256Hex(code)) !== row.code_hash) {
    await c.env.DB.prepare('UPDATE otps SET attempts = attempts + 1 WHERE email = ?').bind(e).run()
    return c.json({ error: 'Incorrect code' }, 400)
  }

  const user = await c.env.DB.prepare(
    'SELECT id, email, name, age, height_cm, weight_kg, sex FROM users WHERE email = ?',
  ).bind(e).first<any>()
  if (!user) return c.json({ error: 'No account' }, 404)

  await c.env.DB.prepare('DELETE FROM otps WHERE email = ?').bind(e).run()
  return c.json(await issueSession(c, user))
})

async function issueSession(c: any, user: any) {
  const access = await signJwt({ sub: user.id }, c.env.JWT_SECRET, ACCESS_TTL)
  const refresh = randomToken()
  await c.env.DB.prepare(
    'INSERT INTO refresh_tokens (token_hash, user_id, expires_at) VALUES (?,?,?)',
  ).bind(await sha256Hex(refresh), user.id, Math.floor(Date.now() / 1000) + REFRESH_TTL).run()
  return { access_jwt: access, refresh_token: refresh, user }
}

app.post('/auth/refresh', async (c) => {
  const { refresh_token } = await c.req.json<{ refresh_token: string }>()
  if (!refresh_token) return c.json({ error: 'refresh_token required' }, 400)
  const hash = await sha256Hex(refresh_token)
  const row = await c.env.DB.prepare(
    'SELECT user_id, expires_at FROM refresh_tokens WHERE token_hash = ?',
  ).bind(hash).first<{ user_id: string; expires_at: number }>()
  if (!row || row.expires_at < Math.floor(Date.now() / 1000)) {
    return c.json({ error: 'Invalid refresh token' }, 401)
  }
  await c.env.DB.prepare('DELETE FROM refresh_tokens WHERE token_hash = ?').bind(hash).run()
  const newRefresh = randomToken()
  await c.env.DB.prepare(
    'INSERT INTO refresh_tokens (token_hash, user_id, expires_at) VALUES (?,?,?)',
  ).bind(await sha256Hex(newRefresh), row.user_id, Math.floor(Date.now() / 1000) + REFRESH_TTL).run()
  const access = await signJwt({ sub: row.user_id }, c.env.JWT_SECRET, ACCESS_TTL)
  return c.json({ access_jwt: access, refresh_token: newRefresh })
})

// ========================= PROFILE =========================

app.get('/profile', async (c) => {
  const user = await c.env.DB.prepare(
    'SELECT id, email, name, age, height_cm, weight_kg, sex, step_goal, created_at FROM users WHERE id = ?',
  ).bind(c.get('userId')).first()
  if (!user) return c.json({ error: 'Not found' }, 404)
  return c.json(user)
})

app.patch('/profile', async (c) => {
  const { name, age, height_cm, weight_kg, sex, step_goal } =
    await c.req.json<{ name?: string; age?: number; height_cm?: number; weight_kg?: number; sex?: string; step_goal?: number }>()
  const sexVal = sex === 'm' || sex === 'f' ? sex : null
  // step_goal: clamp to a sane range when provided; null leaves it unchanged.
  const goalVal = (typeof step_goal === 'number' && isFinite(step_goal))
    ? Math.max(1000, Math.min(50000, Math.round(step_goal))) : null
  await c.env.DB.prepare(
    'UPDATE users SET name=COALESCE(?,name), age=COALESCE(?,age), height_cm=COALESCE(?,height_cm), ' +
    'weight_kg=COALESCE(?,weight_kg), sex=COALESCE(?,sex), step_goal=COALESCE(?,step_goal) WHERE id = ?',
  ).bind(name ?? null, age ?? null, height_cm ?? null, weight_kg ?? null, sexVal, goalVal, c.get('userId')).run()
  const user = await c.env.DB.prepare(
    'SELECT id, email, name, age, height_cm, weight_kg, sex, step_goal, created_at FROM users WHERE id = ?',
  ).bind(c.get('userId')).first()
  return c.json(user)
})

// ========================= INGEST =========================
app.post('/ingest/batch', ingestBatch)
app.post('/ingest/events', ingestEvents)

// ========================= QUERY =========================
app.get('/today', getToday)
app.get('/sleep', getSleep)
app.get('/sleep/v2', getSleepV2)
app.get('/strain', getStrain)
app.get('/sessions', getSessions)
app.get('/trends', getTrends)
app.get('/chart', getChart)
app.get('/history', getHistory)
app.post('/journal', postJournal)
app.get('/journal', getJournal)
app.get('/journal/insights', getJournalInsights)
app.get('/day/strain', getDayStrain)
app.get('/day/sleep', getDaySleep)
app.get('/day/v2/sleep', getDaySleepV2)
app.get('/day/timeline', getDayTimeline)
app.get('/day/stress', getDayStress)
app.get('/day/heart', getDayHeart)
app.get('/day/lungs', getDayLungs)
app.get('/day/wear', getDayWear)
app.get('/trend/:metric', getTrend)
app.post('/workout/start', workoutStart)
app.post('/workout/end', workoutEnd)
app.get('/workouts', listWorkouts)
app.get('/workout/:id', getWorkout)
app.delete('/workout/:id', deleteWorkout)
app.get('/records', getRecords)
app.get('/notifications', getNotifications)
app.post('/notifications/read', markNotificationsRead)

// ========================= ADMIN =========================

// App config — OTA update pointer + home-screen alert banner (see appconfig.ts).
app.get('/admin/config', adminGetConfig)
app.post('/admin/config', adminSetConfig)

// Raw R2 objects auto-expire after this many days (bucket lifecycle rule
// `expire-raw-14d`). The R2-re-decode jobs (HRV/resp/IMU steps) can therefore
// only run over data this recent — older raw is gone, so we cap their `days`
// here. Keep this in sync with the R2 lifecycle rule.
const RAW_RETENTION_DAYS = 14

app.post('/admin/run-analytics', async (c) => {
  const body = await c.req.json<{ user_id?: string; days?: number; bio?: boolean }>().catch(() => ({} as any))
  const days = body.days ?? 3
  // Biometrics re-decodes from R2 → can only go back as far as raw is retained.
  const bioDays = Math.min(days, RAW_RETENTION_DAYS)
  if (body.user_id) {
    // Full re-derive sequence so HRV recovery feeds the coach:
    //   1. processUser  — minute metrics (strain/RHR/sleep/sessions...) + daily rows
    //      (reads the `minute` table, retained 90d — not R2 — so `days` is unbounded here)
    //   2. runBiometrics — HRV recovery/stress/illness from RR (R2; capped to retention)
    //   3. processUser  — coach picks up the recovery written in step 2
    const r1 = await processUser(c.env.DB, body.user_id, { historyDays: days })
    let bio: any = null
    if (body.bio !== false) {
      bio = await runBiometrics(c.env, body.user_id, bioDays)
      await processUser(c.env.DB, body.user_id, { historyDays: days })
    }
    return c.json({ ok: true, ...r1, bio, bio_days: bioDays, bio_capped: bioDays < days })
  }
  const res = await runAnalytics(c.env.DB, { historyDays: days })
  return c.json({ ok: true, ...res })
})

// Respiratory rate from PPG (R21 re-decoded from R2). Heavy (R2 reads) → admin /
// cron only. Stores resp_rate/resp_conf on daily; gated (conf ≥ 0.5) before surfaced.
// `days` is capped to the R2 retention window (older raw no longer exists).
app.post('/admin/run-resp', async (c) => {
  const body = await c.req.json<{ user_id: string; days?: number }>().catch(() => ({} as any))
  if (!body.user_id) return c.json({ error: 'user_id required' }, 400)
  const reqDays = body.days ?? 3
  const days = Math.min(reqDays, RAW_RETENTION_DAYS)
  const res = await runRespRate(c.env, body.user_id, days)
  return c.json({ ok: true, ...res, capped: days < reqDays, max_days: RAW_RETENTION_DAYS })
})

// HRV (RMSSD) + relative skin-temp / SpO₂ from the V24 RR/ADC bytes, re-decoded
// from R2. Heavy (R2 reads) → admin / cron only. Writes daily.hrv_rmssd etc.
// `days` is capped to the R2 retention window (older raw no longer exists).
app.post('/admin/run-biometrics', async (c) => {
  const body = await c.req.json<{ user_id: string; days?: number }>().catch(() => ({} as any))
  if (!body.user_id) return c.json({ error: 'user_id required' }, 400)
  const reqDays = body.days ?? 3
  const days = Math.min(reqDays, RAW_RETENTION_DAYS)
  const res = await runBiometrics(c.env, body.user_id, days)
  return c.json({ ok: true, ...res, capped: days < reqDays, max_days: RAW_RETENTION_DAYS })
})

// Steps from the wrist IMU (AN-2554 over raw R2). mode=incremental (default) counts
// only newly-settled minutes since the cursor; mode=full recomputes the last `days`
// (capped to R2 retention) and realigns the cursor. Heavy (R2) → admin / cron only.
app.post('/admin/run-steps', async (c) => {
  const body = await c.req.json<{ user_id: string; mode?: 'incremental' | 'full'; days?: number }>().catch(() => ({} as any))
  if (!body.user_id) return c.json({ error: 'user_id required' }, 400)
  if (body.mode === 'full') {
    const reqDays = body.days ?? 1
    const days = Math.min(reqDays, RAW_RETENTION_DAYS)
    const res = await runStepsImu(c.env, body.user_id, days)
    return c.json({ ok: true, mode: 'full', ...res, capped: days < reqDays })
  }
  const res = await runStepsIncremental(c.env, body.user_id)
  return c.json({ ok: true, mode: 'incremental', ...res })
})

// Phased to stay under the free-plan per-request subrequest cap. The shell
// orchestrates: init → minutes (chunked) → analytics. `now` is pinned across
// phases so day indices line up. Each phase is idempotent.
app.post('/admin/seed-demo', async (c) => {
  const body = await c.req.json<{
    email: string; days?: number; phase?: string;
    user_id?: string; day_from?: number; day_to?: number; now?: number;
  }>()
  if (!body.email && !body.user_id) return c.json({ error: 'email or user_id required' }, 400)
  const days = body.days ?? 90
  const now = body.now ?? Math.floor(Date.now() / 1000)
  const phase = body.phase ?? 'all'

  if (phase === 'init') {
    const r = await seedInit(c.env.DB, body.email, now)
    return c.json({ ok: true, phase, days, now, ...r })
  }
  if (phase === 'minutes') {
    if (!body.user_id) return c.json({ error: 'user_id required for minutes phase' }, 400)
    const r = await seedMinutes(c.env.DB, body.user_id, days,
      body.day_from ?? 0, body.day_to ?? days, now)
    return c.json({ ok: true, phase, ...r })
  }
  if (phase === 'analytics') {
    if (!body.user_id) return c.json({ error: 'user_id required for analytics phase' }, 400)
    const r = await seedAnalytics(c.env.DB, body.user_id, days, now)
    return c.json({ ok: true, phase, ...r })
  }
  // phase=all: only safe for short windows (few subrequests). For 90d use phases.
  const init = await seedInit(c.env.DB, body.email, now)
  const min = await seedMinutes(c.env.DB, init.user_id, days, 0, days, now)
  const an = await seedAnalytics(c.env.DB, init.user_id, days, now)
  return c.json({ ok: true, phase: 'all', user_id: init.user_id, minutes: min.minutes, ...an })
})

// Mint access+refresh for an email (admin-gated). For ops/testing without
// reading the OTP inbox. Same ADMIN_TOKEN gate as the other admin ops.
app.post('/admin/issue-token', async (c) => {
  const { email } = await c.req.json<{ email: string }>().catch(() => ({ email: '' as string }))
  if (!email) return c.json({ error: 'email required' }, 400)
  const e = email.toLowerCase().trim()
  const user = await c.env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(e).first<{ id: string }>()
  if (!user) return c.json({ error: 'No account for that email' }, 404)
  const access = await signJwt({ sub: user.id }, c.env.JWT_SECRET, ACCESS_TTL)
  const refresh = randomToken()
  await c.env.DB.prepare(
    'INSERT INTO refresh_tokens (token_hash, user_id, expires_at) VALUES (?,?,?)',
  ).bind(await sha256Hex(refresh), user.id, Math.floor(Date.now() / 1000) + REFRESH_TTL).run()
  return c.json({ access_jwt: access, refresh_token: refresh, user_id: user.id })
})

// Enqueue an analytics job for a user (ops + verification). The consumer runs it
// in its own bounded invocation. 404 if Queues isn't bound.
app.post('/admin/enqueue', async (c) => {
  const body = await c.req.json<{ user_id: string; job?: AnalyticsJob; day?: string }>().catch(() => ({} as any))
  if (!body.user_id) return c.json({ error: 'user_id required' }, 400)
  if (!c.env.ANALYTICS_Q) return c.json({ error: 'queue not bound' }, 404)
  const msg = { user_id: body.user_id, job: body.job ?? 'sweep', ...(body.day ? { day: body.day } : {}) }
  await c.env.ANALYTICS_Q.send(msg)
  return c.json({ ok: true, enqueued: msg })
})

app.post('/admin/wipe-raw', async (c) => {
  let deleted = 0
  let cursor: string | undefined
  do {
    const listing = await c.env.RAW_BUCKET.list({ cursor, limit: 1000 })
    const keys = listing.objects.map((o) => o.key)
    if (keys.length > 0) {
      await c.env.RAW_BUCKET.delete(keys)
      deleted += keys.length
    }
    cursor = listing.truncated ? listing.cursor : undefined
  } while (cursor)
  return c.json({ deleted })
})

// Prune minute rows older than 90 days (raw stays in R2).
app.post('/admin/prune', async (c) => {
  const cutoff = Math.floor(Date.now() / 1000) - MINUTE_RETENTION_DAYS * DAY
  const res = await c.env.DB.prepare('DELETE FROM minute WHERE ts_min < ?').bind(cutoff).run()
  const ev = await c.env.DB.prepare('DELETE FROM events WHERE ts < ?').bind(cutoff).run()
  return c.json({ ok: true, deleted: res.meta?.changes ?? 0, events_deleted: ev.meta?.changes ?? 0, cutoff })
})

// Send messages in sendBatch chunks of 100 (the Queues per-call max), so the cron
// itself never blows its own subrequest budget no matter how many users.
async function sendChunked(q: Queue<AnalyticsMessage>, msgs: { body: AnalyticsMessage }[]): Promise<void> {
  for (let i = 0; i < msgs.length; i += 100) {
    await q.sendBatch(msgs.slice(i, i + 100))
  }
}

// One (user, job) message per user (day-less — for 'sweep').
async function enqueueJobs(q: Queue<AnalyticsMessage>, userIds: string[], job: AnalyticsJob): Promise<void> {
  await sendChunked(q, userIds.map((user_id) => ({ body: { user_id, job } })))
}

// One (user, job, day) message per user × day — heavy R2 jobs fanned out so each
// consumer invocation processes exactly ONE bounded day (works for any user, no
// matter how much they wore the band).
async function enqueueJobDays(q: Queue<AnalyticsMessage>, userIds: string[], job: AnalyticsJob, dates: string[]): Promise<void> {
  const msgs: { body: AnalyticsMessage }[] = []
  for (const user_id of userIds) for (const day of dates) msgs.push({ body: { user_id, job, day } })
  await sendChunked(q, msgs)
}

// The last `n` UTC dates (today first) as YYYY-MM-DD.
function lastNDates(n: number): string[] {
  const now = Math.floor(Date.now() / 1000)
  return Array.from({ length: n }, (_, d) => new Date((Math.floor((now - d * DAY) / DAY) * DAY) * 1000).toISOString().slice(0, 10))
}

export default {
  fetch: app.fetch,

  // Queue consumer — one bounded (user, job) unit per invocation.
  async queue(batch: MessageBatch<AnalyticsMessage>, env: Bindings): Promise<void> {
    await handleQueueBatch(batch, env)
  },

  // Crons: "*/30 * * * *" sweep (enqueue dirty users); "30 3 * * *" nightly full
  // re-derive + R2 jobs + prune. With Queues bound, the cron only ENQUEUES; the
  // consumer does the bounded per-user work. Inline fallback if the binding is absent.
  async scheduled(event: ScheduledEvent, env: Bindings, ctx: ExecutionContext): Promise<void> {
    if (event.cron === '30 3 * * *') {
      ctx.waitUntil((async () => {
        const since2 = new Date(Date.now() - 2 * DAY * 1000).toISOString().slice(0, 10)
        const since3 = new Date(Date.now() - 3 * DAY * 1000).toISOString().slice(0, 10)
        if (env.ANALYTICS_Q) {
          // Fan out per (user, job): each consumer invocation = one bounded unit.
          const dailyU = (await env.DB.prepare('SELECT DISTINCT user_id FROM daily WHERE date >= ?')
            .bind(since3).all<{ user_id: string }>()).results ?? []
          const ids = dailyU.map((u) => u.user_id)
          // 'sweep' = one per user (D1 + incremental steps, light). 'steps_full' =
          // late-frame true-up, per (user, day).
          await enqueueJobs(env.ANALYTICS_Q, ids, 'sweep')
          await enqueueJobDays(env.ANALYTICS_Q, ids, 'steps_full', lastNDates(2))
          // Biometrics + resp are the expensive R2 re-decodes. Enqueue ONLY the
          // (user, day) nights still MISSING the metric in the last 2 days — this
          // skips every already-scored night (no redundant decode vs the
          // event-driven fire) AND retries nights the event fire left null
          // (self-healing). The single biggest R2 cost saver.
          const bioNeed = (await env.DB.prepare(
            'SELECT user_id, date FROM daily WHERE date >= ? AND hrv_rmssd IS NULL',
          ).bind(since2).all<{ user_id: string; date: string }>()).results ?? []
          await sendChunked(env.ANALYTICS_Q,
            bioNeed.map((r) => ({ body: { user_id: r.user_id, job: 'biometrics', day: r.date } as AnalyticsMessage })))
          const respNeed = (await env.DB.prepare(
            'SELECT d.user_id, d.date FROM daily d WHERE d.date >= ? AND d.resp_rate IS NULL ' +
            'AND EXISTS (SELECT 1 FROM sleep s WHERE s.user_id = d.user_id AND s.date = d.date AND s.onset_ts IS NOT NULL)',
          ).bind(since2).all<{ user_id: string; date: string }>()).results ?? []
          await sendChunked(env.ANALYTICS_Q,
            respNeed.map((r) => ({ body: { user_id: r.user_id, job: 'resp', day: r.date } as AnalyticsMessage })))
        } else {
          // Fallback (no queue): inline. Only viable at small scale.
          await runAnalytics(env.DB, { historyDays: 2 })
          try {
            const u = (await env.DB.prepare('SELECT DISTINCT user_id FROM sleep WHERE date >= ? AND onset_ts IS NOT NULL').bind(since2).all<{ user_id: string }>()).results ?? []
            for (const x of u) await runRespRate(env, x.user_id, 2)
          } catch (e) { console.error('resp cron failed', e) }
          try {
            const u = (await env.DB.prepare('SELECT DISTINCT user_id FROM daily WHERE date >= ?').bind(since3).all<{ user_id: string }>()).results ?? []
            for (const x of u) await runBiometrics(env, x.user_id, 3)
          } catch (e) { console.error('biometrics cron failed', e) }
          try {
            const u = (await env.DB.prepare('SELECT DISTINCT user_id FROM daily WHERE date >= ?').bind(since2).all<{ user_id: string }>()).results ?? []
            for (const x of u) await runStepsImu(env, x.user_id, 2)
          } catch (e) { console.error('steps cron failed', e) }
        }
        // Prune is light D1 — always inline. Minute + events share the same
        // drill-down window (/day/timeline), so they age out together.
        const cutoff = Math.floor(Date.now() / 1000) - MINUTE_RETENTION_DAYS * DAY
        await env.DB.prepare('DELETE FROM minute WHERE ts_min < ?').bind(cutoff).run()
        await env.DB.prepare('DELETE FROM events WHERE ts < ?').bind(cutoff).run()
      })())
    } else {
      ctx.waitUntil((async () => {
        await autoCloseStaleWorkouts(env.DB)
        if (env.ANALYTICS_Q) {
          // Enqueue a 'sweep' (processUser + incremental steps) per dirty user.
          const dirty = (await env.DB.prepare('SELECT user_id FROM analytics_cursor WHERE dirty = 1')
            .all<{ user_id: string }>()).results ?? []
          const dids = dirty.map((u) => u.user_id)
          await enqueueJobs(env.ANALYTICS_Q, dids, 'sweep')
          // Optimistically clear dirty for exactly the users we enqueued, so a
          // persistently-failing sweep isn't re-enqueued every 30 min (a user
          // re-dirtied after this SELECT keeps dirty=1 and is caught next tick).
          for (let i = 0; i < dids.length; i += 100) {
            const chunk = dids.slice(i, i + 100)
            await env.DB.prepare(
              `UPDATE analytics_cursor SET dirty = 0 WHERE user_id IN (${chunk.map(() => '?').join(',')})`,
            ).bind(...chunk).run()
          }
        } else {
          // Fallback (no queue): inline.
          await runAnalytics(env.DB, { historyDays: 3 })
          try {
            const since = Math.floor(Date.now() / 1000) - 2 * 3600
            const u = (await env.DB.prepare('SELECT DISTINCT user_id FROM minute WHERE ts_min >= ?').bind(since).all<{ user_id: string }>()).results ?? []
            for (const x of u) await runStepsIncremental(env, x.user_id)
          } catch (e) { console.error('steps incremental cron failed', e) }
        }
      })())
    }
  },
}
