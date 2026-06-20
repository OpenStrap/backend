import { Hono } from 'hono'
import {
  signJwt, verifyJwt, sha256Hex, generateOtp, randomToken, uuid, sendOtpEmail,
} from './auth'
import { runAnalytics, processUser } from './analytics'
import { ingestBatch, ingestEvents } from './ingest'
import { handleQueueBatch, type AnalyticsMessage, type AnalyticsJob } from './queue'
import { runWakeLadder, retryStaleCloses } from './wake_cron'
import { sealOldDays, pruneMinuteDays } from './minute_store'
import { getToday, getSleep, getSleepV2, getStrain, getSessions, getTrends, getChart } from './query'
import { getHistory } from './history'
import { postJournal, getJournal, getJournalInsights } from './journal'
import { postCycleLog, deleteCycleLog, getCycle } from './cycle'
import { postSpotCheck } from './spotcheck'
import { getDayStrain, getDaySleep, getDaySleepV2, getDayTimeline, getDayStress, getDayHeart, getDayLungs, getDayWear } from './daydetail'
import { getTrend } from './trend'
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
app.use('/cycle', requireJwt)
app.use('/cycle/*', requireJwt)
app.use('/spotcheck', requireJwt)
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
    'SELECT id, email, name, age, height_cm, weight_kg, sex, step_goal, track_cycle, created_at FROM users WHERE id = ?',
  ).bind(c.get('userId')).first()
  if (!user) return c.json({ error: 'Not found' }, 404)
  return c.json(user)
})

app.patch('/profile', async (c) => {
  const { name, age, height_cm, weight_kg, sex, step_goal, track_cycle } =
    await c.req.json<{ name?: string; age?: number; height_cm?: number; weight_kg?: number; sex?: string; step_goal?: number; track_cycle?: boolean | number }>()
  const sexVal = sex === 'm' || sex === 'f' ? sex : null
  // step_goal: clamp to a sane range when provided; null leaves it unchanged.
  const goalVal = (typeof step_goal === 'number' && isFinite(step_goal))
    ? Math.max(1000, Math.min(50000, Math.round(step_goal))) : null
  // track_cycle: explicit opt-in toggle (null leaves it unchanged).
  const trackVal = track_cycle === undefined ? null : (track_cycle ? 1 : 0)
  await c.env.DB.prepare(
    'UPDATE users SET name=COALESCE(?,name), age=COALESCE(?,age), height_cm=COALESCE(?,height_cm), ' +
    'weight_kg=COALESCE(?,weight_kg), sex=COALESCE(?,sex), step_goal=COALESCE(?,step_goal), track_cycle=COALESCE(?,track_cycle) WHERE id = ?',
  ).bind(name ?? null, age ?? null, height_cm ?? null, weight_kg ?? null, sexVal, goalVal, trackVal, c.get('userId')).run()
  const user = await c.env.DB.prepare(
    'SELECT id, email, name, age, height_cm, weight_kg, sex, step_goal, track_cycle, created_at FROM users WHERE id = ?',
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
app.post('/cycle/log', postCycleLog)
app.delete('/cycle/log', deleteCycleLog)
app.get('/cycle', getCycle)
app.post('/spotcheck', postSpotCheck)
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

// (Steps are AN-2554-only now: counted at ingest into minute.steps, summed into
// daily.steps by processUser, served live on-read. No R2 step job / admin endpoint.)

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
  const body = await c.req.json<{ user_id: string; job?: AnalyticsJob; day?: string; onset_ts?: number; wake_ts?: number }>().catch(() => ({} as any))
  if (!body.user_id) return c.json({ error: 'user_id required' }, 400)
  if (!c.env.ANALYTICS_Q) return c.json({ error: 'queue not bound' }, 404)
  const msg = {
    user_id: body.user_id, job: body.job ?? 'sweep',
    ...(body.day ? { day: body.day } : {}),
    // Forward the sleep window so a close_day job can be fired for verification
    // (the wake_cron supplies these in production).
    ...(body.onset_ts ? { onset_ts: body.onset_ts } : {}),
    ...(body.wake_ts ? { wake_ts: body.wake_ts } : {}),
  }
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
  const now = Math.floor(Date.now() / 1000)
  await pruneMinuteDays(c.env, now, MINUTE_RETENTION_DAYS) // day-packed minute_day rows
  const ev = await c.env.DB.prepare('DELETE FROM events WHERE ts < ?').bind(now - MINUTE_RETENTION_DAYS * DAY).run()
  return c.json({ ok: true, events_deleted: ev.meta?.changes ?? 0 })
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

  // [feat/wake-trigger] The frequent cron does ONE thing: the isUserAwake ladder
  // (wake_cron.runWakeLadder) — detect each user's real wake and fire ONE close_day
  // per physiological day. Cheap enough for */10 (awake-and-closed users = a cursor
  // read; asleep = a 30-min peek; the heavy ensemble runs once, at the wake). All
  // derivation lives in the close_day QUEUE job, never here. A SEPARATE nightly tick
  // does maintenance only: prune aged minute/events + a retry-net for missed closes.
  async scheduled(event: ScheduledEvent, env: Bindings, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil((async () => {
      // EVERY tick — wake detection ONLY (the cron's sole job). Auto-workout detection
      // and forgotten-live-workout close moved ON-READ (workouts.ensureTodayWorkouts);
      // the nightly tick keeps autoCloseStaleWorkouts as a safety net for users who
      // never open the app.
      try { await runWakeLadder(env) } catch (e) { console.error('wake ladder failed', e) }
      // Nightly maintenance ONLY (separate from detection): seal + retention + retry-net.
      if (event.cron === '30 3 * * *') {
        try { await autoCloseStaleWorkouts(env.DB) } catch (e) { console.error('autoclose failed', e) }
        // Seal days older than the hot window to gzipped R2 objects and drop them from
        // D1 (D1-hot / R2-sealed tiering — cuts D1 storage + per-row prune-deletes).
        try { await sealOldDays(env) } catch (e) { console.error('seal failed', e) }
        // Backstop: clear any minute_day/events still unsealed past retention (e.g. no bucket).
        const nowS = Math.floor(Date.now() / 1000)
        await pruneMinuteDays(env, nowS, MINUTE_RETENTION_DAYS)
        await env.DB.prepare('DELETE FROM events WHERE ts < ?').bind(nowS - MINUTE_RETENTION_DAYS * DAY).run()
        try { await retryStaleCloses(env) } catch (e) { console.error('retry-net failed', e) }
      }
    })())
  },
}
