// openstrap-backend — SLIM companion worker (local-first era).
//
// The app does all decode + analytics on-device. This worker only:
//   • OTA + announcements      GET /app/status, GET/POST /admin/config
//   • Legal/Terms              GET /legal/terms (drives the onboarding consent gate)
//   • Auth (for import only)    POST /auth/{request-otp,verify-otp,refresh}
//   • Import reads              GET /profile, /strain, /sleep, /sessions  (JWT)
//   • Consent ledger            POST /consent
//   • Telemetry (opt-in)        POST /telemetry            (device-id keyed, no JWT)
//   • Health-data (opt-in)      POST /health/upload        (device-id keyed, no JWT)
//   • Withdrawal                DELETE /data               (right to be forgotten)
//
// Everything else (live ingest, derivation, cron, queues, minute storage) was
// removed — see git history + migrate_to_slim.sql.

import { Hono } from 'hono'
import {
  signJwt, verifyJwt, sha256Hex, generateOtp, randomToken, sendOtpEmail,
} from './auth'
import { getAppStatus, getTerms, adminGetConfig, adminSetConfig } from './appconfig'
import { getProfile, getStrain, getSleep, getSessions } from './reads'
import { postTelemetry, postConsent } from './telemetry'
import { postHealthUpload } from './health'

type Bindings = {
  DB: D1Database
  HEALTH_BUCKET: R2Bucket
  RATE_LIMITER?: { limit(opts: { key: string }): Promise<{ success: boolean }> }
  JWT_SECRET: string
  ADMIN_TOKEN?: string
  BREVO_API_KEY?: string
  RESEND_API_KEY?: string
  EMAIL_FROM?: string
  EMAIL_FROM_NAME?: string
  RESEND_FROM?: string
  // Self-host escape hatch: a fixed verification code. When set, OTPs are NOT
  // emailed (the operator already knows it). NEVER set in a hosted deployment.
  // The code is still never returned in the API response.
  STATIC_OTP?: string
}

type Vars = { userId: string }

const ACCESS_TTL = 24 * 60 * 60          // 24h
const REFRESH_TTL = 30 * 24 * 60 * 60    // 30d
const OTP_TTL = 10 * 60                   // 10m
const OTP_MAX_ATTEMPTS = 5

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

// Import reads are the only JWT-gated user endpoints.
app.use('/profile', requireJwt)
app.use('/strain', requireJwt)
app.use('/sleep', requireJwt)
app.use('/sessions', requireJwt)
app.use('/admin/*', requireAdmin)

app.get('/', (c) => c.json({ ok: true, service: 'openstrap-backend', ts: Math.floor(Date.now() / 1000) }))

// Public app status (OTA pointer + admin alert banner + Terms pointer). No JWT.
app.get('/app/status', getAppStatus)
app.get('/legal/terms', getTerms)

// ========================= AUTH (import sign-in only) =========================

app.post('/auth/request-otp', async (c) => {
  const { email } = await c.req.json<{ email: string }>()
  if (!email) return c.json({ error: 'Email required' }, 400)
  const e = email.toLowerCase().trim()
  const user = await c.env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(e).first()
  if (!user) return c.json({ error: 'No account for that email' }, 404)
  return issueOtp(c, e)
})

async function issueOtp(c: any, email: string) {
  // Self-host: a fixed, operator-known code. No email, code never leaves the server.
  const staticOtp: string | undefined = c.env.STATIC_OTP
  const code = staticOtp || generateOtp()
  const codeHash = await sha256Hex(code)
  const expires = Math.floor(Date.now() / 1000) + OTP_TTL
  await c.env.DB.prepare(
    'INSERT INTO otps (email, code_hash, expires_at, attempts) VALUES (?,?,?,0) ' +
    'ON CONFLICT(email) DO UPDATE SET code_hash=excluded.code_hash, expires_at=excluded.expires_at, attempts=0',
  ).bind(email, codeHash, expires).run()

  if (staticOtp) return c.json({ ok: true }) // self-host: operator already knows the code

  const sent = await sendOtpEmail(c.env, email, code)
  if (!sent.delivered) {
    // FAIL CLOSED. The OTP is NEVER returned to the client — a missing/broken email
    // provider must not become an account-takeover.
    console.error(`[otp] delivery failed for ${email} — not exposing code`)
    return c.json(
      { error: 'Could not send your verification code. Please try again shortly.' },
      502,
    )
  }
  return c.json({ ok: true })
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

// ========================= IMPORT READS (JWT) =========================
app.get('/profile', getProfile)
app.get('/strain', getStrain)     // daily derived rows
app.get('/sleep', getSleep)
app.get('/sessions', getSessions)

// ========================= CONSENT + INGEST (opt-in, device-id keyed) =========
app.post('/consent', postConsent)
app.post('/telemetry', postTelemetry)
app.post('/health/upload', postHealthUpload)
// NOTE: no public data-deletion route by design. With no verifiable identity
// (these are anonymous device ids, no email), a public DELETE keyed on a guessable
// device_id would let anyone wipe another install's data — or spam-wipe everything.
// Any erasure must be admin-gated (ADMIN_TOKEN) or proven-identity, not open.

// ========================= ADMIN (OTA + announcements + Terms) ================
app.get('/admin/config', adminGetConfig)
app.post('/admin/config', adminSetConfig)

export default {
  fetch: app.fetch,
  // No queue() consumer and no scheduled() cron: the heavy pipeline they drove is gone.
}
