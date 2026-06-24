// strava.ts — Strava OAuth + token management (foundation for bidirectional sync).
//
// Connect flow (no client secret ever touches the app):
//   app → GET /strava/connect (JWT)  → { url } authorize link with a short-lived
//         signed `state` carrying the user id
//   user authorizes in a browser → Strava redirects to
//   GET /strava/callback?code&state → we exchange the code for tokens, store them
//   per user, and show a "return to OpenStrap" page.
//
// Access tokens are short-lived (~6h); getStravaAccessToken() transparently
// refreshes via the stored refresh_token and persists the rotation.

import { signJwt, verifyJwt } from './auth'

const STRAVA_AUTH = 'https://www.strava.com/oauth/authorize'
const STRAVA_TOKEN = 'https://www.strava.com/oauth/token'
const STRAVA_API = 'https://www.strava.com/api/v3'
// Read all activities + write new ones (both directions).
const SCOPE = 'activity:read_all,activity:write'
const STATE_TTL = 600 // 10 min to complete the browser hop

export interface StravaEnv {
  DB: D1Database
  JWT_SECRET: string
  STRAVA_CLIENT_ID?: string
  STRAVA_CLIENT_SECRET?: string
}

function configured(env: StravaEnv): boolean {
  return !!(env.STRAVA_CLIENT_ID && env.STRAVA_CLIENT_SECRET)
}

function originOf(c: any): string {
  const url = new URL(c.req.url)
  return `${url.protocol}//${url.host}`
}

function htmlPage(message: string, ok: boolean): string {
  const accent = ok ? '#34d399' : '#f87171'
  return `<!doctype html><html><head><meta charset="utf-8">` +
    `<meta name="viewport" content="width=device-width,initial-scale=1">` +
    `<title>OpenStrap × Strava</title></head>` +
    `<body style="font-family:-apple-system,system-ui,sans-serif;background:#0b0b0c;color:#fff;` +
    `display:grid;place-items:center;min-height:100vh;margin:0;text-align:center;padding:24px">` +
    `<div><div style="font-size:40px;margin-bottom:12px">${ok ? '✅' : '⚠️'}</div>` +
    `<h2 style="margin:0 0 8px">${message}</h2>` +
    `<p style="opacity:.6;margin:0">You can close this tab and return to OpenStrap.</p>` +
    `<p style="color:${accent};opacity:.8;margin-top:24px;font-size:13px">OpenStrap × Strava</p></div></body></html>`
}

// GET /strava/connect  (requireJwt) → { url } for the app to open in a browser.
export async function stravaConnect(c: any) {
  if (!configured(c.env)) return c.json({ error: 'Strava integration not configured on this backend.' }, 503)
  const userId: string = c.get('userId')
  const state = await signJwt({ sub: userId, typ: 'strava_state' }, c.env.JWT_SECRET, STATE_TTL)
  const redirectUri = `${originOf(c)}/strava/callback`
  const url = `${STRAVA_AUTH}?client_id=${encodeURIComponent(c.env.STRAVA_CLIENT_ID)}` +
    `&response_type=code&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&approval_prompt=auto&scope=${encodeURIComponent(SCOPE)}&state=${encodeURIComponent(state)}`
  return c.json({ url })
}

// GET /strava/callback?code&state  (public; Strava redirect target) → HTML page.
export async function stravaCallback(c: any) {
  if (!configured(c.env)) return c.html(htmlPage('Strava is not configured.', false), 503)
  const error = c.req.query('error')
  if (error) return c.html(htmlPage('Strava authorization was declined.', false), 400)
  const code = c.req.query('code')
  const state = c.req.query('state')
  if (!code || !state) return c.html(htmlPage('Missing authorization code.', false), 400)

  const payload = await verifyJwt(state, c.env.JWT_SECRET)
  if (!payload || (payload as any).typ !== 'strava_state') {
    return c.html(htmlPage('This link expired. Start the connection again from the app.', false), 400)
  }
  const userId = payload.sub

  const form = new URLSearchParams({
    client_id: String(c.env.STRAVA_CLIENT_ID),
    client_secret: String(c.env.STRAVA_CLIENT_SECRET),
    code,
    grant_type: 'authorization_code',
  })
  const resp = await fetch(STRAVA_TOKEN, { method: 'POST', body: form })
  if (!resp.ok) return c.html(htmlPage('Strava token exchange failed.', false), 502)
  const tok = await resp.json() as {
    access_token: string; refresh_token: string; expires_at: number
    athlete?: { id?: number }
  }

  await c.env.DB.prepare(
    'INSERT INTO strava_tokens (user_id, athlete_id, access_token, refresh_token, expires_at, scope, connected_at) ' +
    'VALUES (?,?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET ' +
    'athlete_id=excluded.athlete_id, access_token=excluded.access_token, refresh_token=excluded.refresh_token, ' +
    'expires_at=excluded.expires_at, scope=excluded.scope',
  ).bind(
    userId, tok.athlete?.id ?? null, tok.access_token, tok.refresh_token, tok.expires_at, SCOPE,
    Math.floor(Date.now() / 1000),
  ).run()

  return c.html(htmlPage('Strava connected!', true))
}

// GET /strava/status  (requireJwt) → { connected, athlete_id, scope }
export async function stravaStatus(c: any) {
  const userId: string = c.get('userId')
  const row = (await c.env.DB.prepare(
    'SELECT athlete_id, scope, connected_at FROM strava_tokens WHERE user_id = ?',
  ).bind(userId).first()) as { athlete_id: number | null; scope: string | null; connected_at: number | null } | null
  return c.json({
    connected: !!row,
    athlete_id: row?.athlete_id ?? null,
    scope: row?.scope ?? null,
    connected_at: row?.connected_at ?? null,
  })
}

// POST /strava/disconnect  (requireJwt) → { ok }
export async function stravaDisconnect(c: any) {
  const userId: string = c.get('userId')
  await c.env.DB.prepare('DELETE FROM strava_tokens WHERE user_id = ?').bind(userId).run()
  return c.json({ ok: true })
}

// ── Strava → Whoop : pull recent activities into strava_activities ────────────
export async function stravaPull(
  env: StravaEnv, userId: string, perPage = 30,
): Promise<{ pulled: number }> {
  const token = await getStravaAccessToken(env, userId)
  if (!token) return { pulled: 0 }
  const r = await fetch(`${STRAVA_API}/athlete/activities?per_page=${perPage}`, {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!r.ok) return { pulled: 0 }
  const acts = (await r.json()) as Array<Record<string, unknown>>
  const now = Math.floor(Date.now() / 1000)
  let pulled = 0
  for (const a of acts) {
    const startTs = typeof a.start_date === 'string'
      ? Math.floor(new Date(a.start_date).getTime() / 1000) : null
    await env.DB.prepare(
      'INSERT INTO strava_activities (user_id, activity_id, start_ts, elapsed_sec, type, name, distance_m, avg_hr, max_hr, raw, pulled_at) ' +
      'VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(user_id, activity_id) DO UPDATE SET ' +
      'start_ts=excluded.start_ts, elapsed_sec=excluded.elapsed_sec, type=excluded.type, name=excluded.name, ' +
      'distance_m=excluded.distance_m, avg_hr=excluded.avg_hr, max_hr=excluded.max_hr, raw=excluded.raw, pulled_at=excluded.pulled_at',
    ).bind(
      userId, a.id, startTs, a.elapsed_time ?? null, (a.sport_type ?? a.type) ?? null, a.name ?? null,
      a.distance ?? null, a.average_heartrate ?? null, a.max_heartrate ?? null,
      JSON.stringify({
        id: a.id, name: a.name, type: a.sport_type ?? a.type, start_date: a.start_date,
        moving_time: a.moving_time, elapsed_time: a.elapsed_time, distance: a.distance,
        total_elevation_gain: a.total_elevation_gain, average_speed: a.average_speed,
        average_heartrate: a.average_heartrate, max_heartrate: a.max_heartrate,
        average_watts: a.average_watts, kilojoules: a.kilojoules, calories: a.calories,
      }), now,
    ).run()
    pulled++
  }
  return { pulled }
}

// ── Whoop → Strava : push completed OpenStrap workouts as manual activities ────
// Dedupe against pulled activities (a ride already on Strava via the bike computer
// must NOT be duplicated) and against strava_pushed (idempotent). HR/strain land in
// the description (manual activities don't accept HR streams without a file upload).
const STRAVA_SPORT: Record<string, string> = {
  run: 'Run', running: 'Run', walk: 'Walk', walking: 'Walk', hike: 'Hike',
  ride: 'Ride', cycling: 'Ride', bike: 'Ride',
  strength: 'WeightTraining', weights: 'WeightTraining', gym: 'Workout',
  swim: 'Swim', yoga: 'Yoga', rowing: 'Rowing', elliptical: 'Elliptical',
}

export async function stravaPush(
  env: StravaEnv, userId: string,
): Promise<{ pushed: number; skipped: number }> {
  const token = await getStravaAccessToken(env, userId)
  if (!token) return { pushed: 0, skipped: 0 }

  const { results: sessions } = await env.DB.prepare(
    "SELECT id, start_ts, end_ts, type, avg_hr, max_hr, strain, calories FROM sessions " +
    "WHERE user_id = ? AND status = 'done' AND start_ts IS NOT NULL AND end_ts > start_ts " +
    "ORDER BY start_ts DESC LIMIT 20",
  ).bind(userId).all<Record<string, number | string | null>>()

  let pushed = 0, skipped = 0
  for (const s of sessions ?? []) {
    const sid = String(s.id)
    const startTs = Number(s.start_ts), endTs = Number(s.end_ts)

    const already = await env.DB.prepare(
      'SELECT 1 FROM strava_pushed WHERE user_id = ? AND session_key = ?',
    ).bind(userId, sid).first()
    if (already) { skipped++; continue }

    // Overlap with an existing Strava activity (e.g. a Wahoo-synced ride) → skip.
    const overlap = await env.DB.prepare(
      'SELECT 1 FROM strava_activities WHERE user_id = ? AND start_ts BETWEEN ? AND ? LIMIT 1',
    ).bind(userId, startTs - 1800, endTs + 1800).first()
    if (overlap) {
      await env.DB.prepare(
        'INSERT OR IGNORE INTO strava_pushed (user_id, session_key, activity_id, pushed_at) VALUES (?,?,?,?)',
      ).bind(userId, sid, null, Math.floor(Date.now() / 1000)).run()
      skipped++; continue
    }

    const typeKey = String(s.type ?? '').toLowerCase()
    const sportType = STRAVA_SPORT[typeKey] ?? 'Workout'
    const descParts = [
      s.strain != null ? `Strain ${Number(s.strain).toFixed(1)}` : null,
      s.avg_hr != null ? `Avg HR ${s.avg_hr}` : null,
      s.max_hr != null ? `Max HR ${s.max_hr}` : null,
      s.calories != null ? `${Math.round(Number(s.calories))} kcal` : null,
      'via OpenStrap',
    ].filter(Boolean)

    const form = new URLSearchParams({
      name: `OpenStrap ${sportType}`,
      sport_type: sportType,
      start_date_local: new Date(startTs * 1000).toISOString(),
      elapsed_time: String(Math.max(60, endTs - startTs)),
      description: descParts.join(' · '),
    })
    const r = await fetch(`${STRAVA_API}/activities`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}` },
      body: form,
    })
    if (!r.ok) { skipped++; continue }
    const created = (await r.json()) as { id?: number }
    await env.DB.prepare(
      'INSERT OR IGNORE INTO strava_pushed (user_id, session_key, activity_id, pushed_at) VALUES (?,?,?,?)',
    ).bind(userId, sid, created.id ?? null, Math.floor(Date.now() / 1000)).run()
    pushed++
  }
  return { pushed, skipped }
}

// Cron backstop: pull + push for every connected user (called from the nightly tick).
export async function syncAllStrava(env: StravaEnv): Promise<void> {
  const { results } = await env.DB.prepare(
    'SELECT user_id FROM strava_tokens LIMIT 1000',
  ).all<{ user_id: string }>()
  for (const u of results ?? []) {
    try {
      await stravaPull(env, u.user_id)
      await stravaPush(env, u.user_id)
    } catch (e) {
      console.error('strava sync failed', u.user_id, e)
    }
  }
}

// GET /strava/sync (requireJwt) → pull (Strava→Whoop) then push (Whoop→Strava).
export async function stravaSync(c: any) {
  const userId: string = c.get('userId')
  const pull = await stravaPull(c.env, userId)
  const push = await stravaPush(c.env, userId)
  return c.json({ ok: true, ...pull, ...push })
}

// GET /strava/activities (requireJwt) → the pulled activities (for the app overlay).
export async function stravaActivities(c: any) {
  const userId: string = c.get('userId')
  const { results } = await c.env.DB.prepare(
    'SELECT activity_id, start_ts, elapsed_sec, type, name, distance_m, avg_hr, max_hr ' +
    'FROM strava_activities WHERE user_id = ? ORDER BY start_ts DESC LIMIT 50',
  ).bind(userId).all()
  return c.json({ activities: results ?? [] })
}

// A valid access token for `userId`, refreshing + persisting if expired. Returns
// null if the user has not connected Strava or the refresh fails.
export async function getStravaAccessToken(env: StravaEnv, userId: string): Promise<string | null> {
  if (!configured(env)) return null
  const row = await env.DB.prepare(
    'SELECT access_token, refresh_token, expires_at FROM strava_tokens WHERE user_id = ?',
  ).bind(userId).first<{ access_token: string; refresh_token: string; expires_at: number }>()
  if (!row) return null

  const now = Math.floor(Date.now() / 1000)
  if (row.expires_at - 60 > now) return row.access_token // still valid (60s skew)

  const form = new URLSearchParams({
    client_id: String(env.STRAVA_CLIENT_ID),
    client_secret: String(env.STRAVA_CLIENT_SECRET),
    grant_type: 'refresh_token',
    refresh_token: row.refresh_token,
  })
  const resp = await fetch(STRAVA_TOKEN, { method: 'POST', body: form })
  if (!resp.ok) return null
  const tok = await resp.json() as { access_token: string; refresh_token: string; expires_at: number }
  await env.DB.prepare(
    'UPDATE strava_tokens SET access_token=?, refresh_token=?, expires_at=? WHERE user_id=?',
  ).bind(tok.access_token, tok.refresh_token, tok.expires_at, userId).run()
  return tok.access_token
}
