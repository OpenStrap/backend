// reads.ts — the IMPORT surface. The local-first app's BackendClient pulls a
// returning v2 user's already-derived history exactly once, at onboarding, via
// four authed reads: GET /profile, /strain (daily rows), /sleep, /sessions.
//
// These are plain SELECTs over the legacy derived tables (daily/sleep/sessions/
// users/baselines). No minute store, no on-read derivation, no analytics — the
// heavy pipeline that once populated these tables is gone; we only read what's
// already there. Shapes match what cloud_import.dart expects (see that file).

import type { Context } from 'hono'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

const nowSec = () => Math.floor(Date.now() / 1000)
const parseFlags = (s: string | null): any => {
  if (!s) return {}
  try { return JSON.parse(s) } catch { return {} }
}

// GET /profile — the user's row (name/age/height/weight/sex/step_goal/track_cycle).
export async function getProfile(c: Ctx) {
  const user = await c.env.DB.prepare(
    'SELECT id, email, name, age, height_cm, weight_kg, sex, step_goal, track_cycle, created_at FROM users WHERE id = ?',
  ).bind(c.get('userId')).first()
  if (!user) return c.json({ error: 'Not found' }, 404)
  return c.json(user)
}

// GET /strain?from&to — daily derived rows (newest first), flags parsed. The
// importer reads strain/resting_hr/hrv_*/recovery/resp_rate/calories/wear_min/
// stress/nocturnal/skin_temp_idx/spo2_idx off each row, so we return SELECT *.
export async function getStrain(c: Ctx) {
  const from = c.req.query('from'), to = c.req.query('to')
  let sql = 'SELECT * FROM daily WHERE user_id = ?'
  const binds: any[] = [c.get('userId')]
  if (from) { sql += ' AND date >= ?'; binds.push(from) }
  if (to) { sql += ' AND date <= ?'; binds.push(to) }
  sql += ' ORDER BY date DESC LIMIT 400'
  const { results } = await c.env.DB.prepare(sql).bind(...binds).all<any>()
  const rows = (results ?? []).map((r: any) => ({ ...r, flags: parseFlags(r.flags) }))
  return c.json(rows)
}

// GET /sleep?from&to — nightly rows (newest first). Carries the user's baseline
// need_min so the importer/app can seed the sleep-need ring without /trends.
export async function getSleep(c: Ctx) {
  const userId = c.get('userId')
  const from = c.req.query('from'), to = c.req.query('to')
  let sql = 'SELECT * FROM sleep WHERE user_id = ?'
  const binds: any[] = [userId]
  if (from) { sql += ' AND date >= ?'; binds.push(from) }
  if (to) { sql += ' AND date <= ?'; binds.push(to) }
  sql += ' ORDER BY date DESC LIMIT 400'
  const { results } = await c.env.DB.prepare(sql).bind(...binds).all<any>()
  const baseline = await c.env.DB.prepare('SELECT sleep_need_min FROM baselines WHERE user_id = ?')
    .bind(userId).first<any>()
  // Plausibility floor: a sleep need < 3h is never real (sparse-data garbage) → 8h.
  const needMin = (baseline?.sleep_need_min && baseline.sleep_need_min >= 180)
    ? baseline.sleep_need_min : 480
  const rows = (results ?? []).map((r: any) => ({
    ...r, flags: parseFlags(r.flags), need_min: needMin,
  }))
  return c.json(rows)
}

// GET /sessions?from&to — workouts by start_ts (unix seconds), excluding deleted.
// (No on-read auto-detection anymore — that lived in the now-removed pipeline.)
export async function getSessions(c: Ctx) {
  const from = parseInt(c.req.query('from') || '0')
  const to = parseInt(c.req.query('to') || String(nowSec()))
  const { results } = await c.env.DB.prepare(
    "SELECT * FROM sessions WHERE user_id = ? AND start_ts >= ? AND start_ts <= ? AND status != 'deleted' ORDER BY start_ts DESC LIMIT 400",
  ).bind(c.get('userId'), from, to).all<any>()
  const rows = (results ?? []).map((r: any) => ({
    ...r,
    zones: r.zones ? safeParse(r.zones) : null,
    duration_min: (r.end_ts != null && r.start_ts != null)
      ? Math.round((r.end_ts - r.start_ts) / 60) : null,
  }))
  return c.json(rows)
}

const safeParse = (s: any): any => {
  if (s == null) return null
  if (typeof s === 'object') return s
  try { return JSON.parse(s) } catch { return null }
}
