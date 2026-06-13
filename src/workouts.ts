// workouts.ts — manual + live + auto-detected workouts, all backed by the SAME
// raw-first stream we already capture. A workout is a time-tagged window over the
// `minute` rollups; the breakdown is computed server-side (reusing the analytics)
// so the live foreground app never has to hold anything — backgrounded, killed, or
// blipped, the record is reconstructed from uploaded data on /workout/end.
import type { Context } from 'hono'
import {
  calcStrain, calcHrZones, calcCalories, calcHrRecovery,
  type Minute, type Profile, type Baseline,
} from 'openstrap-analytics'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>
const nowSec = () => Math.floor(Date.now() / 1000)
const DAY = 86400

const VALID_TYPES = ['run', 'cycle', 'strength', 'walk', 'swim', 'cardio', 'yoga', 'other']

async function loadProfile(db: D1Database, userId: string): Promise<Profile> {
  const u = await db.prepare('SELECT age, weight_kg, height_cm, sex FROM users WHERE id = ?')
    .bind(userId).first<any>()
  return {
    age: u?.age ?? undefined, weight_kg: u?.weight_kg ?? undefined,
    height_cm: u?.height_cm ?? undefined,
    sex: (u?.sex === 'm' || u?.sex === 'f') ? u.sex : undefined,
  }
}
async function loadBaseline(db: D1Database, userId: string): Promise<Baseline> {
  const b = await db.prepare('SELECT resting_hr, max_hr, sleep_need_min, skin_temp, chronic_strain FROM baselines WHERE user_id = ?')
    .bind(userId).first<any>()
  return {
    resting_hr: b?.resting_hr ?? 60, max_hr: b?.max_hr ?? 0,
    sleep_need_min: b?.sleep_need_min ?? 480,
    skin_temp: b?.skin_temp ?? undefined, chronic_strain: b?.chronic_strain ?? undefined,
  }
}
async function loadMinutes(db: D1Database, userId: string, from: number, to: number): Promise<Minute[]> {
  const { results } = await db.prepare(
    'SELECT ts_min, hr_avg, hr_min, hr_max, hr_n, activity, steps, wrist_on FROM minute ' +
    'WHERE user_id = ? AND ts_min >= ? AND ts_min <= ? ORDER BY ts_min ASC',
  ).bind(userId, from, to).all<any>()
  return (results ?? []).map((r: any) => ({
    ts: r.ts_min, hr_avg: r.hr_avg ?? 0, hr_min: r.hr_min ?? 0, hr_max: r.hr_max ?? 0,
    hr_n: r.hr_n ?? 0, activity: r.activity ?? 0, steps: r.steps ?? 0, wrist_on: !!r.wrist_on,
  }))
}

// Compute a workout's full breakdown from the minutes in its window.
async function computeBreakdown(db: D1Database, userId: string, startTs: number, endTs: number) {
  const profile = await loadProfile(db, userId)
  const baseline = await loadBaseline(db, userId)
  const mins = await loadMinutes(db, userId, startTs, endTs)
  const worn = mins.filter((m) => m.wrist_on && m.hr_avg > 0)
  const strain = calcStrain(mins, baseline, profile)
  const zones = calcHrZones(mins, baseline, profile)
  const cals = calcCalories(mins, profile)
  const hrr = calcHrRecovery(mins, baseline, profile)
  const hrs = worn.map((m) => m.hr_avg)
  const avgHr = hrs.length ? Math.round(hrs.reduce((a, b) => a + b, 0) / hrs.length) : 0
  const maxHr = worn.length ? Math.round(Math.max(...worn.map((m) => m.hr_max))) : 0
  const durationMin = Math.max(1, Math.round((endTs - startTs) / 60))
  return {
    durationMin, avgHr, maxHr,
    strain: strain.score, calories: cals.kcal, hrr60: hrr.hrr60,
    zones: {
      zone1_min: zones.zone1_min, zone2_min: zones.zone2_min, zone3_min: zones.zone3_min,
      zone4_min: zones.zone4_min, zone5_min: zones.zone5_min,
      max_hr_used: zones.max_hr_used, max_hr_source: zones.max_hr_source,
    },
    confidence: strain.confidence,
  }
}

// POST /workout/start {type} → mint a live workout id.
export async function workoutStart(c: Ctx) {
  const userId = c.get('userId')
  const body = await c.req.json<{ type?: string; title?: string }>().catch(() => ({} as any))
  const type = VALID_TYPES.includes(body.type ?? '') ? body.type! : 'other'
  const id = crypto.randomUUID()
  const start = nowSec()
  await c.env.DB.prepare(
    'INSERT INTO sessions (user_id, id, start_ts, end_ts, type, avg_hr, max_hr, strain, calories, hrr60, zones, confidence, status, source, title) ' +
    "VALUES (?,?,?,?,?,0,0,0,0,NULL,NULL,0,'live','manual',?)",
  ).bind(userId, id, start, start, type, body.title ?? null).run()
  return c.json({ workout_id: id, start_ts: start, type, status: 'live' })
}

// POST /workout/end {workout_id} → close + compute the breakdown from minutes.
export async function workoutEnd(c: Ctx) {
  const userId = c.get('userId')
  const body = await c.req.json<{ workout_id?: string }>().catch(() => ({} as any))
  if (!body.workout_id) return c.json({ error: 'workout_id required' }, 400)
  const w = await c.env.DB.prepare('SELECT * FROM sessions WHERE user_id = ? AND id = ?')
    .bind(userId, body.workout_id).first<any>()
  if (!w) return c.json({ error: 'not found' }, 404)
  const end = nowSec()
  const b = await computeBreakdown(c.env.DB, userId, w.start_ts, end)
  await c.env.DB.prepare(
    'UPDATE sessions SET end_ts = ?, status = "done", avg_hr = ?, max_hr = ?, strain = ?, ' +
    'calories = ?, hrr60 = ?, zones = ?, confidence = ? WHERE user_id = ? AND id = ?',
  ).bind(end, b.avgHr, b.maxHr, b.strain, b.calories, b.hrr60 == null ? null : Math.round(b.hrr60),
    JSON.stringify(b.zones), b.confidence, userId, body.workout_id).run()
  return c.json({ workout_id: body.workout_id, end_ts: end, ...b })
}

const RANGE_DAYS: Record<string, number> = { week: 7, month: 35, quarter: 91, '7d': 7, '30d': 30, '90d': 90 }

// GET /workouts?range=week|month|quarter → list + training-volume summary.
export async function listWorkouts(c: Ctx) {
  const userId = c.get('userId')
  const range = c.req.query('range') || 'month'
  const days = RANGE_DAYS[range] ?? 35
  const from = nowSec() - days * DAY
  const { results } = await c.env.DB.prepare(
    'SELECT id, start_ts, end_ts, type, avg_hr, max_hr, strain, calories, hrr60, zones, status, source, title ' +
    'FROM sessions WHERE user_id = ? AND start_ts >= ? ORDER BY start_ts DESC',
  ).bind(userId, from).all<any>()
  const rows = (results ?? []).map((r: any) => ({
    ...r,
    duration_min: r.end_ts && r.start_ts ? Math.max(0, Math.round((r.end_ts - r.start_ts) / 60)) : 0,
    zones: r.zones ? safeParse(r.zones) : null,
  }))
  // Training-volume summary (HONEST: time/type/zones/calories — no GPS distance,
  // no rep/weight; we don't fabricate what the band can't measure).
  const done = rows.filter((r) => r.status !== 'live')
  const byType: Record<string, { count: number; min: number }> = {}
  let totalMin = 0, totalCal = 0
  const zoneTotals = [0, 0, 0, 0, 0]
  for (const r of done) {
    totalMin += r.duration_min
    totalCal += r.calories ?? 0
    const t = (byType[r.type] ??= { count: 0, min: 0 })
    t.count++; t.min += r.duration_min
    if (r.zones) {
      zoneTotals[0] += r.zones.zone1_min ?? 0; zoneTotals[1] += r.zones.zone2_min ?? 0
      zoneTotals[2] += r.zones.zone3_min ?? 0; zoneTotals[3] += r.zones.zone4_min ?? 0
      zoneTotals[4] += r.zones.zone5_min ?? 0
    }
  }
  const hardest = done.reduce<any>((best, r) => (best == null || (r.strain ?? 0) > (best.strain ?? 0)) ? r : best, null)
  return c.json({
    range,
    workouts: rows,
    summary: {
      count: done.length,
      total_min: totalMin,
      total_calories: Math.round(totalCal),
      by_type: byType,
      zone_min: zoneTotals,
      hardest: hardest ? { id: hardest.id, type: hardest.type, strain: hardest.strain } : null,
    },
  })
}

// GET /workout/:id → one workout's full breakdown + HR/activity timeline.
export async function getWorkout(c: Ctx) {
  const userId = c.get('userId')
  const id = c.req.param('id')
  const w = await c.env.DB.prepare('SELECT * FROM sessions WHERE user_id = ? AND id = ?')
    .bind(userId, id).first<any>()
  if (!w) return c.json({ error: 'not found' }, 404)
  const end = w.end_ts && w.end_ts > w.start_ts ? w.end_ts : nowSec() // live → up to now
  const mins = await loadMinutes(c.env.DB, userId, w.start_ts, end)
  const hr = mins.map((m) => ({ t: m.ts, v: m.hr_avg }))
  return c.json({
    id: w.id, type: w.type, title: w.title, status: w.status, source: w.source,
    start_ts: w.start_ts, end_ts: w.end_ts,
    duration_min: Math.max(0, Math.round((end - w.start_ts) / 60)),
    avg_hr: w.avg_hr, max_hr: w.max_hr, strain: w.strain, calories: w.calories,
    hrr60: w.hrr60, zones: w.zones ? safeParse(w.zones) : null,
    hr,
  })
}

function safeParse(s: string) { try { return JSON.parse(s) } catch { return null } }
