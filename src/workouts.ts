// workouts.ts — manual + live + auto-detected workouts, all backed by the SAME
// raw-first stream we already capture. A workout is a time-tagged window over the
// `minute` rollups; the breakdown is computed server-side (reusing the analytics)
// so the live foreground app never has to hold anything — backgrounded, killed, or
// blipped, the record is reconstructed from uploaded data on /workout/end.
import type { Context } from 'hono'
import {
  calcStrain, calcHrZones, calcCalories, calcHrRecovery, detectSessions,
  type Minute, type Profile, type Baseline,
} from 'openstrap-analytics'
import { readMinutes } from './minute_store'
import { cached } from './cache'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>
const nowSec = () => Math.floor(Date.now() / 1000)
const DAY = 86400
const startOfDayUtc = (ts: number) => Math.floor(ts / DAY) * DAY
const ymd = (ts: number) => new Date(ts * 1000).toISOString().slice(0, 10)

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
  // Day-packed store. Only ever called at workout-end (recent/hot) → D1 minute_day.
  const recs = await readMinutes({ DB: db }, userId, from, to)
  return recs.map((r) => ({
    ts: r.ts_min, hr_avg: r.hr_avg, hr_min: r.hr_min, hr_max: r.hr_max,
    hr_n: r.hr_n, activity: r.activity, steps: r.steps, wrist_on: !!r.wrist_on,
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

// DELETE /workout/:id → soft-delete (tombstone). We DON'T hard-delete because
// auto-detected sessions would just be re-created on the next analytics re-derive;
// the tombstone (status='deleted') is respected by detectSessions so it stays gone.
export async function deleteWorkout(c: Ctx) {
  const userId = c.get('userId')
  const id = c.req.param('id')
  const r = await c.env.DB.prepare(
    "UPDATE sessions SET status = 'deleted' WHERE user_id = ? AND id = ?",
  ).bind(userId, id).run()
  return c.json({ ok: true, deleted: r.meta?.changes ?? 0 })
}

const RANGE_DAYS: Record<string, number> = { week: 7, month: 35, quarter: 91, '7d': 7, '30d': 30, '90d': 90 }

// GET /workouts?range=week|month|quarter → list + training-volume summary.
export async function listWorkouts(c: Ctx) {
  const userId = c.get('userId')
  await ensureTodayWorkouts(c.env.DB, userId) // on-read auto-detect + stale-live close (throttled)
  const range = c.req.query('range') || 'month'
  const days = RANGE_DAYS[range] ?? 35
  const from = nowSec() - days * DAY
  const { results } = await c.env.DB.prepare(
    'SELECT id, start_ts, end_ts, type, avg_hr, max_hr, strain, calories, hrr60, zones, status, source, title, ' +
    'segments, detected_type, type_confidence, type_source ' +
    "FROM sessions WHERE user_id = ? AND start_ts >= ? AND status != 'deleted' ORDER BY start_ts DESC",
  ).bind(userId, from).all<any>()
  const rows = (results ?? []).map((r: any) => ({
    ...r,
    duration_min: r.end_ts && r.start_ts ? Math.max(0, Math.round((r.end_ts - r.start_ts) / 60)) : 0,
    zones: r.zones ? safeParse(r.zones) : null,
    segments: r.segments ? safeParse(r.segments) : null,
    // distinguish in UI: detected (auto/auto_live) vs selected (manual)
    detected: r.source === 'auto' || r.source === 'auto_live',
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
  // Calibration ledger: of the workouts the user confirmed/corrected, how often did the
  // classifier already have it right? Surfaced so we know when the model needs retraining.
  const acc = await c.env.DB.prepare(
    "SELECT COUNT(*) AS total, SUM(CASE WHEN type = detected_type THEN 1 ELSE 0 END) AS correct " +
    "FROM sessions WHERE user_id = ? AND detected_type IS NOT NULL AND type_source IN ('confirmed','corrected')",
  ).bind(userId).first<{ total: number; correct: number }>()
  const accTotal = acc?.total ?? 0
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
      // model accuracy from user feedback (null until the user has confirmed/corrected any)
      classifier: { reviewed: accTotal, correct: acc?.correct ?? 0, accuracy: accTotal > 0 ? Math.round(((acc!.correct) / accTotal) * 100) / 100 : null },
    },
  })
}

// POST /workout/:id/type { type } — user confirms or corrects an auto-detected type.
// Feeds the calibration ledger (was the model right?) and pins the type so re-derivation
// won't overwrite it. The accumulated (detected_type → corrected type) pairs are the
// labelled dataset that will train the real classifier.
export async function setWorkoutType(c: Ctx) {
  const userId = c.get('userId')
  const id = c.req.param('id')
  const body = await c.req.json<{ type?: string }>().catch(() => ({} as any))
  if (!body.type || !VALID_TYPES.includes(body.type)) return c.json({ error: 'invalid type' }, 400)
  const w = await c.env.DB.prepare('SELECT detected_type FROM sessions WHERE user_id = ? AND id = ?')
    .bind(userId, id).first<{ detected_type: string | null }>()
  if (!w) return c.json({ error: 'not found' }, 404)
  const source = w.detected_type && w.detected_type === body.type ? 'confirmed' : 'corrected'
  await c.env.DB.prepare(
    'UPDATE sessions SET type = ?, type_source = ?, type_confidence = 1.0 WHERE user_id = ? AND id = ?',
  ).bind(body.type, source, userId, id).run()
  return c.json({ ok: true, type: body.type, type_source: source })
}

/**
 * sweepWorkoutDetection(env) — the #D incremental cron. Re-derive TODAY's auto-workouts
 * for every user who's ingested since their last close (dirty=1), so a workout surfaces
 * ~one tick (≤10 min) after it ends WITHOUT the app being opened. Reuses the throttled
 * ensureTodayWorkouts (≤1 detect / 120 s / user) and is bounded per tick. The bout closes
 * at its last data minute naturally (re-derivation over available minutes).
 */
export async function sweepWorkoutDetection(env: { DB: D1Database }, limit = 200): Promise<number> {
  const { results } = await env.DB.prepare(
    'SELECT user_id FROM analytics_cursor WHERE dirty = 1 LIMIT ?',
  ).bind(limit).all<{ user_id: string }>()
  let n = 0
  for (const r of results ?? []) {
    try { await ensureTodayWorkouts(env.DB, r.user_id); n++ } catch (e) { console.error('wkt detect failed', r.user_id, e) }
  }
  return n
}

// GET /workout/:id → full breakdown + HR/activity timeline + derived effort
// analytics (zone bands w/ bpm ranges + %, min HR, HR drift, time-to-peak, the
// HR-recovery curve, steps/cadence, wrist coverage). Derived fields are computed
// on READ from the minute stream — no schema change, always re-derivable.
export async function getWorkout(c: Ctx) {
  const userId = c.get('userId')
  const id = c.req.param('id')
  const w = await c.env.DB.prepare('SELECT * FROM sessions WHERE user_id = ? AND id = ?')
    .bind(userId, id).first<any>()
  if (!w || w.status === 'deleted') return c.json({ error: 'not found' }, 404)
  const end = w.end_ts && w.end_ts > w.start_ts ? w.end_ts : nowSec() // live → up to now
  const mins = await loadMinutes(c.env.DB, userId, w.start_ts, end)
  const tail = await loadMinutes(c.env.DB, userId, end, end + 4 * 60) // for recovery curve
  const baseline = await loadBaseline(c.env.DB, userId)
  const zones = w.zones ? safeParse(w.zones) : null
  const hr = mins.map((m) => ({ t: m.ts, v: m.hr_avg }))
  const effort = deriveEffort(mins, tail, w, end, zones, baseline)
  return c.json({
    id: w.id, type: w.type, title: w.title, status: w.status, source: w.source,
    start_ts: w.start_ts, end_ts: w.end_ts,
    duration_min: Math.max(0, Math.round((end - w.start_ts) / 60)),
    avg_hr: w.avg_hr, max_hr: w.max_hr, strain: w.strain, calories: w.calories,
    hrr60: w.hrr60, zones, hr,
    ...effort,
  })
}

const avg = (xs: number[]) => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : 0

// Per-read effort analytics derived from the workout's minute stream.
function deriveEffort(
  mins: Minute[], tail: Minute[], w: any, end: number, zones: any, baseline: Baseline,
) {
  const worn = mins.filter((m) => m.wrist_on && m.hr_avg > 0)
  const resting = baseline.resting_hr || 60

  // Lowest HR seen during the effort.
  const minHr = worn.length
    ? Math.round(Math.min(...worn.map((m) => (m.hr_min && m.hr_min > 0 ? m.hr_min : m.hr_avg))))
    : null

  // Cardiac drift: 2nd-half vs 1st-half mean HR (positive = HR crept up = fatigue/heat).
  let hrDriftPct: number | null = null
  if (worn.length >= 6) {
    const half = Math.floor(worn.length / 2)
    const a = avg(worn.slice(0, half).map((m) => m.hr_avg))
    const b = avg(worn.slice(half).map((m) => m.hr_avg))
    if (a > 0) hrDriftPct = Math.round(((b - a) / a) * 1000) / 10
  }

  // Time from start to peak HR (minutes).
  let timeToPeakMin: number | null = null
  if (worn.length) {
    let pkTs = 0, pkV = -1
    for (const m of worn) { const v = m.hr_max || m.hr_avg; if (v > pkV) { pkV = v; pkTs = m.ts } }
    if (pkTs > 0) timeToPeakMin = Math.max(0, Math.round((pkTs - w.start_ts) / 60))
  }

  // Zone bands with bpm ranges (50/60/70/80/90% of maxHR) + minutes + share.
  const maxHrUsed = (zones?.max_hr_used) || baseline.max_hr || 190
  const ZP = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
  const ZN = ['Very light', 'Light', 'Moderate', 'Hard', 'Maximum']
  const zmin = zones
    ? [zones.zone1_min, zones.zone2_min, zones.zone3_min, zones.zone4_min, zones.zone5_min].map((x: any) => x || 0)
    : [0, 0, 0, 0, 0]
  const ztot = zmin.reduce((a: number, b: number) => a + b, 0)
  const zoneBands = ZN.map((name, i) => ({
    zone: i + 1, name,
    lo: Math.round(ZP[i] * maxHrUsed),
    hi: i === 4 ? Math.max(Math.round(maxHrUsed), w.max_hr || 0) : Math.round(ZP[i + 1] * maxHrUsed),
    min: zmin[i],
    pct: ztot > 0 ? Math.round((zmin[i] / ztot) * 100) : 0,
  }))

  // HR-recovery curve: drop from end-HR at 60/120/180s after the effort ends.
  const recoveryCurve: { sec: number; hr: number; drop: number }[] = []
  const endHr = worn.length ? worn[worn.length - 1].hr_avg : 0
  const stream = [...mins, ...tail].filter((m) => m.hr_avg > 0)
  const hrAt = (ts: number): number | null => {
    let best: Minute | null = null, bestD = 90
    for (const m of stream) { const d = Math.abs(m.ts - ts); if (d <= bestD) { bestD = d; best = m } }
    return best ? best.hr_avg : null
  }
  if (endHr > 0 && tail.length) {
    for (const sec of [60, 120, 180]) {
      const v = hrAt(end + sec)
      if (v != null) recoveryCurve.push({ sec, hr: Math.round(v), drop: Math.max(0, Math.round(endHr - v)) })
    }
  }

  // Output: steps, cadence (steps/min over moving minutes), wrist coverage.
  const steps = Math.round(mins.reduce((s, m) => s + (m.steps || 0), 0))
  const movingMin = mins.filter((m) => (m.steps || 0) > 0).length
  const cadenceSpm = movingMin > 0 ? Math.round(steps / movingMin) : null
  const coveragePct = mins.length ? Math.round((worn.length / mins.length) * 100) : null

  return {
    min_hr: minHr,
    hr_drift_pct: hrDriftPct,
    time_to_peak_min: timeToPeakMin,
    zone_bands: zoneBands,
    recovery_curve: recoveryCurve,
    steps,
    cadence_spm: cadenceSpm,
    coverage_pct: coveragePct,
  }
}

// Auto-stop forgotten live workouts. Run from cron. A live session is closed when
// HR has been back near resting for QUIET_MIN minutes (the effort clearly ended),
// or after MAX_LIVE_HOURS as a hard safety cap. End time = the last elevated minute,
// so a forgotten "stop" never inflates duration with idle time. Returns # closed.
const QUIET_MIN = 12
const MAX_LIVE_HOURS = 6
export async function autoCloseStaleWorkouts(db: D1Database, userId?: string): Promise<number> {
  const now = nowSec()
  // Optionally scope to ONE user (on-demand read path); else all (nightly safety net).
  const stmt = userId
    ? db.prepare("SELECT user_id, id, start_ts FROM sessions WHERE status = 'live' AND user_id = ?").bind(userId)
    : db.prepare("SELECT user_id, id, start_ts FROM sessions WHERE status = 'live'")
  const { results } = await stmt.all<any>()
  let closed = 0
  for (const s of results ?? []) {
    const baseline = await loadBaseline(db, s.user_id)
    const resting = baseline.resting_hr || 60
    const maxHr = baseline.max_hr || 190
    const calm = resting + Math.max(15, Math.round(0.15 * (maxHr - resting))) // ~very-light effort
    const mins = await loadMinutes(db, s.user_id, s.start_ts, now)
    const worn = mins.filter((m) => m.wrist_on && m.hr_avg > 0)
    const ageH = (now - s.start_ts) / 3600

    let lastElevated = 0
    for (const m of worn) if (m.hr_avg > calm) lastElevated = m.ts
    const recent = worn.filter((m) => m.ts >= now - QUIET_MIN * 60)
    const calmedDown = lastElevated > 0 && recent.length >= 3 && recent.every((m) => m.hr_avg <= calm)
    const tooOld = ageH >= MAX_LIVE_HOURS
    if (!calmedDown && !tooOld) continue

    let end = lastElevated > 0 ? lastElevated + 60 : s.start_ts + 60
    if (end > now) end = now
    if (end <= s.start_ts) end = s.start_ts + 60
    const b = await computeBreakdown(db, s.user_id, s.start_ts, end)
    await db.prepare(
      'UPDATE sessions SET end_ts = ?, status = "done", avg_hr = ?, max_hr = ?, strain = ?, ' +
      'calories = ?, hrr60 = ?, zones = ?, confidence = ? WHERE user_id = ? AND id = ?',
    ).bind(end, b.avgHr, b.maxHr, b.strain, b.calories, b.hrr60 == null ? null : Math.round(b.hrr60),
      JSON.stringify(b.zones), b.confidence, s.user_id, s.id).run()
    closed++
  }
  return closed
}

function safeParse(s: string) { try { return JSON.parse(s) } catch { return null } }

// ── On-demand auto-workout detection (replaces the cron sweep) ────────────────
// detectSessions used to run only inside the once-a-day close_day (via processUser),
// so an auto-detected workout didn't surface until the next wake. We now run it
// ON READ for TODAY, mirroring processUser Pass-2's session block EXACTLY (delete
// auto-non-deleted, insert 'done'/'auto'; manual/edited/deleted sessions untouched),
// so the on-read path and the day-close produce identical rows.
async function detectStoreDay(db: D1Database, userId: string, dayStart: number, baseline: Baseline, profile: Profile): Promise<void> {
  const recs = await readMinutes({ DB: db }, userId, dayStart, dayStart + DAY)
  if (!recs.length) return
  const dayMin: Minute[] = recs.map((r) => ({
    ts: r.ts_min, hr_avg: r.hr_avg, hr_min: r.hr_min, hr_max: r.hr_max,
    hr_n: r.hr_n, activity: r.activity, steps: r.steps, wrist_on: !!r.wrist_on,
    act_class: r.act_class as Minute['act_class'],
  }))
  const sessions = detectSessions(dayMin, baseline, profile)
  // Reconcile: don't clobber the user's manual workouts, deleted tombstones, OR any
  // already-typed live-detected sessions (source='auto_live'); only the minute-detector's
  // own 'auto' rows are re-derived. Then skip auto bouts that overlap a manual/auto_live
  // session so a live-streamed workout isn't double-counted by the backstop.
  const { results: keep } = await db.prepare(
    "SELECT start_ts, end_ts FROM sessions WHERE user_id = ? AND start_ts < ? AND end_ts > ? AND status != 'deleted' AND source IN ('manual','auto_live')",
  ).bind(userId, dayStart + DAY, dayStart).all<{ start_ts: number; end_ts: number }>()
  const covered = (keep ?? []) as { start_ts: number; end_ts: number }[]
  const overlaps = (s: { start_ts: number; end_ts: number }) =>
    covered.some((k) => s.start_ts < k.end_ts && s.end_ts > k.start_ts)
  const stmts: D1PreparedStatement[] = [
    db.prepare("DELETE FROM sessions WHERE user_id = ? AND start_ts >= ? AND start_ts < ? AND (source IS NULL OR source = 'auto') AND status != 'deleted'")
      .bind(userId, dayStart, dayStart + DAY),
  ]
  for (const s of sessions) {
    if (overlaps(s)) continue // a manual/live-detected session already owns this window
    const sid = `${userId}:${s.start_ts}`
    stmts.push(db.prepare(
      'INSERT INTO sessions (user_id, id, start_ts, end_ts, type, avg_hr, max_hr, strain, calories, hrr60, zones, confidence, status, source, segments, detected_type, type_confidence, type_source) ' +
      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'done','auto',?,?,?,'model') ON CONFLICT(user_id, id) DO UPDATE SET " +
      'end_ts=excluded.end_ts, type=excluded.type, avg_hr=excluded.avg_hr, max_hr=excluded.max_hr, ' +
      'strain=excluded.strain, calories=excluded.calories, hrr60=excluded.hrr60, zones=excluded.zones, confidence=excluded.confidence, ' +
      // Preserve a user-confirmed/corrected type across re-derivation; only refresh the model fields.
      'segments=excluded.segments, detected_type=excluded.detected_type, ' +
      'type_confidence=CASE WHEN sessions.type_source IN (\'confirmed\',\'corrected\') THEN sessions.type_confidence ELSE excluded.type_confidence END, ' +
      'type=CASE WHEN sessions.type_source IN (\'confirmed\',\'corrected\') THEN sessions.type ELSE excluded.type END',
    ).bind(userId, sid, s.start_ts, s.end_ts, s.type, Math.round(s.avg_hr), Math.round(s.max_hr),
      s.strain, s.kcal, s.hrr60 == null ? null : Math.round(s.hrr60), JSON.stringify(s.zones), s.confidence,
      s.segments ? JSON.stringify(s.segments) : null, s.detected_type ?? s.type, s.type_confidence))
  }
  await db.batch(stmts)
}

/**
 * On-read freshness for workouts (the wake-trigger replacement for the cron sweep).
 * Closes this user's forgotten LIVE workouts and (re)detects TODAY's auto sessions.
 * THROTTLED via read_cache (~120s) so a burst of reads costs one detect, not N — and
 * it only ever runs for users actively opening the app. Call from any workout-
 * surfacing read endpoint. The nightly cron still runs autoCloseStaleWorkouts as a
 * safety net for users who never open the app; close_day still re-derives at wake.
 */
export async function ensureTodayWorkouts(db: D1Database, userId: string): Promise<void> {
  const today = ymd(nowSec())
  await cached(db, userId, `wkscan:${today}`, 120, async () => {
    await autoCloseStaleWorkouts(db, userId)
    const baseline = await loadBaseline(db, userId)
    const profile = await loadProfile(db, userId)
    await detectStoreDay(db, userId, startOfDayUtc(nowSec()), baseline, profile)
    return { scanned: nowSec() }
  })
}
