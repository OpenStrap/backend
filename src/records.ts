// records.ts — "Your body over time": personal records, streaks, and baseline
// drift. All computed server-side from the derived tables (daily/sleep/sessions/
// baselines). The client renders only. Honest tallies of real data — a record is
// only shown if it exists; "—" otherwise. GET /records (JWT, user-scoped).

import type { Context } from 'hono'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

const safe = (s: any): any => {
  if (s == null) return null
  if (typeof s === 'object') return s
  try { return JSON.parse(s) } catch { return null }
}

// 'YYYY-MM-DD' → integer day index (days since epoch) for adjacency checks.
const dayIndex = (d: string): number => Math.floor(Date.parse(`${d}T00:00:00Z`) / 86400000)

interface DailyRow {
  date: string; strain: number | null; resting_hr: number | null;
  readiness: number | null; wear_min: number | null; steps: number | null;
  nocturnal: string | null; coach: string | null;
}
interface SleepRow { date: string; duration_min: number | null; efficiency: number | null; regularity: number | null }

export async function getRecords(c: Ctx) {
  const userId = c.get('userId')
  const { results: dailyRaw } = await c.env.DB.prepare(
    'SELECT date, strain, resting_hr, recovery AS readiness, wear_min, steps, nocturnal, coach FROM daily ' +
    'WHERE user_id = ? ORDER BY date ASC',
  ).bind(userId).all<DailyRow>()
  const { results: sleepRaw } = await c.env.DB.prepare(
    'SELECT date, duration_min, efficiency, regularity FROM sleep WHERE user_id = ? ORDER BY date ASC',
  ).bind(userId).all<SleepRow>()
  const { results: sessRaw } = await c.env.DB.prepare(
    'SELECT start_ts, end_ts, type, strain, max_hr, hrr60 FROM sessions WHERE user_id = ? ORDER BY start_ts ASC',
  ).bind(userId).all<any>()
  const baseline = await c.env.DB.prepare(
    'SELECT resting_hr, sleeping_hr, sleep_need_min FROM baselines WHERE user_id = ?',
  ).bind(userId).first<any>()

  const daily = dailyRaw ?? []
  const sleeps = sleepRaw ?? []
  const sessions = sessRaw ?? []

  // ── Personal records (best-ever, each with the date it happened). ──
  type Rec = { value: number; date: string } | null
  const lowestOf = (rows: any[], field: string, dateField = 'date'): Rec => {
    let best: Rec = null
    for (const r of rows) {
      const v = r[field]
      if (v == null || !(v > 0)) continue
      if (!best || v < best.value) best = { value: v, date: r[dateField] }
    }
    return best
  }
  const highestOf = (rows: any[], field: string, dateField = 'date'): Rec => {
    let best: Rec = null
    for (const r of rows) {
      const v = r[field]
      if (v == null) continue
      if (!best || v > best.value) best = { value: v, date: r[dateField] }
    }
    return best
  }

  // lowest sleeping HR ever (from daily.nocturnal JSON).
  let lowestSleepingHr: Rec = null
  for (const d of daily) {
    const n = safe(d.nocturnal)
    const v = n?.sleeping_hr_avg
    if (v == null || !(v > 0)) continue
    if (!lowestSleepingHr || v < lowestSleepingHr.value) lowestSleepingHr = { value: v, date: d.date }
  }

  // top workout (by strain) — keep type + date.
  let topWorkout: { value: number; date: string; type: string } | null = null
  for (const s of sessions) {
    if (s.strain == null) continue
    if (!topWorkout || s.strain > topWorkout.value) {
      topWorkout = {
        value: Math.round(s.strain * 10) / 10,
        date: new Date((s.start_ts ?? 0) * 1000).toISOString().slice(0, 10),
        type: s.type ?? 'workout',
      }
    }
  }

  const records = {
    lowest_rhr: lowestOf(daily, 'resting_hr'),
    top_strain: highestOf(daily, 'strain'),
    top_readiness: highestOf(daily, 'readiness'),
    most_steps: highestOf(daily, 'steps'),
    longest_sleep: highestOf(sleeps, 'duration_min'),
    best_efficiency: highestOf(sleeps, 'efficiency'),
    lowest_sleeping_hr: lowestSleepingHr,
    top_workout: topWorkout,
  }

  // ── Streaks (current run ending at the most recent day with data). ──
  // Consecutive CALENDAR days; a date gap breaks the streak.
  const currentStreak = (
    rows: { date: string }[], ok: (r: any) => boolean,
  ): number => {
    const passing = rows.filter(ok).map((r) => dayIndex(r.date)).sort((a, b) => b - a)
    if (passing.length === 0) return 0
    let streak = 1
    for (let i = 1; i < passing.length; i++) {
      if (passing[i] === passing[i - 1] - 1) streak++
      else if (passing[i] === passing[i - 1]) continue
      else break
    }
    return streak
  }

  const needMin = (baseline?.sleep_need_min && baseline.sleep_need_min >= 180)
    ? baseline.sleep_need_min : 480
  // strain-target streak: strain ≥ that day's coach strain_target.low.
  const hitTarget = (d: DailyRow): boolean => {
    const c2 = safe(d.coach)
    const low = c2?.strain_target?.low
    return low != null && d.strain != null && d.strain >= low
  }

  const streaks = {
    wear: {
      current: currentStreak(daily, (d) => (d.wear_min ?? 0) >= 360),
      label: 'Days worn 6h+',
    },
    sleep: {
      current: currentStreak(sleeps, (s) => (s.duration_min ?? 0) >= 0.8 * needMin),
      label: 'Nights near your sleep need',
    },
    strain_target: {
      current: currentStreak(daily, hitTarget),
      label: 'Days you hit your strain target',
    },
  }

  // ── Baseline drift — resting HR now vs ~30 days of history. ──
  const rhrSeries = daily.filter((d) => d.resting_hr != null && d.resting_hr! > 0)
  let rhrDrift: { now: number; then: number; delta: number; direction: string; days: number } | null = null
  if (rhrSeries.length >= 6) {
    const recent = rhrSeries.slice(-3).map((d) => d.resting_hr as number)
    const older = rhrSeries.slice(0, 3).map((d) => d.resting_hr as number)
    const avg = (a: number[]) => a.reduce((s, v) => s + v, 0) / a.length
    const now = Math.round(avg(recent) * 10) / 10
    const then = Math.round(avg(older) * 10) / 10
    const delta = Math.round((now - then) * 10) / 10
    // For RHR, DOWN is improving fitness.
    const direction = Math.abs(delta) < 1 ? 'flat' : (delta < 0 ? 'improving' : 'rising')
    rhrDrift = { now, then, delta, direction, days: rhrSeries.length }
  }

  return c.json({
    days_tracked: daily.length,
    nights_tracked: sleeps.length,
    workouts_tracked: sessions.length,
    records,
    streaks,
    rhr_drift: rhrDrift,
    baseline: baseline ?? null,
  })
}
