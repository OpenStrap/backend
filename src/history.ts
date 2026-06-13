// history.ts — rich time-range aggregation for the Stats screen.
//
// GET /history?range=7d|30d|90d|365d  (default 30d)
// Returns, for the requested window:
//   - per-metric daily series [{t,v}]  (strain, readiness, resting_hr, calories,
//     wear_min, sleep_duration, sleep_efficiency, sleep_regularity)
//   - per-metric summary {avg, min, max, latest, total?, delta_pct, trend}
//     where delta_pct compares this window's avg vs the immediately-preceding
//     window of equal length (so "vs last week / vs last month" is honest).
//   - calendar[] one row per day (strain/readiness/wear/sleep) for the heatmap.
//   - hr_zones totals over the window (z1..z5 minutes).
//   - worn_days / total_days coverage.
// All JWT, scoped by user_id. Read-only over derived tables.

import type { Context } from 'hono'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

const DAY = 86400
const RANGES: Record<string, number> = { '7d': 7, '30d': 30, '90d': 90, '365d': 365 }

const toEpoch = (d: string) => Math.floor(Date.parse(`${d}T00:00:00Z`) / 1000)

interface Summary {
  avg: number | null
  min: number | null
  max: number | null
  latest: number | null
  total: number | null
  delta_pct: number | null   // this-window avg vs prior-window avg
  trend: 'up' | 'down' | 'flat'
}

// Summarize a numeric field over `cur` rows; delta vs `prev` rows (same length).
function summarize(cur: number[], prev: number[]): Summary {
  if (cur.length === 0) {
    return { avg: null, min: null, max: null, latest: null, total: null, delta_pct: null, trend: 'flat' }
  }
  const sum = cur.reduce((s, v) => s + v, 0)
  const avg = sum / cur.length
  const r2 = (n: number) => Math.round(n * 100) / 100
  let delta_pct: number | null = null
  let trend: 'up' | 'down' | 'flat' = 'flat'
  if (prev.length > 0) {
    const pAvg = prev.reduce((s, v) => s + v, 0) / prev.length
    if (pAvg !== 0) {
      delta_pct = r2(((avg - pAvg) / Math.abs(pAvg)) * 100)
      trend = delta_pct > 1 ? 'up' : delta_pct < -1 ? 'down' : 'flat'
    }
  }
  return {
    avg: r2(avg),
    min: r2(Math.min(...cur)),
    max: r2(Math.max(...cur)),
    latest: r2(cur[cur.length - 1]),
    total: r2(sum),
    delta_pct,
    trend,
  }
}

export async function getHistory(c: Ctx) {
  const userId = c.get('userId')
  const rangeKey = (c.req.query('range') || '30d').toLowerCase()
  const days = RANGES[rangeKey] ?? 30
  const now = Math.floor(Date.now() / 1000)
  const todayStart = Math.floor(now / DAY) * DAY
  const winStart = todayStart - (days - 1) * DAY        // inclusive start of current window
  const prevStart = winStart - days * DAY               // start of the prior equal window
  const winStartDate = new Date(winStart * 1000).toISOString().slice(0, 10)
  const prevStartDate = new Date(prevStart * 1000).toISOString().slice(0, 10)

  // Pull 2× the window so we can compute deltas vs the prior period in one query.
  const { results: dailyRows } = await c.env.DB.prepare(
    'SELECT date, strain, resting_hr, recovery AS readiness, calories, wear_min, steps, hr_zones, anomaly FROM daily ' +
    'WHERE user_id = ? AND date >= ? ORDER BY date ASC',
  ).bind(userId, prevStartDate).all<any>()
  const { results: sleepRows } = await c.env.DB.prepare(
    'SELECT date, duration_min, efficiency, regularity FROM sleep ' +
    'WHERE user_id = ? AND date >= ? ORDER BY date ASC',
  ).bind(userId, prevStartDate).all<any>()

  const daily = dailyRows ?? []
  const sleep = sleepRows ?? []
  const curDaily = daily.filter((r: any) => r.date >= winStartDate)
  const prevDaily = daily.filter((r: any) => r.date < winStartDate)
  // A "night" = a real sleep ≥ 2h. Off-wrist / no-sleep rows (duration ~0) must NOT
  // count toward the sleep average/efficiency, or they drag it to garbage (matches
  // the baseline definition). Stats then reflects nights you actually slept.
  const realNight = (r: any) => (r.duration_min ?? 0) >= 120
  const curSleep = sleep.filter((r: any) => r.date >= winStartDate && realNight(r))
  const prevSleep = sleep.filter((r: any) => r.date < winStartDate && realNight(r))

  // {t,v} series over the current window only (chronological).
  const series = (rows: any[], field: string) =>
    rows.filter((r) => r[field] != null).map((r) => ({ t: toEpoch(r.date), v: Number(r[field]) }))
  // numeric column extractor (drops nulls) for summaries.
  const col = (rows: any[], field: string) =>
    rows.filter((r) => r[field] != null).map((r) => Number(r[field]))

  const metrics = {
    strain: summarize(col(curDaily, 'strain'), col(prevDaily, 'strain')),
    readiness: summarize(col(curDaily, 'readiness'), col(prevDaily, 'readiness')),
    resting_hr: summarize(col(curDaily, 'resting_hr'), col(prevDaily, 'resting_hr')),
    calories: summarize(col(curDaily, 'calories'), col(prevDaily, 'calories')),
    steps: summarize(col(curDaily, 'steps'), col(prevDaily, 'steps')),
    wear_min: summarize(col(curDaily, 'wear_min'), col(prevDaily, 'wear_min')),
    sleep_duration: summarize(col(curSleep, 'duration_min'), col(prevSleep, 'duration_min')),
    sleep_efficiency: summarize(col(curSleep, 'efficiency'), col(prevSleep, 'efficiency')),
    sleep_regularity: summarize(col(curSleep, 'regularity'), col(prevSleep, 'regularity')),
  }

  const seriesOut = {
    strain: series(curDaily, 'strain'),
    readiness: series(curDaily, 'readiness'),
    resting_hr: series(curDaily, 'resting_hr'),
    calories: series(curDaily, 'calories'),
    steps: series(curDaily, 'steps'),
    wear_min: series(curDaily, 'wear_min'),
    sleep_duration: series(curSleep, 'duration_min'),
    sleep_efficiency: series(curSleep, 'efficiency'),
    sleep_regularity: series(curSleep, 'regularity'),
  }

  // Per-day calendar (heatmap) — strain + readiness + wear + sleep, keyed by date.
  const sleepByDate = new Map<string, any>()
  for (const s of curSleep) sleepByDate.set(s.date, s)
  const calendar = curDaily.map((r: any) => ({
    date: r.date,
    t: toEpoch(r.date),
    strain: r.strain ?? null,
    readiness: r.readiness ?? null,
    wear_min: r.wear_min ?? null,
    sleep_min: sleepByDate.get(r.date)?.duration_min ?? null,
  }))

  // HR zone minutes summed over the window.
  const zones = [0, 0, 0, 0, 0]
  for (const r of curDaily) {
    if (!r.hr_zones) continue
    try {
      const z = JSON.parse(r.hr_zones)
      zones[0] += z.zone1_min ?? 0; zones[1] += z.zone2_min ?? 0
      zones[2] += z.zone3_min ?? 0; zones[3] += z.zone4_min ?? 0
      zones[4] += z.zone5_min ?? 0
    } catch { /* ignore */ }
  }

  const wornDays = curDaily.filter((r: any) => (r.wear_min ?? 0) > 0).length

  return c.json({
    range: rangeKey in RANGES ? rangeKey : '30d',
    days,
    from_epoch: winStart,
    to_epoch: todayStart + DAY - 1,
    metrics,
    series: seriesOut,
    calendar,
    hr_zones: { z1: zones[0], z2: zones[1], z3: zones[2], z4: zones[3], z5: zones[4], total: zones.reduce((s, v) => s + v, 0) },
    worn_days: wornDays,
    total_days: days,
  })
}
