// query.ts — read API over derived tables + downsampled `minute`. All JWT,
// all scoped by user_id. Every metric carries {value, unit, confidence, tier,
// label, inputs_used}; rows include their `flags` JSON.

import type { Context } from 'hono'
import { calcFitnessTrend, type DayHistory } from 'openstrap-analytics'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

const nowSec = () => Math.floor(Date.now() / 1000)
const todayDate = () => new Date().toISOString().slice(0, 10)

// Wrap a scalar in the canonical metric envelope using a flags entry if present.
function metric(value: any, unit: string, label: string, flags: any, key: string) {
  const f = flags?.[key]
  return {
    value: value ?? null,
    unit,
    confidence: f?.c ?? (value == null ? 0 : null),
    tier: f?.tier ?? null,
    label: f?.label ?? label,
    inputs_used: f?.inputs_used ?? [],
  }
}

const parseFlags = (s: string | null): any => {
  if (!s) return {}
  try { return JSON.parse(s) } catch { return {} }
}

// GET /today — today's daily + last night's sleep, as metric envelopes.
export async function getToday(c: Ctx) {
  const userId = c.get('userId')
  const date = todayDate()
  const daily = await c.env.DB.prepare('SELECT * FROM daily WHERE user_id = ? AND date = ?')
    .bind(userId, date).first<any>()
  const sleep = await c.env.DB.prepare('SELECT * FROM sleep WHERE user_id = ? ORDER BY date DESC LIMIT 1')
    .bind(userId).first<any>()
  const baseline = await c.env.DB.prepare('SELECT * FROM baselines WHERE user_id = ?')
    .bind(userId).first<any>()

  // Live status: most recent minute (HR + worn) in the last 10 minutes.
  const live = await c.env.DB.prepare(
    'SELECT ts_min, hr_avg, wrist_on FROM minute WHERE user_id = ? AND ts_min >= ? ORDER BY ts_min DESC LIMIT 1',
  ).bind(userId, nowSec() - 600).first<any>()

  const df = parseFlags(daily?.flags)
  const sf = parseFlags(sleep?.flags)

  // Resting-HR delta vs baseline (negative = below baseline = better). HIGH tier
  // because both inputs are measured. Null when either side is missing.
  const rhrBaseline = baseline?.resting_hr ?? null
  const rhrDelta = (daily?.resting_hr != null && rhrBaseline != null)
    ? Math.round((daily.resting_hr - rhrBaseline) * 10) / 10
    : null
  // Plausibility floor: a sleep need < 3h is never real (sparse-data garbage) → 8h.
  const sleepNeedMin =
    (baseline?.sleep_need_min && baseline.sleep_need_min >= 180)
      ? baseline.sleep_need_min : 480

  return c.json({
    date,
    // Deterministic coach plan + strain target + readiness contributors + summary.
    coach: daily?.coach ? safeParse(daily.coach) : null,
    // HRV stress (Baevsky SI + LF/HF, personal-relative) — includes its drivers.
    stress: daily?.stress ? safeParse(daily.stress) : null,
    // Multivariate illness signal (Mahalanobis) — "a signal, not a diagnosis".
    illness: daily?.illness ? safeParse(daily.illness) : null,
    // Nocturnal arousal / sleep-stress (HR surges + motion during sleep).
    sleep_stress: daily?.sleep_stress ? safeParse(daily.sleep_stress) : null,
    // Per-metric driver graph for today ("what affected what" — tappable in UI).
    drivers: daily?.drivers ? safeParse(daily.drivers) : null,
    // Nocturnal-heart summary (sleeping-HR dynamics).
    nocturnal: daily?.nocturnal ? safeParse(daily.nocturnal) : null,
    // Respiratory rate (PPG) — GATED: only surfaced once it validates (conf ≥ 0.5).
    resp: (daily?.resp_rate != null && (daily?.resp_conf ?? 0) >= 0.5)
      ? { value: Math.round(daily.resp_rate * 10) / 10, confidence: daily.resp_conf }
      : null,
    // Nocturnal HRV (RMSSD, ms) from beat-to-beat RR — the real one, measured.
    hrv: (daily?.hrv_rmssd != null && (daily?.hrv_conf ?? 0) > 0)
      ? {
          rmssd: Math.round(daily.hrv_rmssd * 10) / 10,
          confidence: daily.hrv_conf,
          // Personal RMSSD baseline so clients can show HRV relative to "your normal".
          baseline: baseline?.hrv_rmssd != null ? Math.round(baseline.hrv_rmssd * 10) / 10 : null,
          tier: 'HIGH',
          label: 'Nocturnal HRV (RMSSD)',
        }
      : null,
    // Skin temp + blood-oxygen as RELATIVE indices (raw ADC − personal baseline);
    // the band never sends a finished °C / %, so we only show the deviation.
    skin_temp: daily?.skin_temp_idx != null
      ? { value: daily.skin_temp_idx, unit: 'Δ', tier: 'RELATIVE', label: 'Skin temp vs baseline' }
      : null,
    spo2: daily?.spo2_idx != null
      ? { value: daily.spo2_idx, unit: 'Δ', tier: 'RELATIVE', label: 'Blood-oxygen vs baseline' }
      : null,
    daily: daily ? {
      strain: metric(daily.strain, 'score', 'Strain', df, 'strain'),
      resting_hr: metric(daily.resting_hr, 'bpm', 'Resting HR', df, 'resting_hr'),
      resting_hr_delta: {
        value: rhrDelta,
        unit: 'bpm',
        confidence: rhrDelta == null ? 0 : (df?.resting_hr?.c ?? 1),
        tier: 'HIGH',
        label: 'vs baseline',
        inputs_used: ['resting_hr', 'baseline.resting_hr'],
      },
      // Recovery is HRV-based (Plews lnRMSSD z) — replaces the old heuristic readiness.
      recovery: metric(daily.recovery, 'score', 'Recovery (HRV)', df, 'recovery'),
      calories: metric(daily.calories, 'kcal', 'Active calories (est.)', df, 'calories'),
      steps: metric(daily.steps, 'steps', 'Steps (est.)', df, 'steps'),
      // Wear is a direct count of worn minutes — full confidence when present
      // (otherwise the UI hides any metric with null/0 confidence).
      wear_min: { ...metric(daily.wear_min, 'min', 'Worn', df, 'wear'), confidence: daily.wear_min != null ? 1 : 0, tier: 'AUTH' },
      hr_zones: daily.hr_zones ? JSON.parse(daily.hr_zones) : null,
      acwr: metric(daily.acwr, 'ratio', `Load`, df, 'load'),
      fitness_trend: metric(daily.fitness_trend, '', 'Fitness trend', df, 'fitness'),
      anomaly: daily.anomaly ? JSON.parse(daily.anomaly) : { signal: false },
      confidence: daily.confidence,
      flags: df,
    } : null,
    sleep: sleep ? {
      date: sleep.date,
      duration_min: metric(sleep.duration_min, 'min', 'Sleep', sf, 'duration'),
      // Sleep need (minutes) sourced from the user's baseline — drives the
      // asleep-vs-need ring. HIGH tier when present.
      need_min: {
        value: sleepNeedMin,
        unit: 'min',
        confidence: sleepNeedMin == null ? 0 : 0.8,
        tier: 'HIGH',
        label: 'Sleep need',
        inputs_used: ['baseline.sleep_need_min'],
      },
      efficiency: metric(sleep.efficiency, 'ratio', 'Sleep efficiency', sf, 'duration'),
      onset_ts: sleep.onset_ts, wake_ts: sleep.wake_ts,
      stages: { light_min: sleep.light_min, deep_min: sleep.deep_min, rem_min: sleep.rem_min },
      stages_meta: sf.stages ?? { c: 0, tier: 'ESTIMATE', label: 'Sleep stages (beta)' },
      regularity: sleep.regularity,
      confidence: sleep.confidence,
      flags: sf,
    } : null,
    live: live ? { ts: live.ts_min, hr: live.hr_avg, wrist_on: !!live.wrist_on } : null,
  })
}

// Generic date-range fetch over a derived table.
function rangeRows(table: string) {
  return async (c: Ctx) => {
    const from = c.req.query('from'), to = c.req.query('to')
    let sql = `SELECT * FROM ${table} WHERE user_id = ?`
    const binds: any[] = [c.get('userId')]
    if (from) { sql += ' AND date >= ?'; binds.push(from) }
    if (to) { sql += ' AND date <= ?'; binds.push(to) }
    sql += ' ORDER BY date DESC LIMIT 120'
    const { results } = await c.env.DB.prepare(sql).bind(...binds).all<any>()
    const rows = (results ?? []).map((r: any) => ({ ...r, flags: parseFlags(r.flags) }))
    return c.json(rows)
  }
}

// GET /sleep?from&to — nightly rows (newest first). Each row also carries the
// user's baseline `need_min` so the asleep-vs-need ring can render from /sleep
// alone (the screen doesn't fetch /trends).
export async function getSleep(c: Ctx) {
  const userId = c.get('userId')
  const from = c.req.query('from'), to = c.req.query('to')
  let sql = 'SELECT * FROM sleep WHERE user_id = ?'
  const binds: any[] = [userId]
  if (from) { sql += ' AND date >= ?'; binds.push(from) }
  if (to) { sql += ' AND date <= ?'; binds.push(to) }
  sql += ' ORDER BY date DESC LIMIT 120'
  const { results } = await c.env.DB.prepare(sql).bind(...binds).all<any>()
  const baseline = await c.env.DB.prepare('SELECT sleep_need_min FROM baselines WHERE user_id = ?')
    .bind(userId).first<any>()
  // Plausibility floor: a sleep need < 3h is never real (sparse-data garbage) → 8h.
  const needMin =
    (baseline?.sleep_need_min && baseline.sleep_need_min >= 180)
      ? baseline.sleep_need_min : 480
  const rows = (results ?? []).map((r: any) => ({
    ...r,
    flags: parseFlags(r.flags),
    need_min: needMin,
  }))
  return c.json(rows)
}

export const getStrain = rangeRows('daily')

// GET /sessions?from&to — auto-detected workouts (by start_ts unix range).
export async function getSessions(c: Ctx) {
  const from = parseInt(c.req.query('from') || '0')
  const to = parseInt(c.req.query('to') || String(nowSec()))
  const { results } = await c.env.DB.prepare(
    'SELECT * FROM sessions WHERE user_id = ? AND start_ts >= ? AND start_ts <= ? ORDER BY start_ts DESC LIMIT 200',
  ).bind(c.get('userId'), from, to).all<any>()
  const rows = (results ?? []).map((r: any) => ({
    ...r,
    zones: r.zones ? JSON.parse(r.zones) : null,
    duration_min: (r.end_ts != null && r.start_ts != null)
      ? Math.round((r.end_ts - r.start_ts) / 60)
      : null,
  }))
  return c.json(rows)
}

// GET /trends?days=30 — RHR / fitness / ACWR / sleep / wear series + baselines.
export async function getTrends(c: Ctx) {
  const userId = c.get('userId')
  const days = Math.min(365, Math.max(1, parseInt(c.req.query('days') || '30')))
  const baseline = await c.env.DB.prepare('SELECT * FROM baselines WHERE user_id = ?')
    .bind(userId).first<any>()
  const { results: daily } = await c.env.DB.prepare(
    'SELECT date, strain, resting_hr, recovery AS readiness, acwr, fitness_trend, wear_min, calories, anomaly FROM daily ' +
    'WHERE user_id = ? ORDER BY date DESC LIMIT ?',
  ).bind(userId, days).all<any>()
  const { results: sleepRows } = await c.env.DB.prepare(
    'SELECT date, duration_min, efficiency, regularity FROM sleep WHERE user_id = ? ORDER BY date DESC LIMIT ?',
  ).bind(userId, days).all<any>()

  // Chronological order for series + fitness regression.
  const dailyAsc = (daily ?? []).reverse()
  const sleepAsc = (sleepRows ?? []).reverse()

  // date (YYYY-MM-DD) → epoch seconds (UTC midnight) for {t,v} series points.
  const toEpoch = (d: string) => Math.floor(Date.parse(`${d}T00:00:00Z`) / 1000)
  const series = (rows: any[], field: string) =>
    rows
      .filter((r) => r[field] != null)
      .map((r) => ({ t: toEpoch(r.date), v: Number(r[field]) }))

  // Representative session HRR60 per day (max that day), for the HRR fitness slope.
  const since = days > 0 && dailyAsc.length
    ? toEpoch(dailyAsc[0].date)
    : Math.floor(Date.now() / 1000) - days * 86400
  const { results: sess } = await c.env.DB.prepare(
    'SELECT start_ts, hrr60 FROM sessions WHERE user_id = ? AND start_ts >= ? ORDER BY start_ts ASC',
  ).bind(userId, since).all<any>()
  const hrrByDate = new Map<string, number>()
  for (const s of (sess ?? [])) {
    if (s.hrr60 == null) continue
    const d = new Date(s.start_ts * 1000).toISOString().slice(0, 10)
    hrrByDate.set(d, Math.max(hrrByDate.get(d) ?? 0, s.hrr60))
  }

  // Fitness trend over the window — direction + RHR & HRR slopes (NEVER VO2max).
  const dayHistory: DayHistory[] = dailyAsc.map((r) => ({
    resting_hr: r.resting_hr ?? undefined,
    hrr60: hrrByDate.get(r.date),
  }))
  const fitness = calcFitnessTrend(dayHistory)

  // Anomaly: surface the most recent daily's anomaly signal honestly.
  const lastDaily = dailyAsc.length ? dailyAsc[dailyAsc.length - 1] : null
  const lastAnomaly = lastDaily?.anomaly ? safeParse(lastDaily.anomaly) : null
  const anomaly = {
    signal: !!lastAnomaly?.signal,
    message: lastAnomaly?.signal
      ? (lastAnomaly.note || 'Possible strain on your body — a signal, not a diagnosis.')
      : '',
  }

  return c.json({
    baseline,
    daily: dailyAsc,
    sleep: sleepAsc,
    series: {
      resting_hr: series(dailyAsc, 'resting_hr'),
      strain: series(dailyAsc, 'strain'),
      acwr: series(dailyAsc, 'acwr'),
      wear: series(dailyAsc, 'wear_min'),
      sleep_duration: series(sleepAsc, 'duration_min'),
    },
    fitness_direction: fitness.direction,
    rhr_slope: fitness.rhr_slope,
    hrr_slope: fitness.hrr_slope,
    anomaly,
  })
}

const safeParse = (s: any): any => {
  if (s == null) return null
  if (typeof s === 'object') return s
  try { return JSON.parse(s) } catch { return null }
}

// GET /chart?metric=hr&from&to — minute series, downsampled server-side to ≤500
// points via min/max bucketing (preserves extremes for HR/activity charts).
export async function getChart(c: Ctx) {
  const userId = c.get('userId')
  const metricName = (c.req.query('metric') || 'hr').toLowerCase()
  const from = parseInt(c.req.query('from') || String(nowSec() - 86400))
  const to = parseInt(c.req.query('to') || String(nowSec()))
  // allowlist: only hr_avg + activity + steps are chartable minute columns.
  const col = metricName === 'activity'
      ? 'activity'
      : metricName === 'steps'
          ? 'steps'
          : 'hr_avg'

  const { results } = await c.env.DB.prepare(
    `SELECT ts_min, ${col} AS v, wrist_on FROM minute WHERE user_id = ? AND ts_min >= ? AND ts_min <= ? ORDER BY ts_min ASC`,
  ).bind(userId, from, to).all<{ ts_min: number; v: number | null; wrist_on: number }>()
  const rows = results ?? []

  const MAX = 500
  let points: { ts: number; v: number | null }[]
  if (rows.length <= MAX) {
    points = rows.map((r) => ({ ts: r.ts_min, v: r.v }))
  } else {
    // Min/max bucketing: split into MAX/2 buckets, emit the min and max of each.
    const nBuckets = Math.floor(MAX / 2)
    const span = (to - from) / nBuckets
    points = []
    for (let i = 0; i < nBuckets; i++) {
      const lo = from + i * span, hi = lo + span
      const bucket = rows.filter((r) => r.ts_min >= lo && r.ts_min < hi && r.v != null)
      if (bucket.length === 0) continue
      let min = bucket[0], max = bucket[0]
      for (const r of bucket) {
        if ((r.v ?? 0) < (min.v ?? 0)) min = r
        if ((r.v ?? 0) > (max.v ?? 0)) max = r
      }
      if (min.ts_min <= max.ts_min) { points.push({ ts: min.ts_min, v: min.v }); if (max !== min) points.push({ ts: max.ts_min, v: max.v }) }
      else { points.push({ ts: max.ts_min, v: max.v }); points.push({ ts: min.ts_min, v: min.v }) }
    }
  }
  return c.json({ metric: metricName, unit: col === 'hr_avg' ? 'bpm' : col, from, to, points })
}
