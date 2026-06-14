// trend.ts — the Metric Explorer's data source. ONE endpoint serves every metric
// at every time scale, pre-aggregated SERVER-SIDE (heavy compute server-side,
// client renders):
//   scale=week    → 7 daily bars
//   scale=month   → ~4–5 weekly-mean bars
//   scale=quarter → 3 monthly-mean bars
// Each bucket carries coverage + confidence + (where a goal exists) achieved/target,
// so the client never aggregates or fakes a zero. Drill-down = re-call with a
// narrower scale+anchor; the leaf is /day/*.
import type { Context } from 'hono'

type Ctx = Context<{ Bindings: Bindings; Variables: { userId: string } }>
interface Bindings { DB: D1Database }

const DAY = 86400
const dayStartOf = (date: string) => Math.floor(Date.parse(date + 'T00:00:00Z') / 1000)
const dateOf = (ts: number) => new Date(ts * 1000).toISOString().slice(0, 10)

// Metric registry: where the value comes from + its goal semantics.
interface MetricDef {
  // pull a numeric value from a joined daily+sleep row (null = no data that day)
  pull: (r: DayRow) => number | null
  unit: string
  label: string
  // optional per-day target + whether higher is "good" (for achieved%)
  target?: (r: DayRow, needMin: number | null) => number | null
  goalDir?: 'min' | 'max'; // min = meet-or-exceed (sleep), else none
}

interface DayRow {
  date: string
  strain: number | null; recovery: number | null; resting_hr: number | null
  calories: number | null; steps: number | null; wear_min: number | null
  hrv_rmssd: number | null; resp_rate: number | null; stress: string | null
  duration_min: number | null // from sleep
  // Trends & Fitness additions
  readiness: number | null; vo2max: number | null
  fitness: number | null; fatigue: number | null; form: number | null; monotony: number | null
  hrv_sdnn: number | null; hrv_lfhf: number | null; hrv_si: number | null; hrv_cv: number | null
  nocturnal_dip_pct: number | null; acwr: number | null
  efficiency: number | null; deep_min: number | null; rem_min: number | null
  light_min: number | null; regularity: number | null // from sleep
}

const REGISTRY: Record<string, MetricDef> = {
  strain:      { pull: (r) => r.strain,      unit: '',     label: 'Strain' },
  recovery:    { pull: (r) => r.recovery,    unit: '%',    label: 'Recovery' },
  resting_hr:  { pull: (r) => r.resting_hr,  unit: 'bpm',  label: 'Resting HR' },
  hr:          { pull: (r) => r.resting_hr,  unit: 'bpm',  label: 'Resting HR' },
  hrv:         { pull: (r) => r.hrv_rmssd,   unit: 'ms',   label: 'HRV (RMSSD)' },
  calories:    { pull: (r) => r.calories,    unit: 'kcal', label: 'Active calories' },
  steps:       { pull: (r) => r.steps,       unit: '',     label: 'Steps' },
  wear:        { pull: (r) => r.wear_min,    unit: 'min',  label: 'Wear time' },
  // Trends & Fitness
  readiness:   { pull: (r) => r.readiness,   unit: '',     label: 'Readiness' },
  vo2max:      { pull: (r) => r.vo2max,      unit: 'ml/kg/min', label: 'VO₂max' },
  fitness:     { pull: (r) => r.fitness,     unit: '',     label: 'Fitness' },
  fatigue:     { pull: (r) => r.fatigue,     unit: '',     label: 'Fatigue' },
  form:        { pull: (r) => r.form,        unit: '',     label: 'Form' },
  monotony:    { pull: (r) => r.monotony,    unit: '',     label: 'Monotony' },
  acwr:        { pull: (r) => r.acwr,        unit: '',     label: 'Training load (ACWR)' },
  sdnn:        { pull: (r) => r.hrv_sdnn,    unit: 'ms',   label: 'HRV (SDNN)' },
  lf_hf:       { pull: (r) => r.hrv_lfhf,    unit: '',     label: 'LF / HF' },
  hrv_si:      { pull: (r) => r.hrv_si,      unit: '',     label: 'Stress index' },
  hrv_cv:      { pull: (r) => r.hrv_cv,      unit: '%',    label: 'HRV stability (CV)' },
  dip:         { pull: (r) => r.nocturnal_dip_pct == null ? null : r.nocturnal_dip_pct * 100, unit: '%', label: 'Nocturnal HR dip' },
  efficiency:  { pull: (r) => r.efficiency == null ? null : r.efficiency * 100, unit: '%', label: 'Sleep efficiency' },
  deep:        { pull: (r) => r.deep_min,    unit: 'min',  label: 'Deep sleep' },
  rem:         { pull: (r) => r.rem_min,     unit: 'min',  label: 'REM sleep' },
  light:       { pull: (r) => r.light_min,   unit: 'min',  label: 'Light sleep' },
  regularity:  { pull: (r) => r.regularity,  unit: '',     label: 'Sleep regularity (SRI)' },
  resp:        { pull: (r) => r.resp_rate,   unit: 'brpm', label: 'Respiratory rate' },
  sleep:       { pull: (r) => r.duration_min, unit: 'min', label: 'Sleep',
                 target: (_r, need) => need, goalDir: 'min' },
  stress:      { pull: (r) => { try { return r.stress ? (JSON.parse(r.stress).score ?? null) : null } catch { return null } },
                 unit: '', label: 'Stress' },
}

interface Bucket {
  label: string; t_start: number; t_end: number
  value: number | null; min: number | null; max: number | null
  n_days: number; coverage: number
  achieved: number | null; target: number | null; met: boolean | null
}

const mean = (xs: number[]) => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null
const round = (x: number | null, d = 1) => x == null ? null : Math.round(x * 10 ** d) / 10 ** d

export async function getTrend(c: Ctx) {
  const metric = c.req.param('metric') || ''
  const def = REGISTRY[metric]
  if (!def) return c.json({ error: `unknown metric '${metric}'`, known: Object.keys(REGISTRY) }, 400)
  const scale = (c.req.query('scale') || 'week') as 'week' | 'month' | 'quarter'
  const anchorStr = (c.req.query('anchor') || dateOf(Math.floor(Date.now() / 1000)))
  const anchor = dayStartOf(anchorStr)
  const userId = c.get('userId')

  // Window per scale.
  const spanDays = scale === 'week' ? 7 : scale === 'month' ? 35 : 91
  const from = anchor - (spanDays - 1) * DAY
  const to = anchor + DAY

  const { results } = await c.env.DB.prepare(
    'SELECT d.date AS date, d.strain, d.recovery, d.resting_hr, d.calories, d.steps, d.wear_min, ' +
    'd.hrv_rmssd, d.resp_rate, d.stress, d.readiness, d.vo2max, d.fitness, d.fatigue, d.form, ' +
    'd.monotony, d.hrv_sdnn, d.hrv_lfhf, d.hrv_si, d.hrv_cv, d.nocturnal_dip_pct, d.acwr, ' +
    's.duration_min AS duration_min, s.efficiency AS efficiency, s.deep_min AS deep_min, ' +
    's.rem_min AS rem_min, s.light_min AS light_min, s.regularity AS regularity ' +
    'FROM daily d LEFT JOIN sleep s ON s.user_id = d.user_id AND s.date = d.date ' +
    'WHERE d.user_id = ? AND d.date >= ? AND d.date <= ? ORDER BY d.date ASC',
  ).bind(userId, dateOf(from), dateOf(anchor)).all<DayRow>()
  const rows = results ?? []
  const byDate = new Map(rows.map((r) => [r.date, r]))

  const need = await c.env.DB.prepare('SELECT sleep_need_min FROM baselines WHERE user_id = ?')
    .bind(userId).first<{ sleep_need_min: number | null }>()
  const needMin = need?.sleep_need_min ?? null

  // Build buckets.
  const buckets: Bucket[] = []
  const pushBucket = (label: string, tStart: number, days: string[]) => {
    const vals: number[] = []
    let target: number | null = null
    let met = 0, metTotal = 0
    for (const ds of days) {
      const r = byDate.get(ds)
      if (!r) continue
      const v = def.pull(r)
      if (v == null) continue
      vals.push(v)
      if (def.target) {
        const t = def.target(r, needMin)
        if (t != null) {
          target = t; metTotal++
          if (def.goalDir === 'min' ? v >= t : v <= t) met++
        }
      }
    }
    const value = mean(vals)
    buckets.push({
      label, t_start: tStart, t_end: dayStartOf(days[days.length - 1]) + DAY,
      value: round(value), min: vals.length ? round(Math.min(...vals)) : null,
      max: vals.length ? round(Math.max(...vals)) : null,
      n_days: vals.length, coverage: round(vals.length / days.length, 2) ?? 0,
      achieved: def.target ? metTotal : null, target: round(target),
      met: def.target && metTotal ? met >= Math.ceil(metTotal / 2) : null,
    })
  }

  if (scale === 'week') {
    for (let i = 0; i < 7; i++) {
      const ts = from + i * DAY
      const ds = dateOf(ts)
      pushBucket(ds.slice(5), ts, [ds]) // MM-DD daily bar
    }
  } else if (scale === 'month') {
    for (let w = 0; w < 5; w++) {
      const ws = from + w * 7 * DAY
      if (ws > anchor) break
      const days = Array.from({ length: 7 }, (_, k) => dateOf(ws + k * DAY))
      pushBucket(`wk ${dateOf(ws).slice(5)}`, ws, days)
    }
  } else { // quarter → 3 monthly bars
    for (let m = 0; m < 3; m++) {
      // month relative to anchor month, going back
      const d = new Date(anchor * 1000)
      const mDate = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() - (2 - m), 1))
      const next = new Date(Date.UTC(mDate.getUTCFullYear(), mDate.getUTCMonth() + 1, 1))
      const days: string[] = []
      for (let t = Math.floor(mDate.getTime() / 1000); t < Math.floor(next.getTime() / 1000); t += DAY) days.push(dateOf(t))
      pushBucket(mDate.toISOString().slice(0, 7), Math.floor(mDate.getTime() / 1000), days)
    }
  }

  // Summary across the present buckets.
  const present = buckets.filter((b) => b.value != null)
  const vals = present.map((b) => b.value as number)
  const half = Math.floor(present.length / 2)
  const prev = mean(present.slice(0, half).map((b) => b.value as number))
  const recent = mean(present.slice(half).map((b) => b.value as number))
  const summary = {
    avg: round(mean(vals)),
    min: vals.length ? round(Math.min(...vals)) : null,
    max: vals.length ? round(Math.max(...vals)) : null,
    delta_vs_prev: (recent != null && prev != null) ? round(recent - prev) : null,
    met_count: def.target ? buckets.reduce((s, b) => s + (b.met ? 1 : 0), 0) : null,
    total: def.target ? buckets.filter((b) => b.met != null).length : null,
  }

  return c.json({ metric, label: def.label, unit: def.unit, scale, anchor: anchorStr, summary, buckets })
}
