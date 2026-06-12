// daydetail.ts — server-side drill-down detail for a single day. The client
// renders only; ALL computation here. Reads minute/sessions/events/daily/sleep
// and returns the structures the detail screens need.
//
//   GET /day/strain?date=YYYY-MM-DD    → cumulative strain curve + zones + HR stats + sessions
//   GET /day/sleep?date=YYYY-MM-DD     → hypnogram + stage breakdown + debt + consistency
//   GET /day/timeline?date=YYYY-MM-DD  → 24h HR + activity series + sleep block + sessions + events + highs/lows
// All JWT, scoped by user_id.

import type { Context } from 'hono'
import { classifyArousal } from 'openstrap-analytics'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

const DAY = 86400
const dayStartOf = (date: string) => Math.floor(Date.parse(`${date}T00:00:00Z`) / 1000)
const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v))

interface Min { ts_min: number; hr_avg: number | null; hr_min: number | null; hr_max: number | null; activity: number | null; wrist_on: number | null }

async function loadMinutes(c: Ctx, from: number, to: number): Promise<Min[]> {
  const { results } = await c.env.DB.prepare(
    'SELECT ts_min, hr_avg, hr_min, hr_max, activity, wrist_on FROM minute ' +
    'WHERE user_id = ? AND ts_min >= ? AND ts_min < ? ORDER BY ts_min ASC',
  ).bind(c.get('userId'), from, to).all<Min>()
  return results ?? []
}

async function loadHr(c: Ctx): Promise<{ rhr: number; maxHr: number }> {
  const b = await c.env.DB.prepare('SELECT resting_hr, max_hr FROM baselines WHERE user_id = ?')
    .bind(c.get('userId')).first<{ resting_hr: number | null; max_hr: number | null }>()
  const u = await c.env.DB.prepare('SELECT age FROM users WHERE id = ?')
    .bind(c.get('userId')).first<{ age: number | null }>()
  const rhr = b?.resting_hr && b.resting_hr > 0 ? b.resting_hr : 60
  const maxHr = (b?.max_hr && b.max_hr > 0) ? b.max_hr
    : (u?.age && u.age > 0 ? 220 - u.age : 190)
  return { rhr, maxHr }
}

// Downsample a {t,v} series to ≤ `cap` points by striding (keeps shape cheaply).
function downsample<T>(arr: T[], cap = 300): T[] {
  if (arr.length <= cap) return arr
  const step = Math.ceil(arr.length / cap)
  return arr.filter((_, i) => i % step === 0)
}

// ── /day/strain ──────────────────────────────────────────────────────────────
export async function getDayStrain(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const start = dayStartOf(date)
  const mins = await loadMinutes(c, start, start + DAY)
  const { rhr, maxHr } = await loadHr(c)
  const denom = Math.max(1, maxHr - rhr)

  let trimp = 0
  const curve: { t: number; v: number }[] = []
  const zones = [0, 0, 0, 0, 0]
  let hrMax = 0, hrMinNonZero = 0, hrSum = 0, hrN = 0
  for (const m of mins) {
    const hr = m.hr_avg ?? 0
    if (hr <= 0) { curve.push({ t: m.ts_min, v: round1(strainScale(trimp)) }); continue }
    const ratio = clamp((hr - rhr) / denom, 0, 1)
    trimp += ratio * 0.64 * Math.exp(1.92 * ratio)
    curve.push({ t: m.ts_min, v: round1(strainScale(trimp)) })
    const pct = (hr / maxHr) * 100
    if (pct >= 90) zones[4]++
    else if (pct >= 80) zones[3]++
    else if (pct >= 70) zones[2]++
    else if (pct >= 60) zones[1]++
    else if (pct >= 50) zones[0]++
    hrMax = Math.max(hrMax, hr)
    hrMinNonZero = hrMinNonZero === 0 ? hr : Math.min(hrMinNonZero, hr)
    hrSum += hr; hrN++
  }

  const { results: sessions } = await c.env.DB.prepare(
    'SELECT id, start_ts, end_ts, type, avg_hr, max_hr, strain, calories, hrr60, zones FROM sessions ' +
    'WHERE user_id = ? AND start_ts >= ? AND start_ts < ? ORDER BY start_ts ASC',
  ).bind(c.get('userId'), start, start + DAY).all<any>()

  return c.json({
    date,
    strain: round1(strainScale(trimp)),
    curve: downsample(curve),
    zones: { z1: zones[0], z2: zones[1], z3: zones[2], z4: zones[3], z5: zones[4] },
    hr: { max: hrMax || null, min: hrMinNonZero || null, avg: hrN ? Math.round(hrSum / hrN) : null },
    max_hr_used: maxHr,
    worn_min: mins.filter((m) => m.wrist_on).length,
    sessions: (sessions ?? []).map((s: any) => ({
      ...s, zones: s.zones ? safe(s.zones) : null,
      duration_min: s.end_ts && s.start_ts ? Math.round((s.end_ts - s.start_ts) / 60) : null,
    })),
  })
}

const strainScale = (trimp: number) => Math.min(21, Math.log(trimp + 1) / Math.log(1.5))
const round1 = (n: number) => Math.round(n * 10) / 10
const safe = (s: any) => { try { return JSON.parse(s) } catch { return null } }

// ── /day/sleep ───────────────────────────────────────────────────────────────
export async function getDaySleep(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const row = await c.env.DB.prepare('SELECT * FROM sleep WHERE user_id = ? AND date = ?')
    .bind(c.get('userId'), date).first<any>()
  const baseline = await c.env.DB.prepare('SELECT resting_hr, sleep_need_min FROM baselines WHERE user_id = ?')
    .bind(c.get('userId')).first<any>()
  const need = (baseline?.sleep_need_min && baseline.sleep_need_min >= 180) ? baseline.sleep_need_min : 480

  if (!row || !row.onset_ts || !row.wake_ts) {
    return c.json({ date, has_sleep: false, need_min: need })
  }
  const rhr = baseline?.resting_hr && baseline.resting_hr > 0 ? baseline.resting_hr : 55
  const mins = await loadMinutes(c, row.onset_ts, row.wake_ts + 60)

  // Per-epoch hypnogram (BETA, consistent with the summary's beta stages):
  //   off-wrist / HR > 1.15*RHR → awake; very low activity + low HR → deep;
  //   low activity + higher HR (REM-ish) → rem; else light.
  const hypnogram: { t: number; stage: string }[] = []
  for (const m of mins) {
    const hr = m.hr_avg ?? 0
    const act = m.activity ?? 0
    let stage: string
    if (hr <= 0 || hr > 1.15 * rhr || act > 0.25) stage = 'awake'
    else if (act < 0.01 && hr < rhr + 4) stage = 'deep'
    else if (hr > rhr + 5) stage = 'rem'
    else stage = 'light'
    hypnogram.push({ t: m.ts_min, stage })
  }

  // Sleep-debt over the last 7 real nights (incl. this one).
  const { results: recent } = await c.env.DB.prepare(
    'SELECT duration_min FROM sleep WHERE user_id = ? AND date <= ? ORDER BY date DESC LIMIT 7',
  ).bind(c.get('userId'), date).all<{ duration_min: number | null }>()
  let debt = 0
  for (const r of recent ?? []) {
    const d = r.duration_min ?? 0
    if (d >= 120) debt += Math.max(0, need - d)
  }

  // Nocturnal-heart summary + gated respiratory rate for this date (stored on daily).
  const dailyRow = await c.env.DB.prepare(
    'SELECT nocturnal, resp_rate, resp_conf FROM daily WHERE user_id = ? AND date = ?',
  ).bind(c.get('userId'), date).first<any>()
  const nocturnal = dailyRow?.nocturnal ? safe(dailyRow.nocturnal) : null
  const resp = (dailyRow?.resp_rate != null && (dailyRow?.resp_conf ?? 0) >= 0.5)
    ? { value: Math.round(dailyRow.resp_rate * 10) / 10, confidence: dailyRow.resp_conf }
    : null

  const inBed = Math.round((row.wake_ts - row.onset_ts) / 60)
  const asleep = row.duration_min ?? 0
  return c.json({
    date,
    has_sleep: true,
    nocturnal,
    resp,
    onset_ts: row.onset_ts,
    wake_ts: row.wake_ts,
    in_bed_min: inBed,
    duration_min: asleep,
    awake_min: Math.max(0, inBed - asleep),
    efficiency: row.efficiency,
    need_min: need,
    debt_min: Math.round(debt),
    regularity: row.regularity,
    stages: { light_min: row.light_min, deep_min: row.deep_min, rem_min: row.rem_min },
    stages_beta: true,
    hypnogram: downsample(hypnogram, 240),
  })
}

// ── /day/stress ──────────────────────────────────────────────────────────────
// Per-minute arousal band (calm/balanced/stressed/active) from HR-above-resting
// while sedentary. Mirrors calcStress exactly (same classifyArousal). NOT HRV.
export async function getDayStress(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const start = dayStartOf(date)
  const mins = await loadMinutes(c, start, start + DAY)
  const { rhr, maxHr } = await loadHr(c)

  const band: { t: number; b: string; s: number }[] = []
  let calm = 0, balanced = 0, stressed = 0, active = 0, worn = 0
  let reserveSum = 0, sedN = 0
  const sed: { ts: number; s: number; on: boolean }[] = []
  for (const m of mins) {
    const p = classifyArousal(m.hr_avg ?? 0, m.activity ?? 0, !!m.wrist_on, rhr, maxHr)
    band.push({ t: m.ts_min, b: p.bucket, s: p.score })
    if (p.bucket === 'none') { sed.push({ ts: m.ts_min, s: 0, on: false }); continue }
    worn++
    if (p.bucket === 'active') { active++; sed.push({ ts: m.ts_min, s: 0, on: false }); continue }
    reserveSum += p.reserve; sedN++
    if (p.bucket === 'calm') calm++
    else if (p.bucket === 'balanced') balanced++
    else stressed++
    sed.push({ ts: m.ts_min, s: p.score, on: true })
  }

  const score = sedN > 0 ? Math.round(clamp(reserveSum / sedN / 0.35, 0, 1) * 100) : null

  // Peak = highest 5-min rolling mean over sedentary minutes (≥3 in window).
  let peak: { t: number; score: number } | null = null
  const W = 5
  for (let i = 0; i + W <= sed.length; i++) {
    const win = sed.slice(i, i + W).filter((x) => x.on)
    if (win.length < 3) continue
    const mean = win.reduce((s, x) => s + x.s, 0) / win.length
    if (!peak || mean > peak.score) peak = { t: sed[i + 2].ts, score: Math.round(mean) }
  }

  return c.json({
    date,
    score,
    buckets: { calm, balanced, stressed, active },
    worn_min: worn,
    rhr_used: rhr,
    max_hr_used: maxHr,
    peak,
    band: downsample(band, 240),
  })
}

// ── /day/timeline ────────────────────────────────────────────────────────────
export async function getDayTimeline(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const start = dayStartOf(date)
  const end = start + DAY
  const mins = await loadMinutes(c, start, end)

  const hr: { t: number; v: number }[] = []
  const activity: { t: number; v: number }[] = []
  let peak = { t: 0, v: 0 }, low = { t: 0, v: 0 }
  for (const m of mins) {
    if (m.hr_avg && m.hr_avg > 0) {
      hr.push({ t: m.ts_min, v: m.hr_avg })
      if (m.hr_avg > peak.v) peak = { t: m.ts_min, v: m.hr_avg }
      if (low.v === 0 || m.hr_avg < low.v) low = { t: m.ts_min, v: m.hr_avg }
    }
    activity.push({ t: m.ts_min, v: Math.round((m.activity ?? 0) * 1000) / 1000 })
  }

  // Sleep blocks intersecting this day.
  const { results: sleeps } = await c.env.DB.prepare(
    'SELECT onset_ts, wake_ts, duration_min FROM sleep WHERE user_id = ? AND wake_ts >= ? AND onset_ts < ?',
  ).bind(c.get('userId'), start - DAY, end).all<any>()
  const { results: sessions } = await c.env.DB.prepare(
    'SELECT id, start_ts, end_ts, type, avg_hr, max_hr, strain FROM sessions WHERE user_id = ? AND start_ts >= ? AND start_ts < ? ORDER BY start_ts ASC',
  ).bind(c.get('userId'), start, end).all<any>()
  const { results: events } = await c.env.DB.prepare(
    'SELECT event_id, ts FROM events WHERE user_id = ? AND ts >= ? AND ts < ? ORDER BY ts ASC',
  ).bind(c.get('userId'), start, end).all<any>()

  return c.json({
    date,
    day_start: start,
    hr: downsample(hr),
    activity: downsample(activity),
    sleep: (sleeps ?? []).filter((s: any) => s.onset_ts && s.wake_ts),
    sessions: sessions ?? [],
    events: events ?? [],
    highs: {
      peak_hr: peak.v ? peak : null,
      low_hr: low.v ? low : null,
    },
  })
}
