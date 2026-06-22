// daydetail.ts — server-side drill-down detail for a single day. The client
// renders only; ALL computation here. Reads minute/sessions/events/daily/sleep
// and returns the structures the detail screens need.
//
//   GET /day/strain?date=YYYY-MM-DD    → cumulative strain curve + zones + HR stats + sessions
//   GET /day/sleep?date=YYYY-MM-DD     → hypnogram + stage breakdown + debt + consistency
//   GET /day/timeline?date=YYYY-MM-DD  → 24h HR + activity series + sleep block + sessions + events + highs/lows
// All JWT, scoped by user_id.

import type { Context } from 'hono'
import { cached, ttlForDate } from './cache'
import { readMinutes } from './minute_store'
import { ensureTodayWorkouts } from './workouts'
import { stageHypnogram, detectSleepCycles, calcDaytimeHrv } from 'openstrap-analytics'

type Ctx = Context<{ Bindings: { DB: D1Database; RAW_BUCKET?: R2Bucket }; Variables: { userId: string } }>

const DAY = 86400
const dayStartOf = (date: string) => Math.floor(Date.parse(`${date}T00:00:00Z`) / 1000)

// Per-minute hypnogram (BETA) via analytics `stageHypnogram` — the v1 method:
// calcSleep's Cole-Kripke + HR-dip mask owns asleep/awake, the SAME HR-percentile bands
// as estimateStages own deep/light/rem within sleep, bout-smoothed once. ONE source for
// both the graph and the totals (so they can't disagree), no second stager, no RR, no
// per-night knob — exactly what worked in v1.
interface NightStaging { hypnogram: { t: number; stage: string }[]; totals: { light_min: number; deep_min: number; rem_min: number } | null; awake_min?: number; asleep_min?: number }
function stageNight(
  mins: { ts_min: number; hr_avg?: number | null; hr_min?: number | null; hr_max?: number | null; activity?: number | null; wrist_on?: number | null; rr?: number[] | null }[],
  onset: number, wake: number, rhr: number,
): NightStaging {
  const ms = mins.map((m) => ({
    ts: m.ts_min, hr_avg: m.hr_avg ?? 0, hr_min: m.hr_min ?? 0, hr_max: m.hr_max ?? 0,
    hr_n: (m.hr_avg ?? 0) > 0 ? 60 : 0, activity: m.activity ?? 0, steps: 0, wrist_on: !!m.wrist_on,
  }))
  // Per-minute RR → the REM tiebreaker in stageHypnogram (high HR + low RMSSD = REM, not awake).
  const rrByMin = new Map<number, number[]>()
  for (const m of mins) if (m.rr && m.rr.length) rrByMin.set(m.ts_min, m.rr)
  const h = stageHypnogram(ms, onset, wake, { resting_hr: rhr, max_hr: 0, sleep_need_min: 480 }, rrByMin)
  if (h) {
    return {
      hypnogram: h.hypnogram.map((x) => ({ t: x.t, stage: x.stage })),
      totals: { light_min: h.light_min, deep_min: h.deep_min, rem_min: h.rem_min },
      awake_min: h.awake_min,
      asleep_min: h.asleep_min,
    }
  }
  // No resting-HR baseline → staging can't run; caller keeps the stored breakdown.
  return { hypnogram: [], totals: null }
}
const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v))

interface Min { ts_min: number; hr_avg: number | null; hr_min: number | null; hr_max: number | null; activity: number | null; wrist_on: number | null; rr: number[] }

async function loadMinutes(c: Ctx, from: number, to: number): Promise<Min[]> {
  // Tiered read: D1 for hot days, R2 fallback for sealed days (see minute_store).
  const rows = await readMinutes(c.env, c.get('userId'), from, to)
  return rows.map((m) => ({ ts_min: m.ts_min, hr_avg: m.hr_avg, hr_min: m.hr_min, hr_max: m.hr_max, activity: m.activity, wrist_on: m.wrist_on, rr: m.rr ?? [] }))
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
// PURE READ: serve the snapshot the analytics cron precomputed (daily row +
// sessions). No live recompute on the endpoint — strain, curve, zones and HR
// stats all come from `daily`, so /day/strain can never diverge from /today and
// the read is fast. Freshness is the cron's job (ingest → dirty → cron).
export async function getDayStrain(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const start = dayStartOf(date)
  const isToday = date === new Date().toISOString().slice(0, 10)
  // For today, refresh auto-detected sessions on read (throttled) so the day's
  // workout list isn't stale until the next wake-close.
  if (isToday) await ensureTodayWorkouts(c.env.DB, c.get('userId'))
  // Steps (AN-2554): live SUM of today's minute.steps; past days use the stored
  // daily.steps (persisted by processUser at close). Zero R2 — hot day blob.
  let liveSteps: number | null = null
  if (isToday) {
    const mins = await readMinutes(c.env, c.get('userId'), start, start + DAY)
    liveSteps = mins.reduce((a, m) => a + (m.steps ?? 0), 0)
  }

  const dr = await c.env.DB.prepare(
    'SELECT strain, hr_zones, wear_min, strain_curve, hr_max, hr_min, hr_avg, acwr, fitness_trend, ' +
    'calories, steps, drivers, vo2max, fitness, fatigue, form, monotony FROM daily WHERE user_id = ? AND date = ?',
  ).bind(c.get('userId'), date).first<any>()

  const z = dr?.hr_zones ? safe(dr.hr_zones) : null
  const acwr = dr?.acwr ?? null
  const band = acwr == null ? null
    : acwr < 0.8 ? 'detraining' : acwr <= 1.3 ? 'optimal' : acwr <= 1.5 ? 'caution' : 'high-risk'

  const { results: sessions } = await c.env.DB.prepare(
    'SELECT id, start_ts, end_ts, type, avg_hr, max_hr, strain, calories, hrr60, zones FROM sessions ' +
    "WHERE user_id = ? AND start_ts >= ? AND start_ts < ? AND status != 'deleted' ORDER BY start_ts ASC",
  ).bind(c.get('userId'), start, start + DAY).all<any>()

  return c.json({
    date,
    strain: dr?.strain ?? 0,
    curve: dr?.strain_curve ? safe(dr.strain_curve) : [],
    zones: z
      ? { z1: z.zone1_min ?? 0, z2: z.zone2_min ?? 0, z3: z.zone3_min ?? 0, z4: z.zone4_min ?? 0, z5: z.zone5_min ?? 0 }
      : { z1: 0, z2: 0, z3: 0, z4: 0, z5: 0 },
    hr: { max: dr?.hr_max ?? null, min: dr?.hr_min ?? null, avg: dr?.hr_avg ?? null },
    max_hr_used: z?.max_hr_used ?? null,
    worn_min: dr?.wear_min ?? 0,
    // Training load (ACWR band) + fitness trend + day energy/steps.
    load: acwr == null ? null : { acwr: Math.round(acwr * 100) / 100, band },
    fitness_trend: dr?.fitness_trend ?? null,
    // Banister fitness/fatigue/form + VO₂max + Foster monotony (the Body fitness model).
    vo2max: dr?.vo2max ?? null,
    fitness_model: (dr?.fitness != null || dr?.fatigue != null || dr?.form != null)
      ? { fitness: dr?.fitness ?? null, fatigue: dr?.fatigue ?? null, form: dr?.form ?? null } : null,
    monotony: dr?.monotony ?? null,
    calories: dr?.calories ?? null,
    steps: liveSteps ?? dr?.steps ?? null,
    drivers: dr?.drivers ? safe(dr.drivers) : null,
    sessions: (sessions ?? []).map((s: any) => ({
      ...s, zones: s.zones ? safe(s.zones) : null,
      duration_min: s.end_ts && s.start_ts ? Math.round((s.end_ts - s.start_ts) / 60) : null,
    })),
  })
}

const strainScale = (trimp: number) => Math.min(21, Math.log(trimp + 1) / Math.log(1.5))
const round1 = (n: number) => Math.round(n * 10) / 10
const safe = (s: any) => { try { return JSON.parse(s) } catch { return null } }

// ── /day/wear ──────────────────────────────────────────────────────────────
// How long the strap was actually on the wrist for a day, plus when. Built from
// the minute table's per-minute wrist_on flag (the source of truth for wear_min).
// Returns total worn minutes, coverage %, a 24-bin hourly coverage histogram,
// first-on / last-off timestamps, the number of separate wear stretches, and the
// longest off-wrist gap inside the worn window. UTC day, JWT-scoped.
export async function getDayWear(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const userId = c.get('userId')
  const payload = await cached(c.env.DB, userId, `daywear:${date}`, ttlForDate(date), async () => {
  const start = dayStartOf(date)
  const mins = await loadMinutes(c, start, start + DAY)

  // 24 hourly bins of minutes-worn (0..60). Index by the minute's hour-of-day.
  const hourly = Array.from({ length: 24 }, () => 0)
  let wornMin = 0
  let firstOn: number | null = null
  let lastOn: number | null = null
  let segments = 0
  let prevWorn = false
  // Gap tracking: longest run of not-worn minutes BETWEEN the first and last worn minute.
  let longestGap = 0
  for (const m of mins) {
    const worn = !!m.wrist_on
    const hour = Math.floor(((m.ts_min - start) % DAY) / 3600)
    if (worn) {
      wornMin++
      if (hour >= 0 && hour < 24) hourly[hour]++
      if (firstOn == null) firstOn = m.ts_min
      lastOn = m.ts_min
      if (!prevWorn) segments++
    }
    prevWorn = worn
  }
  // Longest off-wrist gap inside [firstOn, lastOn] (minutes the band wasn't on).
  if (firstOn != null && lastOn != null) {
    const wornSet = new Set(mins.filter((m) => m.wrist_on).map((m) => m.ts_min))
    let gap = 0
    for (let t = firstOn; t <= lastOn; t += 60) {
      if (wornSet.has(t)) { if (gap > longestGap) longestGap = gap; gap = 0 }
      else gap++
    }
    if (gap > longestGap) longestGap = gap
  }

  // Prefer the derived daily.wear_min when present (same source), else the live count.
  const dr = await c.env.DB.prepare(
    'SELECT wear_min FROM daily WHERE user_id = ? AND date = ?',
  ).bind(userId, date).first<{ wear_min: number | null }>()
  const wearMin = dr?.wear_min != null ? Math.round(dr.wear_min) : wornMin

  return {
    date,
    worn_min: wearMin,
    coverage_pct: Math.round((wearMin / 1440) * 100),
    hourly,                                   // [24] minutes worn per hour (0..60)
    first_on: firstOn,                        // unix sec of first worn minute (null = never on)
    last_on: lastOn,                          // unix sec of last worn minute
    segments,                                 // number of separate on-wrist stretches
    longest_off_min: longestGap,              // longest off-wrist gap inside the worn window
    tier: 'AUTH',                             // straight from the device's wrist sensor
  }
  })
  return c.json(payload)
}

// ── /day/sleep ───────────────────────────────────────────────────────────────
// GET /day/v2/sleep?date= — every sleep period for the date (one card each;
// naps = shorter sleeps). Additive companion to GET /day/sleep (single-period).
export async function getDaySleepV2(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const userId = c.get('userId')
  const payload = await cached(c.env.DB, userId, `daysleepv2:${date}`, ttlForDate(date), async () => {
    const { results } = await c.env.DB.prepare(
      'SELECT * FROM sleep_periods WHERE user_id = ? AND date = ? ORDER BY onset_ts ASC',
    ).bind(userId, date).all<any>()
    const baseRow = await c.env.DB.prepare('SELECT sleep_need_min, resting_hr FROM baselines WHERE user_id = ?')
      .bind(userId).first<any>()
    const need = (baseRow?.sleep_need_min && baseRow.sleep_need_min >= 180) ? baseRow.sleep_need_min : 480
    const rhr = baseRow?.resting_hr && baseRow.resting_hr > 0 ? baseRow.resting_hr : 55
    const rows = results ?? []
    // ONE minute read spanning all periods; slice per period for its own hypnogram so
    // every nap renders the same banded timeline as the main sleep (computed on-read;
    // no per-nap storage). Naps are already counted in total_asleep_min.
    let mins: Min[] = []
    const onsets = rows.map((r: any) => r.onset_ts).filter((x: any) => x)
    const wakes = rows.map((r: any) => r.wake_ts).filter((x: any) => x)
    if (onsets.length && wakes.length) {
      mins = await loadMinutes(c, Math.min(...onsets), Math.max(...wakes) + 60)
    }
    const periods = rows.map((r: any) => {
      const pm = mins.filter((m) => m.ts_min >= r.onset_ts && m.ts_min <= (r.wake_ts ?? r.onset_ts))
      const ng = pm.length ? stageNight(pm, r.onset_ts, r.wake_ts ?? r.onset_ts, rhr) : { hypnogram: [], totals: null }
      const storedStages = (r.light_min != null || r.deep_min != null || r.rem_min != null)
        ? { light_min: r.light_min, deep_min: r.deep_min, rem_min: r.rem_min } : null
      return {
        id: r.id,
        onset_ts: r.onset_ts,
        wake_ts: r.wake_ts,
        duration_min: r.duration_min,
        in_bed_min: r.in_bed_min,
        efficiency: r.efficiency,
        stages: ng.totals ?? storedStages, // same stageSleep source as the hypnogram → consistent
        is_main: !!r.is_main,
        confidence: r.confidence,
        hypnogram: downsample(ng.hypnogram, 240),
      }
    })
    return {
      date,
      has_sleep: periods.length > 0,
      need_min: need,
      total_asleep_min: periods.reduce((a: number, p: any) => a + (p.duration_min || 0), 0),
      naps: periods.filter((p: any) => !p.is_main).length,
      periods,
      stages_beta: true,
    }
  })
  return c.json(payload)
}

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
  const userId = c.get('userId')
  // Compute ONCE, then serve from the TTL read-cache (no per-read recompute): a past
  // day is immutable (cached until prune), "today" refreshes ≤60s, and close_day's
  // invalidateDay clears it so the next read after a close rebuilds it once.
  const payload = await cached(c.env.DB, userId, `daysleep:${date}`, ttlForDate(date), async () => {
    // WIDE window (prior evening → wake) so the staging window has full context.
    const mins = await loadMinutes(c, row.onset_ts - 16 * 3600, row.wake_ts + 3600)
    // ONE source (analytics stageHypnogram — v1 Cole-Kripke method) → hypnogram + totals.
    const ng = stageNight(mins, row.onset_ts, row.wake_ts, rhr)
    // Ultradian sleep cycles (Rosenblum 2024 fractal-cycle method, on smoothed z-RMSSD).
    const cyc = detectSleepCycles(mins.map((m) => ({ ts: m.ts_min, rr: m.rr })), row.onset_ts, row.wake_ts)

    // Sleep-debt over the last 7 real nights (incl. this one).
    const { results: recent } = await c.env.DB.prepare(
      'SELECT duration_min FROM sleep WHERE user_id = ? AND date <= ? ORDER BY date DESC LIMIT 7',
    ).bind(userId, date).all<{ duration_min: number | null }>()
    let debt = 0
    for (const r of recent ?? []) { const d = r.duration_min ?? 0; if (d >= 120) debt += Math.max(0, need - d) }

    // Nocturnal-heart summary + gated respiratory rate for this date (stored on daily).
    const dailyRow = await c.env.DB.prepare(
      'SELECT nocturnal, resp_rate, resp_conf FROM daily WHERE user_id = ? AND date = ?',
    ).bind(userId, date).first<any>()
    const nocturnal = dailyRow?.nocturnal ? safe(dailyRow.nocturnal) : null
    const resp = (dailyRow?.resp_rate != null && (dailyRow?.resp_conf ?? 0) >= 0.5)
      ? { value: Math.round(dailyRow.resp_rate * 10) / 10, confidence: dailyRow.resp_conf } : null

    const inBed = Math.round((row.wake_ts - row.onset_ts) / 60)
    // Single source: duration/awake/stages all come from the one hypnogram so the
    // breakdown and the graph can't disagree. Fall back to the stored row only if
    // staging abstained (no resting-HR baseline).
    const asleep = ng.asleep_min ?? row.duration_min ?? 0
    const awakeMin = ng.awake_min ?? Math.max(0, inBed - asleep)
    const inBedMin = ng.asleep_min != null ? asleep + awakeMin : inBed
    const efficiency = inBedMin > 0 && ng.asleep_min != null ? Math.round((asleep / inBedMin) * 10000) / 10000 : row.efficiency
    return {
      date,
      has_sleep: true,
      nocturnal,
      resp,
      onset_ts: row.onset_ts,
      wake_ts: row.wake_ts,
      in_bed_min: inBedMin,
      duration_min: asleep,
      awake_min: awakeMin,
      efficiency,
      need_min: need,
      debt_min: Math.round(debt),
      regularity: row.regularity,
      stages: ng.totals ?? { light_min: row.light_min, deep_min: row.deep_min, rem_min: row.rem_min },
      stages_beta: true,
      hypnogram: downsample(ng.hypnogram, 240),
      // Ultradian cycles (NREM↔REM), fractal-cycle method on HRV. Beta.
      cycles: cyc.cycles,
      cycles_mean_min: cyc.mean_duration_min,
      cycle_series: downsample(cyc.series, 240),
      cycles_beta: true,
    }
  })
  return c.json(payload)
}

// ── /day/stress ──────────────────────────────────────────────────────────────
// HRV stress for the day (Baevsky SI + LF/HF, computed in biometrics.ts from RR)
// + nocturnal arousal (sleep-stress), with a FACTUAL minute HR timeline for
// context. No heuristic arousal banding — stress is the HRV value, not HR-elevation.
export async function getDayStress(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const userId = c.get('userId') as string
  const payload = await cached(c.env.DB, userId, `daystress:${date}`, ttlForDate(date), async () => {
  const start = dayStartOf(date)
  const mins = await loadMinutes(c, start, start + DAY)

  const row = await c.env.DB.prepare(
    'SELECT stress, sleep_stress, drivers FROM daily WHERE user_id = ? AND date = ?',
  ).bind(userId, date).first<{ stress: string | null; sleep_stress: string | null; drivers: string | null }>()
  const parse = (s: string | null) => { try { return s ? JSON.parse(s) : null } catch { return null } }
  const stress = parse(row?.stress ?? null)
  const sleepStress = parse(row?.sleep_stress ?? null)
  const drivers = parse(row?.drivers ?? null)

  // Factual HR timeline (bpm) — context, not a stress band.
  const hr = mins.map((m) => ({ t: m.ts_min, v: m.hr_avg ?? 0 }))

  return {
    date,
    stress,         // {score, si, lf_hf, rmssd, level, drivers}
    sleep_stress: sleepStress, // {score, arousal_events, restless_min, events[...]}
    drivers: drivers?.stress ?? null,
    hr: downsample(hr, 240),
  }
  })
  return c.json(payload)
}

// ── /day/hrv ───────────────────────────────────────────────────────────────────
// Daytime (waking) HRV timeline — the ultradian RMSSD rhythm from minute.rr OUTSIDE
// the main sleep window. Complements nocturnal recovery: a daytime-stress curve.
export async function getDayHrv(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const userId = c.get('userId') as string
  const payload = await cached(c.env.DB, userId, `dayhrv:${date}`, ttlForDate(date), async () => {
    const start = dayStartOf(date)
    const mins = await loadMinutes(c, start, start + DAY)
    // Exclude this date's main sleep window so it's genuinely DAYTIME HRV.
    const sleep = await c.env.DB.prepare(
      'SELECT onset_ts, wake_ts FROM sleep WHERE user_id = ? AND date = ?',
    ).bind(userId, date).first<{ onset_ts: number | null; wake_ts: number | null }>()
    const inSleep = (t: number) => sleep?.onset_ts != null && sleep?.wake_ts != null && t >= sleep.onset_ts && t <= sleep.wake_ts
    const byMinute = mins
      .filter((m) => m.rr && m.rr.length && !inSleep(m.ts_min))
      .map((m) => ({ ts: m.ts_min, rr: m.rr as number[] }))
    const hrv = calcDaytimeHrv(byMinute)
    return { date, daytime_hrv: hrv } // {rmssd_median, series[{ts,rmssd}], lowest_ts, n_windows, confidence, tier}
  })
  return c.json(payload)
}

// ── /day/timeline ────────────────────────────────────────────────────────────
export async function getDayTimeline(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const userId = c.get('userId')
  const payload = await cached(c.env.DB, userId, `daytimeline:${date}`, ttlForDate(date), async () => {
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
    "SELECT id, start_ts, end_ts, type, avg_hr, max_hr, strain FROM sessions WHERE user_id = ? AND start_ts >= ? AND start_ts < ? AND status != 'deleted' ORDER BY start_ts ASC",
  ).bind(c.get('userId'), start, end).all<any>()
  const { results: events } = await c.env.DB.prepare(
    'SELECT event_id, ts FROM events WHERE user_id = ? AND ts >= ? AND ts < ? ORDER BY ts ASC',
  ).bind(userId, start, end).all<any>()

  return {
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
  }
  })
  return c.json(payload)
}

// ── /day/heart ─────────────────────────────────────────────────────────────
// Everything heart/autonomic for a day: 24h HR timeline, resting HR, HRV (RMSSD/
// SDNN/LF-HF), recovery, HR-zone minutes, nocturnal-HR dynamics, stress + illness.
// Recovery/stress/illness/HRV are read from the daily row (computed in biometrics).
export async function getDayHeart(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const userId = c.get('userId')
  const payload = await cached(c.env.DB, userId, `dayheart:${date}`, ttlForDate(date), async () => {
  const start = dayStartOf(date)
  const mins = await loadMinutes(c, start, start + DAY)
  const d = await c.env.DB.prepare(
    'SELECT resting_hr, recovery, readiness, hrv_rmssd, hrv_sdnn, hrv_lfhf, hrv_conf, hrv_cv, irregular, hr_zones, nocturnal, stress, illness, drivers, resp_rate, resp_conf, spo2_idx, skin_temp_idx FROM daily WHERE user_id = ? AND date = ?',
  ).bind(userId, date).first<any>()
  const base = await c.env.DB.prepare('SELECT resting_hr, hrv_rmssd FROM baselines WHERE user_id = ?')
    .bind(userId).first<any>()
  const parse = (s: string | null) => { try { return s ? JSON.parse(s) : null } catch { return null } }
  const worn = mins.filter((m) => m.wrist_on && (m.hr_avg ?? 0) > 0)
  const hrs = worn.map((m) => m.hr_avg as number)
  return {
    date,
    hr: downsample(mins.map((m) => ({ t: m.ts_min, v: m.hr_avg ?? 0 })), 240),
    avg_hr: hrs.length ? Math.round(hrs.reduce((a, b) => a + b, 0) / hrs.length) : null,
    max_hr: hrs.length ? Math.round(Math.max(...hrs)) : null,
    resting_hr: d?.resting_hr ?? null,
    resting_hr_baseline: base?.resting_hr ?? null,
    recovery: d?.recovery ?? null,
    readiness: d?.readiness ?? null,
    hrv: d?.hrv_rmssd != null
      ? { rmssd: d.hrv_rmssd, sdnn: d.hrv_sdnn, lf_hf: d.hrv_lfhf, cv: d.hrv_cv ?? null, confidence: d.hrv_conf, baseline: base?.hrv_rmssd ?? null }
      : null,
    zones: d?.hr_zones ? parse(d.hr_zones) : null,
    nocturnal: parse(d?.nocturnal ?? null),
    stress: parse(d?.stress ?? null),
    illness: parse(d?.illness ?? null),
    irregular: parse(d?.irregular ?? null),
    // Respiratory rate (RSA, gated) + relative SpO₂ now live under Heart.
    resp: (d?.resp_rate != null && (d?.resp_conf ?? 0) >= 0.3)
      ? { value: d.resp_rate, confidence: d.resp_conf } : null,
    spo2: d?.spo2_idx != null ? { value: d.spo2_idx } : null,
    skin_temp: d?.skin_temp_idx != null ? { value: d.skin_temp_idx } : null,
    drivers: parse(d?.drivers ?? null),
  }
  })
  return c.json(payload)
}

// ── /day/lungs ─────────────────────────────────────────────────────────────
// Respiratory rate (RSA from RR, gated on confidence) + relative SpO₂. Honest:
// resp is null on nights without enough clean RR; SpO₂ is a baseline deviation,
// never an absolute %.
export async function getDayLungs(c: Ctx) {
  const date = (c.req.query('date') || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  const userId = c.get('userId')
  const d = await c.env.DB.prepare(
    'SELECT resp_rate, resp_conf, spo2_idx, drivers FROM daily WHERE user_id = ? AND date = ?',
  ).bind(userId, date).first<any>()
  const parse = (s: string | null) => { try { return s ? JSON.parse(s) : null } catch { return null } }
  const respShown = d?.resp_rate != null && (d?.resp_conf ?? 0) >= 0.3
  return c.json({
    date,
    resp_rate: respShown ? { value: d.resp_rate, confidence: d.resp_conf, unit: 'brpm', tier: 'ESTIMATE', label: 'Respiratory rate (RSA)' } : null,
    spo2: d?.spo2_idx != null ? { value: d.spo2_idx, unit: 'Δ', tier: 'RELATIVE', label: 'Blood-oxygen vs baseline' } : null,
    drivers: parse(d?.drivers ?? null),
  })
}
