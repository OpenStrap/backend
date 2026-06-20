// analytics.ts — derives daily / sleep / sessions / baselines from `minute`
// rollups using the openstrap-analytics pure functions. Shared by the queue
// consumer, the cron, and /admin/run-analytics.
//
// Confidence contract (docs/CONFIDENCE.md): every derived metric carries its
// computed {confidence, tier} and a per-metric `flags` JSON {c, tier, label}.
// Missing input → null + confidence 0; never fabricated.

import {
  calcRestingHR, calcStrain, calcHrZones, calcCalories, calcSleep, calcSleepPeriods,
  calcSleepRegularity, detectSessions, calcLoad, calcFitnessTrend,
  calcVo2Max, calcFitnessModel, calcMonotony,
  calcAnomaly, calcBaselines, buildCoach,
  calcSleepStress, calcNocturnalHeart, buildNotifications,
  type Minute, type Profile, type Baseline, type DayHistory,
  type DailyStrain, type NightSummary, type SleepValue, type Metric, type Driver,
} from 'openstrap-analytics'
import { readMinutes } from './minute_store'

const DAY = 86400

// ── small helpers ──────────────────────────────────────────────────────────
const dayKey = (ts: number): string => new Date(ts * 1000).toISOString().slice(0, 10)
const startOfDayUtc = (ts: number): number => Math.floor(ts / DAY) * DAY

interface MinuteRow {
  ts_min: number
  hr_avg: number | null
  hr_min: number | null
  hr_max: number | null
  hr_n: number | null
  activity: number | null
  steps: number | null
  wrist_on: number | null
}

const toMinute = (r: MinuteRow): Minute => ({
  ts: r.ts_min,
  hr_avg: r.hr_avg ?? 0,
  hr_min: r.hr_min ?? 0,
  hr_max: r.hr_max ?? 0,
  hr_n: r.hr_n ?? 0,
  activity: r.activity ?? 0,
  steps: r.steps ?? 0,
  wrist_on: !!r.wrist_on,
})

// Build a per-metric flag entry from any Metric<T>.
const flag = (m: { confidence: number; tier: string }, label: string) =>
  ({ c: Math.round(m.confidence * 1000) / 1000, tier: m.tier, label })

// ── profile / baseline loaders ───────────────────────────────────────────────
async function loadProfile(db: D1Database, userId: string): Promise<Profile> {
  const u = await db.prepare(
    'SELECT age, weight_kg, height_cm, sex FROM users WHERE id = ?',
  ).bind(userId).first<{ age: number | null; weight_kg: number | null; height_cm: number | null; sex: string | null }>()
  return {
    age: u?.age ?? undefined,
    weight_kg: u?.weight_kg ?? undefined,
    height_cm: u?.height_cm ?? undefined,
    sex: (u?.sex === 'm' || u?.sex === 'f') ? u.sex : undefined,
  }
}

type LoadedBaseline = Baseline & { sleeping_hr: number | null; resp_rate: number | null }

export async function loadBaseline(db: D1Database, userId: string): Promise<LoadedBaseline> {
  const b = await db.prepare(
    'SELECT resting_hr, max_hr, sleep_need_min, skin_temp, chronic_strain, sleeping_hr, resp_rate FROM baselines WHERE user_id = ?',
  ).bind(userId).first<any>()
  return {
    resting_hr: b?.resting_hr ?? 60,
    max_hr: b?.max_hr ?? 0,           // 0 → resolveMaxHr falls back to age/observed
    sleep_need_min: b?.sleep_need_min ?? 480,
    skin_temp: b?.skin_temp ?? undefined,
    chronic_strain: b?.chronic_strain ?? undefined,
    sleeping_hr: b?.sleeping_hr ?? null,
    resp_rate: b?.resp_rate ?? null,
  }
}

export async function loadMinutes(db: D1Database, userId: string, from: number, to: number): Promise<Minute[]> {
  // Day-packed store. processUser/wake windows are within HOT_DAYS → all in D1
  // minute_day (RAW_BUCKET omitted = no R2 fallback needed for hot reads).
  const recs = await readMinutes({ DB: db }, userId, from, to)
  return recs.map(toMinute)
}

// Sleep search window for the night that WAKES on `dateDayStart`:
// previous evening 18:00 → this day 12:00.
function sleepSearchWindow(dateDayStart: number): { from: number; to: number } {
  return { from: dateDayStart - 6 * 3600, to: dateDayStart + 12 * 3600 }
}

// Precompute the intra-day cumulative-strain curve + day HR stats so /day/strain
// is a PURE READ (no live recompute on the endpoint). Uses the SAME resolved max
// HR + sex coefficients calcStrain used, so the curve's final point equals the
// stored daily.strain. Curve is downsampled to keep the JSON small.
function dayStrainDetail(
  dayMin: Minute[], rhr: number, maxHr: number, sex?: 'm' | 'f',
): { curve: string; hrMax: number | null; hrMin: number | null; hrAvg: number | null } {
  const [k, b] = sex === 'f' ? [0.86, 1.67] : [0.64, 1.92]
  const denom = Math.max(1, maxHr - rhr)
  const scale = (trimp: number) => Math.min(21, Math.log(trimp + 1) / Math.log(1.5))
  const sorted = [...dayMin].sort((a, c) => a.ts - c.ts)
  let trimp = 0
  let hrMax = 0, hrMin = 0, hrSum = 0, hrN = 0
  const pts: { t: number; v: number }[] = []
  for (const m of sorted) {
    const hr = m.hr_avg
    if (m.wrist_on && hr > 0) {
      const ratio = Math.max(0, Math.min(1, (hr - rhr) / denom))
      trimp += ratio * k * Math.exp(b * ratio)
      hrMax = Math.max(hrMax, hr)
      hrMin = hrMin === 0 ? hr : Math.min(hrMin, hr)
      hrSum += hr; hrN++
    }
    pts.push({ t: m.ts, v: Math.round(scale(trimp) * 10) / 10 })
  }
  const MAXP = 120
  let curvePts = pts
  if (pts.length > MAXP) {
    const step = pts.length / MAXP
    curvePts = Array.from({ length: MAXP }, (_, i) => pts[Math.min(pts.length - 1, Math.floor(i * step))])
  }
  return {
    curve: JSON.stringify(curvePts),
    hrMax: hrMax || null,
    hrMin: hrMin || null,
    hrAvg: hrN ? Math.round(hrSum / hrN) : null,
  }
}

interface DayBuf {
  date: string
  dayStart: number
  idx: number                 // position in the per-day arrays (for trailing slices)
  rhr: Metric<{ resting_hr: number | null }>
  strain: ReturnType<typeof calcStrain>
  zones: ReturnType<typeof calcHrZones>
  calories: ReturnType<typeof calcCalories>
  sleep: Metric<SleepValue>
  wearMin: number
  daySteps: number                // SUM of this day's AN-2554 minute.steps
  sleepStress: string             // calcSleepStress JSON (nocturnal arousal)
  nocturnal: string               // calcNocturnalHeart JSON
  sleepingHr: number | null       // this night's sleeping-HR avg (for baseline)
  nocturnalElevated: boolean      // overnight HR notably above baseline
  mainDrivers: Record<string, Driver[]>  // strain/rhr/sleep/zones driver graph
  strainCurve: string             // precomputed cumulative-strain curve (JSON) for /day/strain
  hrMax: number | null
  hrMin: number | null
  hrAvg: number | null
}

/**
 * Derive everything for one user over the trailing window and upsert the
 * derived rows. Idempotent: re-running recomputes the same rows.
 *
 * `historyDays` controls how far back we recompute daily/sleep (default 3 for
 * the hot path; the nightly cron / seed passes a large value for full re-derive).
 */
export async function processUser(
  db: D1Database,
  userId: string,
  opts: { historyDays?: number; now?: number; dayFrom?: number; dayTo?: number } = {},
): Promise<{ daily: number; sleep: number; sessions: number }> {
  const now = opts.now ?? Math.floor(Date.now() / 1000)
  const historyDays = opts.historyDays ?? 3
  const todayStart = startOfDayUtc(now)
  // The full history spans [oldest .. today]. `dayFrom`/`dayTo` (offsets from
  // the oldest day) let callers process the window in chunks to stay under
  // Worker CPU limits; default = the whole window.
  const oldestDayStart = todayStart - (historyDays - 1) * DAY
  const dFrom = opts.dayFrom ?? 0
  const dTo = opts.dayTo ?? historyDays
  const firstDayStart = oldestDayStart + dFrom * DAY
  const lastDayStart = oldestDayStart + (dTo - 1) * DAY

  const profile = await loadProfile(db, userId)
  let baseline = await loadBaseline(db, userId)

  // HRV recovery (computed in biometrics.ts from RR) is read back here to drive
  // the coach. It may lag a run (biometrics runs after this on the cron) or be
  // null until the first nocturnal RR is processed — the coach degrades to a
  // neutral baseline when absent (it never fabricates a recovery number).
  const { results: recRows } = await db.prepare(
    'SELECT date, recovery FROM daily WHERE user_id = ? AND recovery IS NOT NULL ORDER BY date DESC LIMIT 60',
  ).bind(userId).all<{ date: string; recovery: number }>()
  const recoveryByDate = new Map((recRows ?? []).map((r) => [r.date, r.recovery]))

  // ── Pass 1: recompute baselines from prior derived history. ──
  // 90 days (not 30): baselines use the most-recent 30, but the trailing-window
  // metrics (ACWR/monotony/RHR-trend, and SRI from sleep) need a longer seed —
  // EWMA chronic (τ=28) wants ~3× that to converge. These are permanent derived
  // rows and D1 reads are effectively free at this scale, so the longer read is
  // ~$0. We NEVER re-decode old days (minute data is pruned at 10d); we just
  // reuse what was already computed. Forward-only, no backfill.
  const { results: histRows } = await db.prepare(
    'SELECT date, resting_hr, strain, hr_zones, hr_max FROM daily WHERE user_id = ? ORDER BY date DESC LIMIT 90',
  ).bind(userId).all<any>()
  const { results: sleepHistRows } = await db.prepare(
    'SELECT date, duration_min, onset_ts, wake_ts FROM sleep WHERE user_id = ? ORDER BY date DESC LIMIT 90',
  ).bind(userId).all<any>()
  const sleepByDate = new Map<string, number>()
  for (const s of sleepHistRows ?? []) if (s.duration_min != null) sleepByDate.set(s.date, s.duration_min)

  const history: DayHistory[] = (histRows ?? []).slice().reverse().map((d: any) => {
    let zone_min: [number, number, number, number, number] | undefined
    try {
      const z = d.hr_zones ? JSON.parse(d.hr_zones) : null
      if (z) zone_min = [z.zone1_min ?? 0, z.zone2_min ?? 0, z.zone3_min ?? 0, z.zone4_min ?? 0, z.zone5_min ?? 0]
    } catch { /* ignore */ }
    return {
      resting_hr: d.resting_hr ?? undefined,
      daily_strain: d.strain ?? undefined,
      sleep_duration_min: sleepByDate.get(d.date),
      // The day's observed HR peak. calcBaselines only trusts it as a measured
      // HRmax when it clears the age-predicted floor (a genuine effort); otherwise
      // it falls back to Tanaka. Without this, the measured-max path was dead and
      // everyone ran on the age formula. Forward-only, reuses the daily row.
      session_hr_max: d.hr_max ?? undefined,
      zone_min,
    } as DayHistory
  })

  if (history.length > 0) {
    // Baselines stay a 30-day metric even though we now fetch 90 for the windows.
    const bl = calcBaselines(history.slice(-30), profile)
    baseline = {
      resting_hr: bl.resting_hr ?? baseline.resting_hr,
      max_hr: bl.max_hr ?? baseline.max_hr,
      sleep_need_min: bl.sleep_need_min ?? baseline.sleep_need_min,
      skin_temp: bl.skin_temp ?? baseline.skin_temp,
      chronic_strain: bl.chronic_strain ?? baseline.chronic_strain,
      sleeping_hr: baseline.sleeping_hr,   // updated post-loop from this run's nights
      resp_rate: baseline.resp_rate,       // updated by the PPG resp job
    }
    await db.prepare(
      'INSERT INTO baselines (user_id, resting_hr, max_hr, sleep_need_min, skin_temp, chronic_strain, updated_at) ' +
      'VALUES (?,?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET ' +
      'resting_hr=excluded.resting_hr, max_hr=excluded.max_hr, sleep_need_min=excluded.sleep_need_min, ' +
      'skin_temp=excluded.skin_temp, chronic_strain=excluded.chronic_strain, updated_at=excluded.updated_at',
    ).bind(userId, baseline.resting_hr, baseline.max_hr || null, baseline.sleep_need_min,
      baseline.skin_temp ?? null, baseline.chronic_strain ?? null, now).run()
  }

  // ── Pass 2: per-day daily + sleep + sessions over the trailing window. ──
  const statements: D1PreparedStatement[] = []
  const dayBuffer: DayBuf[] = []
  let dailyN = 0, sleepN = 0, sessionN = 0
  // Per-day parallel arrays (one entry per processed day, in order) so Pass 3
  // can compute cross-day metrics on TRAILING windows ending at each day.
  const nightSummaries: NightSummary[] = []
  const dailyStrains: DailyStrain[] = []
  const rhrSeries: (number | null)[] = []  // per-day RHR (null when unmeasured)
  const hrrSeries: (number | null)[] = []  // per-day representative session HRR60

  // Pre-fetch the minute window for the chunk (covers its days + sleep
  // look-back) and bucket by day so per-day access is O(1), not O(n) scans.
  const wideMinutes = await loadMinutes(db, userId, firstDayStart - DAY, lastDayStart + DAY)
  const byDay = new Map<number, Minute[]>()
  for (const m of wideMinutes) {
    const k = startOfDayUtc(m.ts)
    const arr = byDay.get(k)
    if (arr) arr.push(m); else byDay.set(k, [m])
  }
  const dayMinutes = (dayStart: number) => byDay.get(dayStart) ?? []
  // Sleep window spans the previous evening → this noon; gather the two days.
  const sleepMinutes = (dayStart: number, from: number, to: number) => {
    const prev = byDay.get(dayStart - DAY) ?? []
    const cur = byDay.get(dayStart) ?? []
    const out: Minute[] = []
    for (const m of prev) if (m.ts >= from && m.ts < to) out.push(m)
    for (const m of cur) if (m.ts >= from && m.ts < to) out.push(m)
    return out
  }

  // ── Seed the trailing-window series from DURABLE derived history, so the
  //    cross-day metrics in Pass 3 (ACWR/monotony/RHR-trend/SRI) see their true
  //    28/14/7-day windows instead of just the ~3-day recompute window. We reuse
  //    the daily/sleep rows already fetched above (no extra query, no re-decode).
  //    Gaps INSIDE the active span are genuine rest days → strain 0; days before
  //    the user's earliest derived row are NOT fabricated (else a young account
  //    gets a tiny chronic and a bogus high ACWR).
  const SEED_DAYS = 84 // ≈3× the 28-day chronic time constant
  let seedLen = 0
  {
    const dayStrain = new Map<string, number>()
    const dayRhr = new Map<string, number>()
    for (const d of histRows ?? []) {
      if (d.strain != null) dayStrain.set(d.date, d.strain)
      if (d.resting_hr != null) dayRhr.set(d.date, d.resting_hr)
    }
    const dayWake = new Map<string, { onset: number | null; wake: number | null }>()
    for (const s of sleepHistRows ?? []) dayWake.set(s.date, { onset: s.onset_ts ?? null, wake: s.wake_ts ?? null })

    // Earliest derived day within the seed window — start the continuous series
    // there (don't 0-fill pre-account days).
    const windowStart = firstDayStart - SEED_DAYS * DAY
    let seedStart = firstDayStart
    for (let d = windowStart; d < firstDayStart; d += DAY) {
      const k = dayKey(d)
      if (dayStrain.has(k) || dayRhr.has(k) || dayWake.has(k)) { seedStart = d; break }
    }
    for (let d = seedStart; d < firstDayStart; d += DAY) {
      const k = dayKey(d)
      dailyStrains.push({ ts: d, strain: dayStrain.get(k) ?? 0 }) // intra-span gap = rest day
      rhrSeries.push(dayRhr.get(k) ?? null)
      hrrSeries.push(null) // HRR isn't stored per-day → fitness-trend HRR stays sparse
      const w = dayWake.get(k)
      nightSummaries.push({ onset_ts: w?.onset ?? null, wake_ts: w?.wake ?? null })
      seedLen++
    }
  }

  for (let dayStart = firstDayStart; dayStart <= lastDayStart; dayStart += DAY) {
    const date = dayKey(dayStart)
    const dayMin = dayMinutes(dayStart)

    // -- Sleep (search window spans prev-evening → this-noon). --
    const sw = sleepSearchWindow(dayStart)
    const sleepMin = sleepMinutes(dayStart, sw.from, sw.to)
    const sleep = calcSleep(sleepMin, baseline)
    nightSummaries.push({ onset_ts: sleep.onset_ts, wake_ts: sleep.wake_ts })
    // SRI for THIS night = regularity over a trailing ~2-week window ending here.
    // Needs ≥3 valid nights or it returns conf 0 → store null (matches §6).
    const nightIdx = nightSummaries.length - 1
    const sriNightsForSleep = nightSummaries.slice(Math.max(0, nightIdx - 13), nightIdx + 1)
    const sriForSleep = calcSleepRegularity(sriNightsForSleep)
    const regularityForSleep = sriForSleep.confidence > 0 ? sriForSleep.sri : null

    // -- Resting HR (within the night's window). --
    const rhr = calcRestingHR(dayMin, { onset_ts: sleep.onset_ts, wake_ts: sleep.wake_ts })
    rhrSeries.push(rhr.resting_hr)

    // -- Strain / zones / active-calories over the calendar day. --
    const strain = calcStrain(dayMin, baseline, profile)
    const zones = calcHrZones(dayMin, baseline, profile)
    const calories = calcCalories(dayMin, profile, baseline.resting_hr, zones.max_hr_used)
    dailyStrains.push({ ts: dayStart, strain: strain.score })
    // Precompute the strain curve + HR stats here (cron) so /day/strain just reads.
    const strainDetail = dayStrainDetail(dayMin, baseline.resting_hr, strain.max_hr_used, profile?.sex)

    // -- Sessions for this day. --
    const sessions = detectSessions(dayMin, baseline, profile)
    sessionN += sessions.length
    // Representative HRR60 for the day = best (largest) recovery among sessions.
    const dayHrr = sessions.reduce<number | null>((best, s) =>
      s.hrr60 != null && (best == null || s.hrr60 > best) ? s.hrr60 : best, null)
    hrrSeries.push(dayHrr)

    // Only clear AUTO-detected sessions — manually-started / live workouts are
    // user-owned and must survive a re-derive. Deleted tombstones are KEPT so a
    // user-deleted auto session isn't resurrected (its row's status stays
    // 'deleted'; the re-insert below only updates non-status fields via ON CONFLICT).
    statements.push(
      db.prepare("DELETE FROM sessions WHERE user_id = ? AND start_ts >= ? AND start_ts < ? AND (source IS NULL OR source = 'auto') AND status != 'deleted'")
        .bind(userId, dayStart, dayStart + DAY),
    )
    for (const s of sessions) {
      const sid = `${userId}:${s.start_ts}`
      statements.push(db.prepare(
        'INSERT INTO sessions (user_id, id, start_ts, end_ts, type, avg_hr, max_hr, strain, calories, hrr60, zones, confidence, status, source) ' +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'done','auto') ON CONFLICT(user_id, id) DO UPDATE SET " +
        'end_ts=excluded.end_ts, type=excluded.type, avg_hr=excluded.avg_hr, max_hr=excluded.max_hr, ' +
        'strain=excluded.strain, calories=excluded.calories, hrr60=excluded.hrr60, zones=excluded.zones, confidence=excluded.confidence',
      ).bind(userId, sid, s.start_ts, s.end_ts, s.type, Math.round(s.avg_hr), Math.round(s.max_hr),
        s.strain, s.kcal, s.hrr60 == null ? null : Math.round(s.hrr60), JSON.stringify(s.zones), s.confidence))
    }

    // -- Wear time (worn minutes). --
    const wearMin = dayMin.filter((m) => m.wrist_on).length
    // -- Steps = SUM of this day's per-minute AN-2554 counts (computed at ingest,
    //    stored in minute.steps). Folded into the daily row here at the close so past
    //    days have a permanent total after their minutes prune. NO R2, no recompute —
    //    steps_imu.ts is removed; AN-2554 is the only pedometer. Today's live total is
    //    served on-read by summing minute.steps (query.ts/daydetail.ts). --
    const daySteps = dayMin.reduce((a, m) => a + (m.steps ?? 0), 0)

    // -- Nocturnal heart (sleeping-HR dynamics over the main sleep period). --
    const sleepWorn = (sleep.onset_ts && sleep.wake_ts)
      ? sleepMin.filter((m) => m.ts >= sleep.onset_ts! && m.ts <= sleep.wake_ts!)
      : []
    const nocturnal = calcNocturnalHeart(sleepWorn, dayMin, baseline)

    // -- Sleep stress / nocturnal arousal (HR surges + motion in sleep; NOT HRV;
    //    the HRV-based daytime stress is computed in biometrics.ts from RR). --
    const sleepStress = calcSleepStress(sleepWorn, baseline)

    // -- Main-metric drivers (the cross-metric "what affected this" graph). --
    const mainDrivers: Record<string, Driver[]> = {
      strain: [
        { label: 'Time in higher HR zones', contribution: Math.round(zones.zone3_min + zones.zone4_min + zones.zone5_min), detail: `${zones.zone3_min + zones.zone4_min + zones.zone5_min} min in Z3+`, ref: { metric: 'zones', date, scale: 'day' } },
        { label: 'Workout sessions', contribution: 0, detail: 'auto-detected efforts', ref: { metric: 'sessions', date, scale: 'day' } },
      ],
      resting_hr: [
        { label: 'Sleeping heart rate', contribution: rhr.resting_hr ?? 0, detail: rhr.resting_hr != null ? `${rhr.resting_hr} bpm` : '—', ref: { metric: 'hr', date, scale: 'day' } },
      ],
      sleep: [
        { label: 'Time asleep', contribution: sleep.duration_min, detail: `${Math.round(sleep.duration_min)} min`, ref: { metric: 'sleep', date, scale: 'day' } },
        { label: 'Efficiency', contribution: Math.round(sleep.efficiency * 100), detail: `${Math.round(sleep.efficiency * 100)}%`, ref: { metric: 'sleep', date, scale: 'day' } },
      ],
    }

    const sleepFlags = JSON.stringify({
      duration: flag(sleep, 'Sleep'),
      stages: {
        c: sleep.stages_beta ? Math.round(sleep.confidence * 0.7 * 1000) / 1000 : 0,
        tier: 'ESTIMATE',
        label: 'Sleep stages (beta, estimated)',
      },
    })
    statements.push(db.prepare(
      'INSERT INTO sleep (user_id, date, onset_ts, wake_ts, duration_min, efficiency, light_min, deep_min, rem_min, regularity, confidence, flags, updated_at) ' +
      'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(user_id, date) DO UPDATE SET ' +
      'onset_ts=excluded.onset_ts, wake_ts=excluded.wake_ts, duration_min=excluded.duration_min, ' +
      'efficiency=excluded.efficiency, light_min=excluded.light_min, deep_min=excluded.deep_min, ' +
      'rem_min=excluded.rem_min, regularity=excluded.regularity, confidence=excluded.confidence, ' +
      'flags=excluded.flags, updated_at=excluded.updated_at',
    ).bind(userId, date, sleep.onset_ts, sleep.wake_ts, sleep.duration_min, sleep.efficiency,
      sleep.stages?.light_min ?? null, sleep.stages?.deep_min ?? null, sleep.stages?.rem_min ?? null,
      regularityForSleep, sleep.confidence, sleepFlags, now))
    sleepN++

    // -- Sleep v2 (multi-period; naps = shorter sleeps). ADDITIVE — the v1 write
    //    above is untouched. The window extends to THIS day's evening (18:00) so
    //    daytime naps are captured, without pulling in the NEXT night's onset
    //    (which belongs to date+1, same attribution convention as the night). --
    const periodsWin = sleepMinutes(dayStart, sw.from, dayStart + 18 * 3600)
    const sleepV2 = calcSleepPeriods(periodsWin, baseline)
    statements.push(
      db.prepare('DELETE FROM sleep_periods WHERE user_id = ? AND date = ?').bind(userId, date),
    )
    for (const p of sleepV2.periods) {
      const pid = `${userId}:${p.onset_ts}`
      statements.push(db.prepare(
        'INSERT INTO sleep_periods (user_id, id, date, onset_ts, wake_ts, duration_min, in_bed_min, efficiency, light_min, deep_min, rem_min, is_main, confidence, updated_at) ' +
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(user_id, id) DO UPDATE SET ' +
        'date=excluded.date, wake_ts=excluded.wake_ts, duration_min=excluded.duration_min, in_bed_min=excluded.in_bed_min, ' +
        'efficiency=excluded.efficiency, light_min=excluded.light_min, deep_min=excluded.deep_min, rem_min=excluded.rem_min, ' +
        'is_main=excluded.is_main, confidence=excluded.confidence, updated_at=excluded.updated_at',
      ).bind(userId, pid, date, p.onset_ts, p.wake_ts, p.duration_min, p.in_bed_min, p.efficiency,
        p.stages?.light_min ?? null, p.stages?.deep_min ?? null, p.stages?.rem_min ?? null,
        p.is_main ? 1 : 0, p.confidence, now))
    }

    dayBuffer.push({
      // idx points into the parallel arrays, which now START with `seedLen`
      // seeded history days — so trailing slices in Pass 3 reach into real history.
      date, dayStart, idx: seedLen + dayBuffer.length, rhr, strain, zones, calories, sleep, wearMin, daySteps,
      sleepStress: JSON.stringify(sleepStress),
      nocturnal: JSON.stringify(nocturnal),
      sleepingHr: nocturnal.sleeping_hr_avg,
      nocturnalElevated: nocturnal.elevated,
      mainDrivers,
      strainCurve: strainDetail.curve,
      hrMax: strainDetail.hrMax,
      hrMin: strainDetail.hrMin,
      hrAvg: strainDetail.hrAvg,
    })
  }

  // ── Pass 3: write daily rows, computing cross-day metrics PER DAY on
  //    trailing windows that end at that day (so ACWR/fitness/SRI/anomaly vary
  //    across the history instead of collapsing to a single value). ──
  for (const buf of dayBuffer) {
    const { date, idx, rhr, strain, zones, calories, sleep, wearMin, daySteps,
      sleepStress, nocturnal, nocturnalElevated, mainDrivers,
      strainCurve, hrMax, hrMin, hrAvg } = buf

    // Trailing windows ending at this day (inclusive).
    const sriNights = nightSummaries.slice(Math.max(0, idx - 13), idx + 1) // ~2 weeks
    const loadStrains = dailyStrains.slice(0, idx + 1)                      // calcLoad uses last 7/28 internally
    const fStart = Math.max(0, idx - 27)
    const fitnessHist = dailyStrains.slice(fStart, idx + 1)
      .map((d, i) => ({
        daily_strain: d.strain,
        resting_hr: rhrSeries[fStart + i] ?? undefined,
        hrr60: hrrSeries[fStart + i] ?? undefined,
      } as DayHistory))
    const recentRhr = rhrSeries.slice(Math.max(0, idx - 6), idx + 1)
      .filter((x): x is number => x != null)

    const sri = calcSleepRegularity(sriNights)
    const load = calcLoad(loadStrains)
    const fitness = calcFitnessTrend(fitnessHist)

    // Recovery is HRV-based, computed in biometrics.ts. Read the stored value for
    // this day (may be null until RR is processed) — the coach degrades gracefully.
    const recovery = recoveryByDate.get(date) ?? null

    // ── Fitness modeling (published, from data we already have; no recovery dep) ──
    // VO₂max (Uth–Sørensen) — only with a MEASURED max HR; abstains otherwise.
    const vo2 = calcVo2Max(baseline.max_hr && baseline.max_hr > 0 ? baseline.max_hr : null,
      rhr.resting_hr ?? baseline.resting_hr ?? null)
    // Banister fitness/fatigue/form + Foster monotony from the trailing strain.
    const fitModel = calcFitnessModel(loadStrains)
    const monotony = calcMonotony(loadStrains)
    // Nocturnal HR dip % (persisted as a column so it's trendable). Composite
    // Readiness + HRV CV/irregular are owned by biometrics.ts (HRV is fresh there).
    const nocDip = (() => { try { return JSON.parse(nocturnal)?.dip_pct ?? null } catch { return null } })()

    const anomaly = calcAnomaly({
      recent_rhr: recentRhr,
      skin_temp: baseline.skin_temp ?? null,
      sleep_efficiency: sleep.efficiency != null ? sleep.efficiency : null, // keep a real 0% (not coerced to "no data")
      baseline_sleep_efficiency: null,
    }, baseline)

    const flags = JSON.stringify({
      strain: flag(strain, 'Strain'),
      resting_hr: flag(rhr, 'Resting HR'),
      recovery: { c: recovery != null ? 0.8 : 0, tier: 'HIGH', label: 'Recovery (HRV)' },
      calories: flag(calories, calories.label),
      zones: flag(zones, zones.max_hr_source === 'age' ? 'HR zones (estimated max HR)' : 'HR zones'),
      steps: { c: wearMin > 0 ? 0.5 : 0, tier: 'ESTIMATE', label: 'Steps (est.)' },
      load: flag(load, `Load — ${load.band}`),
      fitness: flag(fitness, `Fitness trend — ${fitness.direction}`),
      anomaly: flag(anomaly, anomaly.note),
    })

    // Body alert = illness anomaly (RHR/temp/sleep) OR overtraining (high ACWR).
    // A signal, not a diagnosis — surfaced as a Today banner on the edge.
    const triggers: string[] = anomaly.signal ? [...anomaly.triggers] : []
    let alertNote = anomaly.signal ? anomaly.note : ''
    let kind = anomaly.signal ? 'strain' : ''
    if (load.band === 'high-risk') {
      triggers.push('high_training_load')
      kind = anomaly.signal ? 'both' : 'overtraining'
      alertNote = anomaly.signal
        ? `${alertNote} Training load is also high (ACWR ${load.acwr?.toFixed(2)}) — prioritize recovery.`
        : `Acute training load is well above your baseline (ACWR ${load.acwr?.toFixed(2)}). Elevated injury/overtraining risk — consider an easier day. A signal, not a diagnosis.`
    }
    // Elevated overnight HR vs your own baseline — an early illness / under-recovery
    // cue (the signal that made wearables famous for catching infections early).
    if (nocturnalElevated) {
      triggers.push('elevated_overnight_hr')
      if (!kind) kind = 'strain'
      const noc = (() => { try { return JSON.parse(nocturnal) } catch { return null } })()
      const bpm = noc?.vs_baseline_bpm
      const phrase = bpm != null
        ? `Your overnight heart rate ran ${bpm > 0 ? '+' : ''}${bpm} bpm above your baseline.`
        : 'Your overnight heart rate ran above your baseline.'
      alertNote = alertNote
        ? `${alertNote} ${phrase} Worth an easier day and extra rest.`
        : `${phrase} Often an early sign of fighting something off or under-recovery — consider an easier day. A signal, not a diagnosis.`
    }
    const bodyAlert = triggers.length > 0
      ? JSON.stringify({ signal: true, kind, triggers, note: alertNote })
      : null

    // ── Coaching engine (deterministic) — ranked plan + strain target +
    //    readiness contributors + narrative. Stored in daily.coach (JSON). ──
    const needFloored = baseline.sleep_need_min >= 180 ? baseline.sleep_need_min : 480
    let sleepDebt = 0
    for (const b of dayBuffer.slice(Math.max(0, idx - 6), idx + 1)) {
      const d = b.sleep.duration_min || 0
      if (d >= 120) sleepDebt += Math.max(0, needFloored - d)
    }
    const rhrRecent = rhrSeries.slice(0, idx + 1).filter((x): x is number => x != null)
    const coach = buildCoach({
      // HRV recovery drives the plan when present; absent → neutral 50 so the
      // coach still produces sleep/load guidance (it never surfaces a fake number).
      readiness: recovery ?? 50,
      readiness_components: null,
      resting_hr: rhr.resting_hr,
      baseline_rhr: baseline.resting_hr,
      rhr_recent: rhrRecent,
      strain_today: strain.score,
      acwr: load.acwr,
      sleep_last_min: sleep.duration_min || null,
      sleep_need_min: needFloored,
      sleep_debt_min: sleepDebt,
      sleep_efficiency: sleep.efficiency != null ? sleep.efficiency : null, // keep a real 0% (not coerced to "no data")
      sri: sri.sri,
      fitness_direction: fitness.direction,
      anomaly: bodyAlert ? JSON.parse(bodyAlert) : null,
    })

    const confs = [strain.confidence, rhr.confidence, calories.confidence, sleep.confidence]
    const confidence = Math.round((confs.reduce((s, v) => s + v, 0) / confs.length) * 1000) / 1000

    // Merge the main-metric drivers with any bio-drivers already stored by a
    // prior biometrics run (recovery/stress/illness) so we never clobber them.
    const driversJson = JSON.stringify(mainDrivers)

    // NOTE: `readiness`, `stress`, `recovery`, `illness`, `drivers.recovery|stress|illness`
    // are owned by biometrics.ts (HRV path) — processUser does NOT write them.
    // It writes `drivers` (main metrics) via COALESCE-free set but biometrics
    // read-merges, and on the hourly path (no biometrics) main drivers stand alone.
    statements.push(db.prepare(
      // `steps` = SUM of this day's AN-2554 minute counts (the only pedometer now).
      'INSERT INTO daily (user_id, date, strain, resting_hr, calories, wear_min, steps, hr_zones, acwr, fitness_trend, anomaly, coach, nocturnal, sleep_stress, drivers, vo2max, fitness, fatigue, form, monotony, nocturnal_dip_pct, strain_curve, hr_max, hr_min, hr_avg, confidence, flags, updated_at) ' +
      'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(user_id, date) DO UPDATE SET ' +
      'strain=excluded.strain, resting_hr=excluded.resting_hr, ' +
      'calories=excluded.calories, wear_min=excluded.wear_min, steps=excluded.steps, hr_zones=excluded.hr_zones, ' +
      'acwr=excluded.acwr, fitness_trend=excluded.fitness_trend, anomaly=excluded.anomaly, coach=excluded.coach, ' +
      'nocturnal=excluded.nocturnal, sleep_stress=excluded.sleep_stress, ' +
      'drivers=json_patch(COALESCE(daily.drivers,\'{}\'), excluded.drivers), ' +
      'vo2max=excluded.vo2max, fitness=excluded.fitness, fatigue=excluded.fatigue, form=excluded.form, ' +
      'monotony=excluded.monotony, nocturnal_dip_pct=excluded.nocturnal_dip_pct, ' +
      'strain_curve=excluded.strain_curve, hr_max=excluded.hr_max, hr_min=excluded.hr_min, hr_avg=excluded.hr_avg, ' +
      'confidence=excluded.confidence, flags=excluded.flags, updated_at=excluded.updated_at',
    ).bind(userId, date, strain.score, rhr.resting_hr == null ? null : Math.round(rhr.resting_hr),
      calories.kcal,
      wearMin, daySteps, JSON.stringify(zones), load.acwr, fitness.direction,
      bodyAlert, JSON.stringify(coach), nocturnal, sleepStress, driversJson,
      vo2.vo2max, fitModel.fitness, fitModel.fatigue, fitModel.form, monotony.monotony, nocDip,
      strainCurve, hrMax, hrMin, hrAvg,
      confidence, flags, now))
    dailyN++

    // ── Notifications (deterministic) — generate for the MOST RECENT day only
    //    (today). Idempotent by id=`${date}:${kind}`; preserves read state. ──
    if (idx === dayBuffer.length - 1) {
      const sleepStressObj = (() => { try { return JSON.parse(sleepStress) } catch { return null } })()
      const alertObj = bodyAlert ? (() => { try { return JSON.parse(bodyAlert) } catch { return null } })() : null
      const notifs = buildNotifications({
        date,
        readiness: recovery ?? 50,
        coach_summary: coach.summary ?? '',
        coach_top: coach.plan && coach.plan.length > 0
          ? { title: coach.plan[0].title, body: coach.plan[0].body } : null,
        body_alert: alertObj ? { kind: alertObj.kind, note: alertObj.note } : null,
        stress_score: sleepStressObj?.score ?? null,
        nocturnal_elevated: nocturnalElevated,
        sleep_debt_min: sleepDebt,
        acwr: load.acwr,
        strain_today: strain.score,
        strain_target_low: coach.strain_target?.low ?? null,
        strain_target_high: coach.strain_target?.high ?? null,
      })
      // Drop unread notifications for this date that no longer apply.
      const ids = notifs.map((n) => n.id)
      const placeholders = ids.map(() => '?').join(',')
      statements.push(
        ids.length > 0
          ? db.prepare(`DELETE FROM notifications WHERE user_id = ? AND date = ? AND read_at IS NULL AND id NOT IN (${placeholders})`)
              .bind(userId, date, ...ids)
          : db.prepare('DELETE FROM notifications WHERE user_id = ? AND date = ? AND read_at IS NULL')
              .bind(userId, date),
      )
      for (const n of notifs) {
        statements.push(db.prepare(
          'INSERT INTO notifications (user_id, id, date, kind, category, priority, title, body, window, quiet_ok, created_at, read_at) ' +
          'VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL) ON CONFLICT(user_id, id) DO UPDATE SET ' +
          'category=excluded.category, priority=excluded.priority, title=excluded.title, ' +
          'body=excluded.body, window=excluded.window, quiet_ok=excluded.quiet_ok',
        ).bind(userId, n.id, date, n.kind, n.category, n.priority, n.title, n.body, n.window,
          n.quiet_ok ? 1 : 0, now))
      }
    }
  }

  // ── Baseline: rolling sleeping-HR (median of this run's real nights, ≥3). ──
  //    Updated AFTER the per-day loop so today's "elevated overnight HR" compares
  //    against the prior baseline (no self-reference), stabilizing over runs.
  const sleepingHrs = dayBuffer
    .map((b) => b.sleepingHr)
    .filter((x): x is number => x != null && x > 0)
    .sort((a, b) => a - b)
  if (sleepingHrs.length >= 3) {
    const med = sleepingHrs[Math.floor(sleepingHrs.length / 2)]
    statements.push(db.prepare('UPDATE baselines SET sleeping_hr = ? WHERE user_id = ?')
      .bind(Math.round(med * 10) / 10, userId))
  }

  if (statements.length > 0) await db.batch(statements)

  await db.prepare(
    'INSERT INTO analytics_cursor (user_id, last_min_ts, dirty, last_run) VALUES (?,?,0,?) ' +
    'ON CONFLICT(user_id) DO UPDATE SET dirty=0, last_run=excluded.last_run, ' +
    'last_min_ts=MAX(analytics_cursor.last_min_ts, excluded.last_min_ts)',
  ).bind(userId, now, now).run()

  return { daily: dailyN, sleep: sleepN, sessions: sessionN }
}

// ── Legacy/cron entrypoint: process all dirty users (per-user isolation). ──
export async function runAnalytics(
  db: D1Database,
  opts: { historyDays?: number; now?: number } = {},
): Promise<{ processed: number; errors: number }> {
  const { results } = await db.prepare(
    'SELECT user_id FROM analytics_cursor WHERE dirty = 1',
  ).all<{ user_id: string }>()
  let processed = 0, errors = 0
  for (const r of results ?? []) {
    try {
      await processUser(db, r.user_id, opts)
      processed++
    } catch (e) {
      console.error('analytics failed for user', r.user_id, e)
      errors++
    }
  }
  return { processed, errors }
}
