// seed.ts — /admin/seed-demo. Generates ~90 days of realistic `minute` rollups
// directly (no raw frames) shaped so EVERY analytics feature is populated, then
// runs processUser over the full history. Idempotent (clears prior seeded rows).
//
// PHASED because Cloudflare's free plan caps subrequests per request (~50) and
// each per-day D1 batch() is one subrequest. The route drives phases:
//   phase=init      → create/clear user, return {user_id, days}
//   phase=minutes   → write days [day_from, day_to) (one batch per day)
//   phase=analytics → run processUser over the full history (one big batch)
// A bare call with no phase runs init+analytics and is safe; the shell
// orchestrates the minutes phases in chunks. Each phase is idempotent.

import { processUser } from './analytics'
import { uuid } from './auth'

const DAY = 86400
const MIN = 60

// Deterministic-ish PRNG so re-seeds look stable (seeded by day index).
function rng(seed: number) {
  let s = seed >>> 0
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0
    return s / 0xffffffff
  }
}
const noise = (r: () => number, amp: number) => (r() - 0.5) * 2 * amp

interface MinuteOut {
  ts_min: number
  hr_avg: number; hr_min: number; hr_max: number; hr_n: number
  activity: number; steps: number; wrist_on: number
}

// Build one day of per-minute rollups for day index `d` (0 = oldest).
function buildDay(dayStart: number, d: number, days: number): MinuteOut[] {
  const r = rng(1000 + d)
  const out: MinuteOut[] = []

  // Fitness trend: RHR drifts DOWN ~3 bpm over the window; daytime HR eases too.
  // Kept modest (51→48) so the sleep floor never sits far above the adaptive
  // global baseline RHR — a large gap would push REM epochs past the awake
  // threshold (1.15*RHR) and fragment early-window nights.
  const progress = d / Math.max(1, days - 1)       // 0..1
  const rhrBase = 51 - 3 * progress                // 51 → 48
  const dayHrBase = 72 - 4 * progress

  // Illness/anomaly window: the MOST RECENT 4 days (days-4..days-1, i.e. today
  // and the three nights before). calcAnomaly (Rule A) fires on day D when its
  // trailing recent_rhr has ≥2 consecutive elevated days ending at D. So a
  // 4-consecutive-day elevated block makes the LAST THREE daily rows (days-3,
  // days-2, days-1=today) each individually fire → /today + /trends (which read
  // today's row) show anomaly.signal=true, and /strain shows 3 recent signaling
  // days. Placing it on the most recent days makes Readiness visibly dip too.
  const illness = d >= days - 4 && d <= days - 1
  // Sleep-HR illness bump. The anomaly detector reads each day's RHR as the
  // 5th-PERCENTILE of that night's SLEEP-window HR (calcRestingHR), i.e. the
  // night's deep-sleep floor. The adaptive baseline RHR (median of last 30 days)
  // sits ≈ the healthy floor (~49–50 bpm). calcAnomaly's threshold is
  // baseline*1.07 (≈53). To clear it with honest margin we lift the illness
  // night's deep floor ~6 bpm above the healthy floor so the 5th-pctile lands
  // ~53–55 bpm = roughly +8–12% over baseline. Upper bound is the SLEEP module's
  // awake override (hr > 1.15*RHR ≈ 56.4): deep at floor+1 (~55) stays under it,
  // so the night doesn't fragment. Non-deep epochs are kept in a tight band just
  // above (see remDelta/light below) so they also stay under ~56.4.
  const sleepIll = illness ? 6 : 0
  // Sleep HR is anchored on the HEALTHY floor (rhrBase), NOT the inflated daytime
  // rhrToday — otherwise illness would double-count (rhrToday already +10%) and
  // push every sleep epoch over the awake threshold, fragmenting the night.
  const sleepFloor = rhrBase + sleepIll

  // Sleep window: onset ~22:30 (varied), wake ~06:30 (varied) → SRI < 100.
  const onsetMin = 22 * 60 + 30 + Math.round(noise(r, 45))
  const wakeMin = 6 * 60 + 30 + Math.round(noise(r, 40))

  // Ultradian sleep structure: ~90-min cycles alternating deep → light → REM,
  // plus a few brief interior awakenings, so calcSleep distributes stages and
  // efficiency lands ~88–96% (interior awake epochs reduce it). The night spans
  // the late-evening block [onsetMin..1440) and the early-morning block
  // [0..wakeMin]. We give every sleep minute a "minutes since sleep onset" so the
  // cycle phase is continuous across midnight.
  const CYCLE = 90
  // Pre-pick 2–4 brief interior awakenings (start + fixed duration) at random
  // minutes-into-sleep. Total awake ≈ 12–30 min over a ~7–8h night → efficiency
  // mostly lands in the 0.85–0.97 band.
  const totalSleepMin = (1440 - onsetMin) + wakeMin
  // Illness nights are more fragmented (worse efficiency is the whole point of
  // the anomaly demo): 5–7 interior awakenings vs the healthy 2–4.
  const nWakes = illness ? 5 + Math.floor(r() * 3) : 2 + Math.floor(r() * 3) // 5..7 ill, else 2..4
  const wakes: { at: number; dur: number }[] = []
  for (let i = 0; i < nWakes; i++) {
    // place each awakening between cycles (avoid the very start/end)
    const at = 60 + Math.floor(r() * Math.max(1, totalSleepMin - 120))
    // Illness awakenings run longer (8–15 min, still ≤ the 20-min consolidation
    // gap so the night stays one period) → lower efficiency on illness nights.
    const dur = illness ? 8 + Math.floor(r() * 8) : 4 + Math.floor(r() * 6)
    wakes.push({ at, dur })
  }
  // minutes-into-sleep for a given minute-of-day m within the night.
  const sleepPhase = (m: number): number => (m >= onsetMin ? m - onsetMin : (1440 - onsetMin) + m)
  // classify a sleep minute → 'deep' | 'light' | 'rem' | 'wake'
  const sleepStageAt = (m: number): 'deep' | 'light' | 'rem' | 'wake' => {
    const sp = sleepPhase(m)
    for (const w of wakes) if (sp >= w.at && sp < w.at + w.dur) return 'wake'
    const inCycle = sp % CYCLE
    // Early portion of each cycle = deep, middle = light, late = REM. Deep
    // dominates EARLY cycles and shrinks later; REM grows toward morning (so the
    // night-wide split lands ~deep 13–23%, REM 18–28%, rest light).
    const cycleNo = Math.floor(sp / CYCLE)
    const deepEnd = Math.max(5, 17 - cycleNo * 5)   // 17,12,7,5,… → less deep later
    const remStart = Math.max(40, 54 - cycleNo * 6) // 54,48,42,40,… → more REM later
    if (inCycle < deepEnd) return 'deep'
    if (inCycle >= remStart) return 'rem'
    return 'light'
  }

  // Workouts 3-5/week with varied type, plus load-band stretches that make daily
  // ACWR (= mean 7d strain / mean 28d strain) span the full set of bands:
  //   detraining dip (ACWR <0.8):  days 45-57 → ~1 short workout/week.
  //   LONG taper before overreach: days 58-75 → fewer/shorter workouts. A long
  //     low stretch immediately before the overreach drives the 28-day CHRONIC
  //     baseline down, so at the overreach PEAK the acute/chronic ratio clears
  //     caution (1.3) and reaches high-risk (>1.5). Strain is log-compressed, so
  //     a low chronic denominator — not raw intensity — is the dominant ACWR
  //     lever (intensity alone capped at ~1.29 before).
  //   SHORT, INTENSE overreach (ACWR caution→high-risk): days 76-82 → daily
  //     triple cardio blocks + elevated all-day HR. Kept to 7 days so that at the
  //     peak day the 7-day acute window is ALL overreach while the 28-day chronic
  //     window is dominated by the long taper (only the 7 overreach days are
  //     high) → the ratio peaks well above 1.5.
  const dow = d % 7
  const detraining = d >= 45 && d <= 57
  const taper = d >= 58 && d <= 75
  const overreach = d >= 76 && d <= 82
  let doWorkout = dow === 1 || dow === 3 || dow === 5 || (dow === 6 && r() > 0.4)
  if (detraining) doWorkout = dow === 3                  // ~1/week
  if (taper) doWorkout = false                           // rest fortnight: no workouts
  if (overreach) doWorkout = dow !== 0                   // ~6/week
  const woStart = 17 * 60 + 30 + Math.round(noise(r, 30))
  let woDur = 25 + Math.round(r() * 35)
  if (overreach) woDur = 60 + Math.round(r() * 35)
  if (taper) woDur = 18 + Math.round(r() * 10)
  if (detraining) woDur = 20 + Math.round(r() * 10)
  const woKind = d % 3 === 0 ? 'walk' : d % 3 === 1 ? 'run' : 'strength'

  for (let m = 0; m < 1440; m++) {
    const ts_min = dayStart + m * MIN
    const isSleep = m <= wakeMin || m >= onsetMin
    const charging = m >= 720 && m < 720 + (d % 5 === 0 ? 25 : 6)
    let wrist = charging ? 0 : 1

    let hr = 0, activity = 0, steps = 0
    if (wrist) {
      if (isSleep) {
        // Ultradian stage shapes HR + activity so calcSleep's heuristic (deep =
        // lowest HR + ~0 activity; REM = a few bpm higher + variable; light =
        // between) yields a realistic deep/REM/light split. The RHR floor stays
        // recoverable as the 5th-pctile sleep HR: DEEP minutes sit at the floor
        // (sleepFloor + ~1), so the night's 5th percentile ≈ the floor. Sleep HR
        // stays well under the awake threshold (hr > 1.1*RHR → awake) so the
        // contiguous asleep run doesn't fragment. Illness nudges the floor up a
        // little (via sleepFloor) so the anomaly detector still sees elevated RHR.
        const stage = sleepStageAt(m)
        // During illness COMPRESS the ultradian HR spread (fever → elevated but
        // monotonous sleep HR): the whole night sits in a tight band just above
        // the floor so it stays under the awake threshold (1.1*RHR) and doesn't
        // fragment, while the 5th-pctile (deep) still reads as elevated RHR.
        // Illness spread is tight: deep≈floor+1, REM/light only +1 over the floor,
        // so the whole asleep night sits ~54–55 bpm — above baseline*1.07 (the
        // anomaly RHR trigger, since the 5th-pctile reads the deep floor) yet
        // below the sleep module's awake override (1.15*RHR ≈ 56.4) so the night
        // does NOT fragment. Interior awakenings (wakeDelta) deliberately clear
        // 56.4 so they read as awake and cut efficiency.
        const remDelta = illness ? 1 : 6
        const wakeDelta = illness ? 8 : 14
        const remVar = illness ? 0.6 : 3
        if (stage === 'deep') {
          // lowest HR (at the floor), essentially no motion, very stable.
          hr = Math.round(sleepFloor + 1 + noise(r, 0.5))
          activity = Math.max(0, 0.004 + Math.abs(noise(r, 0.003)))
        } else if (stage === 'rem') {
          // a few bpm above the floor + MORE variable, activity very low.
          hr = Math.round(sleepFloor + remDelta + noise(r, remVar))
          activity = Math.max(0, 0.012 + Math.abs(noise(r, 0.01)))
        } else if (stage === 'wake') {
          // brief interior awakening: higher activity + HR → counts against eff.
          hr = Math.round(sleepFloor + wakeDelta + noise(r, 3))
          activity = Math.max(0, 0.25 + Math.abs(noise(r, 0.1)))
        } else {
          // light: between deep and REM, mild variance.
          hr = Math.round(sleepFloor + (illness ? 1 : 3) + noise(r, illness ? 0.5 : 1.2))
          activity = Math.max(0, 0.008 + Math.abs(noise(r, 0.006)))
        }
        if (illness && stage !== 'wake' && r() > 0.9) { activity += 0.1 } // extra restlessness (eff only, not HR)
      } else {
        const hourFrac = m / 60
        const circ = Math.sin((hourFrac - 6) / 24 * 2 * Math.PI) * 4
        // Daytime HR must stay UNAMBIGUOUSLY above calcSleep's awake override
        // (hr > 1.15*RHR). With RHR ~48–51 that threshold is ~55–59, and the old
        // floor (dayHrBase 68 + circ −4 + noise −6 ≈ 58) skimmed it — letting the
        // occasional daytime minute read asleep and bridge into the night. Clamp
        // a floor of 64 (≈1.25*RHR even at the high end) so NO worn daytime
        // minute reads asleep. Does NOT touch the sleep HR floor (sleepFloor),
        // so RHR = 5th-pctile sleep HR is unchanged.
        hr = Math.max(64, Math.round(dayHrBase + circ + noise(r, 6)))
        activity = Math.max(0, 0.05 + Math.abs(noise(r, 0.06)))
        steps = Math.round(Math.max(0, 6 + noise(r, 6)))
      }

      // Overreach: TWO–THREE long cardio blocks + elevated all-day HR so daily
      // TRIMP (hence acute 7d load) climbs well above the tapered chronic 28d
      // baseline → ACWR clears caution (1.3) and reaches high-risk (>1.5).
      const inWo1 = doWorkout && m >= woStart && m < woStart + woDur
      const wo2Start = 7 * 60        // morning block during overreach
      const inWo2 = overreach && m >= wo2Start && m < wo2Start + 70
      const wo3Start = 13 * 60       // early-afternoon block during overreach
      const inWo3 = overreach && m >= wo3Start && m < wo3Start + 50
      if (inWo1 || inWo2 || inWo3) {
        const s = inWo1 ? woStart : inWo2 ? wo2Start : wo3Start
        const dur = inWo1 ? woDur : inWo2 ? 70 : 50
        const into = (m - s) / dur
        // Overreach blocks run hot (sustained high HR-reserve) regardless of the
        // day's nominal woKind, to drive a big TRIMP gap over the taper.
        const peak = overreach ? 170 : woKind === 'run' ? 172 : woKind === 'walk' ? 132 : 158
        hr = Math.round(peak - 18 * Math.abs(into - 0.6) * 2 + noise(r, 5))
        // Strength: HIGH HR but LOW mean motion (isometric holds + rest between
        // sets) so detectSessions' classifier reads !highActivity → 'strength'.
        // Walk: low-moderate motion + sub-60% reserve → 'walk'. Run: high both.
        activity = woKind === 'walk' ? 0.35 + Math.abs(noise(r, 0.1))
          : woKind === 'run' ? 0.9 + Math.abs(noise(r, 0.2))
          : 0.07 + Math.abs(noise(r, 0.015))  // strength: motion above day-median but < 2x (→ 'strength')
        steps = woKind === 'walk' ? 110 + Math.round(noise(r, 20))
          : woKind === 'run' ? 165 + Math.round(noise(r, 25))
          : 20 + Math.round(noise(r, 15))
      } else if (overreach && !isSleep) {
        // Elevated active recovery between blocks during overreach — sustained
        // moderate HR-reserve all day keeps daily TRIMP high (lifts acute load).
        hr = Math.round(108 + noise(r, 10))
        activity = Math.max(0, 0.22 + Math.abs(noise(r, 0.08)))
        steps = Math.round(Math.max(0, 30 + noise(r, 12)))
      } else if (taper && !isSleep) {
        // Taper stretch before overreach: a genuinely light fortnight. Worn for a
        // ~10h core (08:00–18:00) and OFF-WRIST the rest of the waking day, all at
        // low sedentary HR → low daily TRIMP. The reduced worn hours (vs a normal
        // ~16h worn day) pull the 28-day CHRONIC baseline DOWN ahead of the
        // overreach, so the acute/chronic ratio at the overreach peak clears
        // caution (1.3) and reaches high-risk (>1.5). HR clears the awake override
        // (1.15*RHR ≈ 57) with margin so worn daytime never reads asleep, but sits
        // well below any cardio zone so worn minutes add little TRIMP. The worn
        // core is a short ~6h window (10:00–16:00) so per-day TRIMP is near the
        // detraining floor — pulling chronic low enough for a >1.5 peak.
        const wornCore = m >= 10 * 60 && m < 16 * 60
        if (wornCore) {
          hr = Math.max(64, Math.round(66 + noise(r, 4)))
          activity = Math.max(0.03, 0.05 + Math.abs(noise(r, 0.03)))
          steps = Math.round(Math.max(0, 4 + noise(r, 4)))
        } else {
          // off-wrist outside the core (band off / charging) — no TRIMP, no wear.
          hr = 0; activity = 0; steps = 0; wrist = 0
        }
      } else if (detraining && !isSleep) {
        // Detraining: mostly off-wrist rest + brief low-key sedentary worn
        // windows → very low daily TRIMP → acute drops below chronic → ACWR < 0.8.
        const restWorn = (m >= 9 * 60 && m < 11 * 60) || (m >= 15 * 60 && m < 17 * 60)
        if (restWorn) {
          // Low-key sedentary, but still WAKING: HR must clear the awake override
          // (hr > 1.15*RHR ≈ 55–59) with margin. Old 56±3 skimmed/dipped below it,
          // so detraining-day rest minutes read asleep and bridged the whole day
          // into one giant "night". Floor at 64 like ordinary daytime; activity
          // nudged up so it's clearly non-sleep motion. Sleep floor untouched —
          // TRIMP stays low (HR still well under any cardio zone) so ACWR < 0.8.
          hr = Math.max(64, Math.round(62 + noise(r, 3)))
          activity = Math.max(0.04, 0.05 + Math.abs(noise(r, 0.02)))
          steps = Math.round(Math.max(0, 1 + noise(r, 2)))
        } else {
          // off-wrist (resting / not worn) — contributes no TRIMP, no wear.
          hr = 0; activity = 0; steps = 0; wrist = 0
        }
      }
      if (doWorkout && m >= woStart + woDur && m < woStart + woDur + 3) {
        const since = m - (woStart + woDur)
        // Recovery improves over the 90d window (steeper drop later) so
        // calcFitnessTrend sees HRR60 slope > 0 → "improving".
        const dropRate = 18 + 10 * progress   // 18 → 28 bpm/min
        hr = Math.round((woKind === 'run' ? 168 : 150) - since * dropRate + noise(r, 3))
      }
    }

    hr = Math.max(0, hr)
    out.push({
      ts_min,
      hr_avg: hr,
      hr_min: hr > 0 ? Math.max(0, hr - 2) : 0,
      hr_max: hr > 0 ? hr + 2 : 0,
      hr_n: hr > 0 ? 60 : 0,
      activity: Math.round(activity * 1000) / 1000,
      steps,
      wrist_on: wrist,
    })
  }
  return out
}

async function findOrCreateUser(db: D1Database, email: string, now: number): Promise<string> {
  const e = email.toLowerCase().trim()
  const user = await db.prepare('SELECT id FROM users WHERE email = ?').bind(e).first<{ id: string }>()
  if (user) {
    await db.prepare('UPDATE users SET age=29, height_cm=178, weight_kg=75, sex=? WHERE id=?')
      .bind('m', user.id).run()
    return user.id
  }
  const userId = uuid()
  await db.prepare(
    'INSERT INTO users (id, email, name, age, height_cm, weight_kg, sex, created_at) VALUES (?,?,?,?,?,?,?,?)',
  ).bind(userId, e, 'Demo', 29, 178, 75, 'm', now).run()
  return userId
}

// phase=init — create/clear user. Returns user_id.
export async function seedInit(db: D1Database, email: string, now: number): Promise<{ user_id: string }> {
  const userId = await findOrCreateUser(db, email, now)
  for (const t of ['minute', 'daily', 'sleep', 'sessions', 'baselines', 'analytics_cursor']) {
    await db.prepare(`DELETE FROM ${t} WHERE user_id = ?`).bind(userId).run()
  }
  return { user_id: userId }
}

// phase=minutes — write days [dayFrom, dayTo). One D1 batch per day (one
// subrequest each), so keep the chunk ≤ ~30 days to stay under the cap.
export async function seedMinutes(
  db: D1Database, userId: string, days: number, dayFrom: number, dayTo: number, now: number,
): Promise<{ minutes: number }> {
  const todayStart = Math.floor(now / DAY) * DAY
  const firstDayStart = todayStart - (days - 1) * DAY
  const insert = db.prepare(
    'INSERT OR REPLACE INTO minute (user_id, ts_min, hr_avg, hr_min, hr_max, hr_n, hr_sum, activity, act_sum, act_n, steps, wrist_on) ' +
    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
  )
  let minutes = 0
  for (let d = dayFrom; d < dayTo && d < days; d++) {
    const dayStart = firstDayStart + d * DAY
    const rows = buildDay(dayStart, d, days)
    const batch: D1PreparedStatement[] = []
    for (const m of rows) {
      const hr_sum = m.hr_avg * m.hr_n
      const act_n = 60
      const act_sum = m.activity * act_n
      batch.push(insert.bind(userId, m.ts_min, m.hr_avg, m.hr_min, m.hr_max, m.hr_n, hr_sum,
        m.activity, act_sum, act_n, m.steps, m.wrist_on))
    }
    if (batch.length) { await db.batch(batch); minutes += batch.length }
  }
  return { minutes }
}

// phase=analytics — run processUser over the full history (two passes so
// baselines feed readiness/anomaly on the second pass).
export async function seedAnalytics(
  db: D1Database, userId: string, days: number, now: number,
): Promise<{ daily: number; sleep: number; sessions: number }> {
  await processUser(db, userId, { historyDays: days, now })
  return processUser(db, userId, { historyDays: days, now })
}
