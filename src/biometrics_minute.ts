// biometrics_minute.ts — [v2] HRV + recovery from minute.rr (D1), NOT R2.
//
// The demand-driven engine stores per-minute RR at ingest (minute.rr), so the
// headline HRV stack (RMSSD/SDNN/LF-HF, Plews recovery, Baevsky stress, illness,
// irregular screen) is computed from D1 — no R2 list/get, no re-decode. Skin-temp
// and SpO₂ (raw ADCs @64/@68) are NOT in the minute rollup, so they stay R2-only
// (rare admin re-derive); we pass null → COALESCE keeps any prior value.
//
// Mirrors runBiometrics' write contract exactly (same daily columns, same
// null-safe COALESCE / json_patch drivers merge) so /today and the trend caches
// see an identical shape.

import {
  timeDomainHrv, freqDomainHrv, baevskyStressIndex,
  calcRecovery, calcStress, calcIllness, calcHrvStability, calcIrregular, calcReadinessIndex,
} from 'openstrap-analytics'
import { loadDayRr } from './dayseries'

// [v3] RAW_BUCKET + MINUTE_SOURCE: RR comes via loadDayRr (D1 minute.rr by default,
// R2-decoded series when MINUTE_SOURCE='r2').
interface BioEnv { DB: D1Database; RAW_BUCKET?: R2Bucket; MINUTE_SOURCE?: string }

interface DailyHistRow { date: string; resting_hr: number | null; hrv_rmssd: number | null; hrv_si: number | null; skin_temp_idx: number | null }

/**
 * Compute + persist HRV/recovery for ONE physiological night, sourced from D1
 * minute.rr over [from,to] (sleep window). Idempotent & null-safe: a night with
 * no usable RR leaves the stored row untouched (never regresses a good night).
 */
export async function runBiometricsMinute(
  env: BioEnv, userId: string, date: string, from: number, to: number,
): Promise<{ computed: boolean }> {
  const now = Math.floor(Date.now() / 1000)

  // 1. RR stream over the sleep window, time-ordered (minute.rr / R2 series).
  const rrByMin = await loadDayRr(env, userId, from, to)
  const rr: number[] = []
  for (const ts of [...rrByMin.keys()].sort((a, b) => a - b)) {
    for (const v of rrByMin.get(ts)!) if (v >= 300 && v <= 2000) rr.push(v)
  }

  const td = timeDomainHrv(rr)
  if (td.rmssd == null) return { computed: false } // no usable HRV → never write null

  const fd = freqDomainHrv(rr)
  const si = baevskyStressIndex(rr)

  // 2. Prior history for Plews baseline / personal SI / illness covariance + sleep-need.
  const { results: hist } = await env.DB.prepare(
    'SELECT date, resting_hr, hrv_rmssd, hrv_si, skin_temp_idx FROM daily WHERE user_id = ? ORDER BY date DESC LIMIT 60',
  ).bind(userId).all<DailyHistRow>()
  const histRows = hist ?? []
  const rmssdHist = histRows.map((h) => h.hrv_rmssd).filter((x): x is number => x != null)
  const siHist = histRows.map((h) => h.hrv_si).filter((x): x is number => x != null)
  const rhrHist = histRows.map((h) => h.resting_hr).filter((x): x is number => x != null)
  const tempHist = histRows.map((h) => h.skin_temp_idx).filter((x): x is number => x != null)
  const rhrByDate = new Map(histRows.map((h) => [h.date, h.resting_hr]))

  const baseRow = await env.DB.prepare(
    'SELECT sleep_need_min FROM baselines WHERE user_id = ?',
  ).bind(userId).first<{ sleep_need_min: number | null }>()
  const sleepNeedMin = baseRow?.sleep_need_min ?? null
  const sleepRow = await env.DB.prepare(
    'SELECT duration_min FROM sleep WHERE user_id = ? AND date = ?',
  ).bind(userId, date).first<{ duration_min: number | null }>()

  // 3. Derived HRV metrics (published; temp/spo2 omitted — not in minute.rr).
  const conf = Math.round(Math.min(1, td.n_beats / 500) * 1000) / 1000
  const recovery = calcRecovery(td.rmssd, rmssdHist, { date })
  const stress = calcStress(rr, siHist, { date })
  const illness = calcIllness(
    { resting_hr: rhrByDate.get(date) ?? null, rmssd: td.rmssd, skin_temp: null },
    { resting_hr: rhrHist, rmssd: rmssdHist, skin_temp: tempHist },
  )
  const hrvStab = calcHrvStability([td.rmssd, ...rmssdHist].filter((x): x is number => x != null).slice(0, 14))
  const irregular = calcIrregular(rr)

  // 4. Composite Readiness (reads this night's dip + sleep-stress written by processUser).
  await env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?, ?)').bind(userId, date).run()
  const existing = await env.DB.prepare(
    'SELECT sleep_stress, nocturnal_dip_pct FROM daily WHERE user_id = ? AND date = ?',
  ).bind(userId, date).first<{ sleep_stress: string | null; nocturnal_dip_pct: number | null }>()
  const sleepStressScore = (() => { try { return existing?.sleep_stress ? JSON.parse(existing.sleep_stress)?.score ?? null : null } catch { return null } })()
  const readiness = calcReadinessIndex({
    recovery: recovery.score,
    sleepDurationMin: sleepRow?.duration_min ?? null,
    sleepNeedMin,
    dipPct: existing?.nocturnal_dip_pct ?? null,
    sleepStress: sleepStressScore,
  })

  const bioDrivers: Record<string, unknown> = {}
  if (recovery.drivers) bioDrivers.recovery = recovery.drivers
  if (stress.drivers) bioDrivers.stress = stress.drivers
  if (illness.drivers) bioDrivers.illness = illness.drivers
  if (readiness.drivers) bioDrivers.readiness = readiness.drivers

  // 5. Persist (same null-safe COALESCE + json_patch contract as runBiometrics; temp/spo2 → null).
  await env.DB.prepare(
    'UPDATE daily SET recovery = ?, hrv_rmssd = COALESCE(?, hrv_rmssd), hrv_conf = ?, ' +
    'hrv_sdnn = COALESCE(?, hrv_sdnn), hrv_lfhf = COALESCE(?, hrv_lfhf), hrv_si = COALESCE(?, hrv_si), ' +
    'hrv_cv = COALESCE(?, hrv_cv), irregular = ?, readiness = ?, ' +
    'resp_rate = COALESCE(?, resp_rate), resp_conf = COALESCE(?, resp_conf), ' +
    'stress = ?, illness = ?, drivers = json_patch(COALESCE(drivers, \'{}\'), ?), updated_at = ? ' +
    'WHERE user_id = ? AND date = ?',
  ).bind(
    recovery.score, td.rmssd, conf, td.sdnn, fd.lf_hf, si.si,
    hrvStab.cv, JSON.stringify(irregular), readiness.score,
    fd.resp_conf >= 0.3 ? fd.resp_rate : null, fd.resp_conf >= 0.3 ? fd.resp_conf : null,
    JSON.stringify(stress), JSON.stringify(illness), JSON.stringify(bioDrivers), now,
    userId, date,
  ).run()
  return { computed: true }
}
