// biometrics_minute.ts — [v2] HRV + recovery from minute.rr (D1), NOT R2.
//
// The demand-driven engine stores per-minute RR at ingest (minute.rr), so the
// headline HRV stack (RMSSD/SDNN/LF-HF, Plews recovery, Baevsky stress, illness,
// irregular screen) is computed from D1 — no R2 list/get, no re-decode. Skin-temp
// and SpO₂ now ride the same minute blob: ingest aggregates per-minute red/IR/temp
// ADCs (wrist-on), and the close path derives a RELATIVE skin-temp index + a red/IR
// SpO₂ index (confidence-gated) vs rolling personal baselines — still zero extra R2.
//
// Mirrors runBiometrics' write contract exactly (same daily columns, same
// null-safe COALESCE / json_patch drivers merge) so /today and the trend caches
// see an identical shape.

import {
  timeDomainHrv, freqDomainHrv, baevskyStressIndex,
  calcRecovery, calcStress, calcIllness, calcHrvStability, calcIrregular, calcReadinessIndex,
  calcSpo2Index, median,
} from 'openstrap-analytics'
import { readMinutes } from './minute_store'

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

  // 1. Read the sleep window's minutes ONCE (D1 hot + R2 sealed) → RR stream + optical
  //    aggregates. Single read keeps the close path cheap (no extra R2).
  const recs = await readMinutes(env, userId, from, to)
  const rr: number[] = []
  const spo2Ratios: number[] = []
  const tempVals: number[] = []
  for (const m of recs) {
    for (const v of m.rr ?? []) if (v >= 300 && v <= 2000) rr.push(v)
    // Optical: per-minute mean red/IR ratio + mean temp (wrist-on samples only at ingest).
    if (m.opt_n && m.opt_n > 0 && m.ir_sum && m.ir_sum > 0) {
      spo2Ratios.push((m.red_sum ?? 0) / m.ir_sum)
      tempVals.push((m.temp_sum ?? 0) / m.opt_n)
    }
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
    // spo2_raw holds the rolling baseline red/IR RATIO (v2 uses the ratio, not raw red);
    // skin_temp_raw holds the rolling baseline temp ADC.
    'SELECT sleep_need_min, spo2_raw, skin_temp_raw FROM baselines WHERE user_id = ?',
  ).bind(userId).first<{ sleep_need_min: number | null; spo2_raw: number | null; skin_temp_raw: number | null }>()
  const sleepNeedMin = baseRow?.sleep_need_min ?? null
  const sleepRow = await env.DB.prepare(
    'SELECT duration_min FROM sleep WHERE user_id = ? AND date = ?',
  ).bind(userId, date).first<{ duration_min: number | null }>()

  // 3a. Optical metrics (RELATIVE): red/IR SpO₂ index + skin-temp index, vs rolling
  //     personal baselines. Confidence-gated in analytics (noisy nights → low conf/null).
  const baseRatio = baseRow?.spo2_raw ?? null
  const baseTemp = baseRow?.skin_temp_raw ?? null
  const spo2 = calcSpo2Index(spo2Ratios, baseRatio)
  const nightTemp = tempVals.length >= 30 ? median(tempVals) : null
  const tempIdx = (nightTemp != null && baseTemp != null) ? Math.round((nightTemp - baseTemp) * 10) / 10 : null
  // EWMA-roll the baselines (α=0.1) so they adapt slowly; seed on first valid night.
  const newBaseRatio = spo2.night_ratio != null
    ? (baseRatio != null ? Math.round((baseRatio * 0.9 + spo2.night_ratio * 0.1) * 10000) / 10000 : spo2.night_ratio)
    : null
  const newBaseTemp = nightTemp != null
    ? (baseTemp != null ? Math.round((baseTemp * 0.9 + nightTemp * 0.1) * 10) / 10 : Math.round(nightTemp * 10) / 10)
    : null

  // 3b. Derived HRV metrics (published; SpO₂/temp now sourced from minute optical aggregates).
  const conf = Math.round(Math.min(1, td.n_beats / 500) * 1000) / 1000
  const recovery = calcRecovery(td.rmssd, rmssdHist, { date })
  const stress = calcStress(rr, siHist, { date })
  const illness = calcIllness(
    { resting_hr: rhrByDate.get(date) ?? null, rmssd: td.rmssd, skin_temp: tempIdx },
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

  if (spo2.drivers) bioDrivers.spo2 = spo2.drivers

  // 5. Persist (same null-safe COALESCE + json_patch contract as runBiometrics). SpO₂/temp
  //    now sourced from minute optical aggregates → spo2_idx / skin_temp_idx (RELATIVE).
  await env.DB.prepare(
    'UPDATE daily SET recovery = ?, hrv_rmssd = COALESCE(?, hrv_rmssd), hrv_conf = ?, ' +
    'hrv_sdnn = COALESCE(?, hrv_sdnn), hrv_lfhf = COALESCE(?, hrv_lfhf), hrv_si = COALESCE(?, hrv_si), ' +
    'hrv_cv = COALESCE(?, hrv_cv), irregular = ?, readiness = ?, ' +
    'resp_rate = COALESCE(?, resp_rate), resp_conf = COALESCE(?, resp_conf), ' +
    'spo2_idx = COALESCE(?, spo2_idx), skin_temp_idx = COALESCE(?, skin_temp_idx), ' +
    'stress = ?, illness = ?, drivers = json_patch(COALESCE(drivers, \'{}\'), ?), updated_at = ? ' +
    'WHERE user_id = ? AND date = ?',
  ).bind(
    recovery.score, td.rmssd, conf, td.sdnn, fd.lf_hf, si.si,
    hrvStab.cv, JSON.stringify(irregular), readiness.score,
    fd.resp_conf >= 0.3 ? fd.resp_rate : null, fd.resp_conf >= 0.3 ? fd.resp_conf : null,
    // SpO₂ index only when confidence is meaningful (noisy nights → keep prior, don't regress).
    spo2.index != null && spo2.confidence >= 0.3 ? spo2.index : null, tempIdx,
    JSON.stringify(stress), JSON.stringify(illness), JSON.stringify(bioDrivers), now,
    userId, date,
  ).run()

  // 5b. Roll the optical baselines (EWMA). Only when we got a fresh night value; null-safe.
  if (newBaseRatio != null || newBaseTemp != null) {
    await env.DB.prepare(
      'UPDATE baselines SET spo2_raw = COALESCE(?, spo2_raw), skin_temp_raw = COALESCE(?, skin_temp_raw), updated_at = ? WHERE user_id = ?',
    ).bind(newBaseRatio, newBaseTemp, now, userId).run()
    // Ensure a baselines row exists (first-ever night for a brand-new user).
    await env.DB.prepare(
      'INSERT OR IGNORE INTO baselines(user_id, spo2_raw, skin_temp_raw, updated_at) VALUES(?,?,?,?)',
    ).bind(userId, newBaseRatio, newBaseTemp, now).run()
  }
  return { computed: true }
}
