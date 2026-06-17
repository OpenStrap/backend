// biometrics.ts — HRV + relative skin-temp + relative SpO₂, re-decoded from the
// raw V24 records already in R2. These fields ride in every 1 Hz historical record
// (rr intervals @18/19, raw red ADC @64, raw temp ADC @68) — validated on 127,971
// of our own records (RR 99.7% physiological; |g|≈1.012). No new capture needed.
//
// HRV is the real one: RMSSD over artifact-filtered beat-to-beat intervals during
// the resting/sleep window — the same substrate WHOOP builds recovery on. SpO₂ and
// skin temp are RAW ADCs (the band never sends a finished %/°C), so we only ever
// express them as a deviation from the user's own baseline — relative, never absolute.

import { parse_r24 } from 'openstrap-protocol/ts/records'
import {
  timeDomainHrv, freqDomainHrv, baevskyStressIndex,
  calcRecovery, calcStress, calcIllness,
  calcHrvStability, calcIrregular, calcReadinessIndex,
} from 'openstrap-analytics'

const DAY = 86400

interface BioEnv { DB: D1Database; RAW_BUCKET: R2Bucket }

const hexToBytes = (hex: string): Uint8Array => {
  const clean = hex.trim()
  const m = clean.match(/.{1,2}/g)
  if (!m) return new Uint8Array(0)
  return new Uint8Array(m.map((b) => parseInt(b, 16)))
}

const median = (a: number[]): number => {
  if (!a.length) return 0
  const s = [...a].sort((x, y) => x - y)
  const m = Math.floor(s.length / 2)
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}

// One decoded V24 record's biometrics, or null if it isn't a usable V24.
interface BioSample { ts: number; hr: number; rr: number[]; temp: number; spo2: number }
function decodeV24(hex: string): BioSample | null {
  const b = hexToBytes(hex)
  if (b.length < 89 || b[0] !== 0x2f || b[1] !== 24) return null
  const r = parse_r24(b)
  if (!r || !(r.ts_epoch > 0)) return null
  return {
    ts: r.ts_epoch,
    hr: r.hr,
    rr: r.rr_intervals_ms.filter((x) => x >= 300 && x <= 2000), // physiological
    temp: r.skin_temp_raw,
    spo2: r.spo2_red_raw,
  }
}

/**
 * RMSSD (ms) over a time-ordered RR stream, with successive-difference artifact
 * rejection (drop jumps > 200 ms — ectopics / missed beats). The standard
 * short-term HRV index; what nocturnal recovery is built on.
 */
export function rmssd(samples: BioSample[]): { rmssd: number | null; nBeats: number } {
  const seq: number[] = []
  for (const s of [...samples].sort((a, b) => a.ts - b.ts)) for (const x of s.rr) seq.push(x)
  if (seq.length < 30) return { rmssd: null, nBeats: seq.length }
  let sumSq = 0, n = 0
  for (let i = 1; i < seq.length; i++) {
    const d = seq[i] - seq[i - 1]
    if (Math.abs(d) <= 200) { sumSq += d * d; n++ }
  }
  if (n < 20) return { rmssd: null, nBeats: seq.length }
  return { rmssd: Math.round(Math.sqrt(sumSq / n) * 10) / 10, nBeats: seq.length }
}

async function rawKeysInWindow(bucket: R2Bucket, userId: string, from: number, to: number): Promise<string[]> {
  const out: string[] = []
  let cursor: string | undefined
  do {
    const listing = await bucket.list({ prefix: `raw/${userId}/`, cursor, limit: 1000 })
    for (const o of listing.objects) {
      const m = o.key.match(/-(\d+)-(\d+)\.txt$/)
      if (!m) { out.push(o.key); continue }
      const minTs = parseInt(m[1]), maxTs = parseInt(m[2])
      if (maxTs >= from && minTs <= to) out.push(o.key)
    }
    cursor = listing.truncated ? listing.cursor : undefined
  } while (cursor)
  return out
}

// Read + decode resting V24 records in [from,to]. "Resting" = HR in a calm band
// so HRV reflects parasympathetic tone, not exercise. Bounded to `maxObjects`
// most-recent objects — a Worker request has a tight CPU/subrequest budget, and
// each object holds ~50–90 records, so ~20 objects ≈ a few hundred beats, which
// is more than enough for a stable RMSSD.
async function restingSamples(env: BioEnv, userId: string, from: number, to: number, maxObjects: number): Promise<BioSample[]> {
  const keys = await rawKeysInWindow(env.RAW_BUCKET, userId, from, to)
  // Newest first by the maxTs embedded in the key (…-<minTs>-<maxTs>.txt).
  const keyTs = (k: string) => { const m = k.match(/-(\d+)\.txt$/); return m ? parseInt(m[1]) : 0 }
  keys.sort((a, b) => keyTs(b) - keyTs(a))
  const out: BioSample[] = []
  for (const key of keys.slice(0, maxObjects)) {
    const obj = await env.RAW_BUCKET.get(key)
    if (!obj) continue
    const text = await obj.text()
    for (const line of text.split('\n')) {
      const s = decodeV24(line)
      if (s && s.ts >= from && s.ts <= to && s.hr > 0 && s.hr <= 70) out.push(s)
    }
  }
  return out
}

interface NightBio {
  date: string
  rmssd: number | null; sdnn: number | null; lfhf: number | null; si: number | null
  resp: number | null; respConf: number; nBeats: number
  temp: number | null; spo2: number | null
  rr: number[] // time-ordered RR stream (ms) for stress scoring
}

/** Collect the time-ordered RR stream over a resting window + full HRV indices. */
async function computeDay(env: BioEnv, userId: string, date: string, from: number, to: number, maxObjects: number): Promise<NightBio> {
  const samples = (await restingSamples(env, userId, from, to, maxObjects)).sort((a, b) => a.ts - b.ts)
  const rr: number[] = []
  for (const s of samples) for (const x of s.rr) rr.push(x)
  const td = timeDomainHrv(rr)
  const fd = freqDomainHrv(rr)
  const si = baevskyStressIndex(rr)
  const temps = samples.map((s) => s.temp).filter((t) => t > 0)
  const spo2s = samples.map((s) => s.spo2).filter((s) => s > 0)
  return {
    date,
    rmssd: td.rmssd, sdnn: td.sdnn, lfhf: fd.lf_hf, si: si.si,
    resp: fd.resp_rate, respConf: fd.resp_conf, nBeats: td.n_beats,
    temp: temps.length ? median(temps) : null,
    spo2: spo2s.length ? median(spo2s) : null,
    rr,
  }
}

interface DailyHistRow { date: string; resting_hr: number | null; hrv_rmssd: number | null; hrv_si: number | null; skin_temp_idx: number | null }

/**
 * runBiometrics — for the last `days` days, re-decode resting RR from R2 and
 * compute the FULL HRV suite (RMSSD/SDNN/LF-HF/Baevsky-SI/resp), then derive the
 * HRV-based metrics that need beat-to-beat data: RECOVERY (Plews lnRMSSD z-score),
 * STRESS (Baevsky SI personal-relative), and the multivariate ILLNESS signal
 * (Mahalanobis). Writes those + their drivers to the daily row. Heavy (R2 reads) →
 * nightly cron / admin only, never inline ingest.
 */
export async function runBiometrics(env: BioEnv, userId: string, days = 3, onlyDate?: string): Promise<{ days: number; computed: number }> {
  const now = Math.floor(Date.now() / 1000)

  // Per-(user,day) fan-out: when onlyDate is set, process exactly that one UTC day
  // (a single bounded unit). Otherwise the trailing `days`.
  const since = onlyDate ?? new Date((now - days * DAY) * 1000).toISOString().slice(0, 10)
  const { results: sleeps } = await env.DB.prepare(
    'SELECT date, onset_ts, wake_ts, duration_min FROM sleep WHERE user_id = ? AND date >= ? AND onset_ts IS NOT NULL AND wake_ts IS NOT NULL',
  ).bind(userId, since).all<{ date: string; onset_ts: number; wake_ts: number; duration_min: number | null }>()
  const sleepByDate = new Map((sleeps ?? []).map((s) => [s.date, s]))

  // Sleep-need baseline (for the composite Readiness sleep component).
  const baseRow = await env.DB.prepare('SELECT sleep_need_min FROM baselines WHERE user_id = ?')
    .bind(userId).first<{ sleep_need_min: number | null }>()
  const sleepNeedMin = baseRow?.sleep_need_min ?? null

  // Prior history (for Plews baseline, personal SI baseline, illness covariance).
  const { results: hist } = await env.DB.prepare(
    'SELECT date, resting_hr, hrv_rmssd, hrv_si, skin_temp_idx FROM daily WHERE user_id = ? ORDER BY date DESC LIMIT 60',
  ).bind(userId).all<DailyHistRow>()
  const histRows = (hist ?? [])
  const rhrByDate = new Map(histRows.map((h) => [h.date, h.resting_hr]))
  const rmssdHist = histRows.map((h) => h.hrv_rmssd).filter((x): x is number => x != null)
  const siHist = histRows.map((h) => h.hrv_si).filter((x): x is number => x != null)
  const rhrHist = histRows.map((h) => h.resting_hr).filter((x): x is number => x != null)
  const tempHist = histRows.map((h) => h.skin_temp_idx).filter((x): x is number => x != null)

  // Per-request work budget (Worker CPU/subrequest limit). Newest day first.
  const PER_DAY = 12
  const TOTAL_OBJECTS = 24
  let spent = 0
  const out: NightBio[] = []
  // Build the explicit list of (dayStart, date) to process.
  const dayList: { dayStart: number; date: string }[] = []
  if (onlyDate) {
    dayList.push({ dayStart: Math.floor(Date.parse(`${onlyDate}T00:00:00Z`) / 1000), date: onlyDate })
  } else {
    for (let d = 0; d < days; d++) {
      const ds = Math.floor((now - d * DAY) / DAY) * DAY
      dayList.push({ dayStart: ds, date: new Date(ds * 1000).toISOString().slice(0, 10) })
    }
  }
  for (const { dayStart, date } of dayList) {
    const budget = Math.min(PER_DAY, TOTAL_OBJECTS - spent)
    // Budget exhausted for this run: do NOT emit a null row. A null here would flow
    // into the write loop and overwrite this day's already-computed HRV with null
    // (the multi-day clobber bug). Skip it entirely — the day keeps its stored value
    // and the next run (or a per-day onlyDate job, which always has full budget)
    // recomputes it. computeWindowSteps/restingSamples are bounded per day, so
    // multi-day runs starve the tail; skipping is the only non-destructive choice.
    if (budget <= 0) continue
    const sl = sleepByDate.get(date)
    const from = sl ? sl.onset_ts : dayStart
    const to = sl ? sl.wake_ts + 60 : dayStart + DAY
    out.push(await computeDay(env, userId, date, from, to, budget))
    spent += budget
  }

  // Baselines: rolling medians over what we just computed + whatever's stored.
  const allRmssd = [...out.map((o) => o.rmssd).filter((x): x is number => x != null), ...rmssdHist]
  const allSi = [...out.map((o) => o.si).filter((x): x is number => x != null), ...siHist]
  const temps = out.map((o) => o.temp).filter((x): x is number => x != null)
  const spo2s = out.map((o) => o.spo2).filter((x): x is number => x != null)
  const blHrv = allRmssd.length ? median(allRmssd) : null
  const blSi = allSi.length ? median(allSi) : null
  const blTemp = temps.length ? median(temps) : null
  const blSpo2 = spo2s.length ? median(spo2s) : null
  if (blHrv != null || blTemp != null || blSpo2 != null || blSi != null) {
    await env.DB.prepare(
      'UPDATE baselines SET hrv_rmssd = COALESCE(?, hrv_rmssd), hrv_si = COALESCE(?, hrv_si), skin_temp_raw = COALESCE(?, skin_temp_raw), spo2_raw = COALESCE(?, spo2_raw) WHERE user_id = ?',
    ).bind(blHrv, blSi, blTemp, blSpo2, userId).run()
  }

  let computed = 0
  for (const o of out) {
    // No usable RMSSD this night → there is nothing to write that wouldn't risk
    // erasing a previously-computed value with null. Leave the stored row untouched;
    // a thin/empty pass must never regress a good night. (processUser owns the
    // non-HRV daily fields, so skipping here loses nothing.)
    if (o.rmssd == null) continue
    const conf = Math.round(Math.min(1, o.nBeats / 500) * 1000) / 1000
    const tempIdx = (o.temp != null && blTemp != null) ? Math.round((o.temp - blTemp) * 10) / 10 : null
    const spo2Idx = (o.spo2 != null && blSpo2 != null) ? Math.round((o.spo2 - blSpo2) * 10) / 10 : null

    // HRV-based metrics (all published algorithms; null when no usable RR).
    const recovery = calcRecovery(o.rmssd, rmssdHist, { date: o.date })
    const stress = calcStress(o.rr, siHist, { date: o.date })
    const illness = calcIllness(
      { resting_hr: rhrByDate.get(o.date) ?? null, rmssd: o.rmssd, skin_temp: tempIdx },
      { resting_hr: rhrHist, rmssd: rmssdHist, skin_temp: tempHist },
    )

    // HRV stability (CV of recent nightly RMSSD) + irregular-rhythm screen (RR).
    const hrvStab = calcHrvStability([o.rmssd, ...rmssdHist].filter((x): x is number => x != null).slice(0, 14))
    const irregular = calcIrregular(o.rr)

    // Composite Readiness — recovery (HRV) ∩ sleep vs need ∩ nocturnal dip ∩ arousal.
    // Read this day's dip + sleep-stress (written by processUser) alongside drivers.
    await env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?, ?)').bind(userId, o.date).run()
    const existing = await env.DB.prepare('SELECT sleep_stress, nocturnal_dip_pct FROM daily WHERE user_id = ? AND date = ?')
      .bind(userId, o.date).first<{ sleep_stress: string | null; nocturnal_dip_pct: number | null }>()
    // Only the driver keys biometrics OWNS. We merge these into daily.drivers via
    // SQL json_patch at write time (below) rather than read-modify-write, so a
    // concurrent processUser sweep writing the MAIN drivers can't be clobbered by a
    // stale snapshot (the previous read→merge→overwrite had a TOCTOU window: a
    // 'sweep' and a 'biometrics' for the same user are distinct queue jobs and run
    // concurrently). json_patch is additive + order-independent for disjoint keys.
    const bioDrivers: Record<string, unknown> = {}
    if (recovery.drivers) bioDrivers.recovery = recovery.drivers
    if (stress.drivers) bioDrivers.stress = stress.drivers
    if (illness.drivers) bioDrivers.illness = illness.drivers

    const sleepStressScore = (() => { try { return existing?.sleep_stress ? JSON.parse(existing.sleep_stress)?.score ?? null : null } catch { return null } })()
    const readiness = calcReadinessIndex({
      recovery: recovery.score,
      sleepDurationMin: sleepByDate.get(o.date)?.duration_min ?? null,
      sleepNeedMin,
      dipPct: existing?.nocturnal_dip_pct ?? null,
      sleepStress: sleepStressScore,
    })
    if (readiness.drivers) bioDrivers.readiness = readiness.drivers

    // Measured HRV indices use COALESCE(?, col): a value computed this run wins, but
    // a null (e.g. enough beats for RMSSD but not for LF/HF) never erases a richer
    // prior night. Mirrors the resp_rate guard. Derived scores (recovery/readiness/
    // stress/illness/irregular) are recomputed fresh whenever we have an RMSSD (we
    // continue'd out of the loop otherwise), so they write directly.
    await env.DB.prepare(
      'UPDATE daily SET recovery = ?, hrv_rmssd = COALESCE(?, hrv_rmssd), hrv_conf = ?, ' +
      'hrv_sdnn = COALESCE(?, hrv_sdnn), hrv_lfhf = COALESCE(?, hrv_lfhf), hrv_si = COALESCE(?, hrv_si), ' +
      'hrv_cv = COALESCE(?, hrv_cv), irregular = ?, readiness = ?, ' +
      'resp_rate = COALESCE(?, resp_rate), resp_conf = COALESCE(?, resp_conf), ' +
      'skin_temp_idx = COALESCE(?, skin_temp_idx), spo2_idx = COALESCE(?, spo2_idx), stress = ?, illness = ?, ' +
      'drivers = json_patch(COALESCE(drivers, \'{}\'), ?), updated_at = ? ' +
      'WHERE user_id = ? AND date = ?',
    ).bind(
      recovery.score, o.rmssd, conf, o.sdnn, o.lfhf, o.si,
      hrvStab.cv, JSON.stringify(irregular), readiness.score,
      o.respConf >= 0.3 ? o.resp : null, o.respConf >= 0.3 ? o.respConf : null,
      tempIdx, spo2Idx, JSON.stringify(stress), JSON.stringify(illness), JSON.stringify(bioDrivers), now,
      userId, o.date,
    ).run()
    computed++ // reached only when o.rmssd != null (we continue'd above otherwise)
  }
  return { days, computed }
}
