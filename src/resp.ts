// resp.ts — respiratory rate from the optical PPG (R21), re-decoded from the raw
// frames in R2. THE ONLY honest source of breaths/min on this hardware: the green
// PPG channel's slow amplitude/baseline modulation (RIIV) tracks breathing.
//
// IMPORTANT REALITY: R21 (6-channel optical PPG) is only emitted during the LIVE
// realtime stream (SEND_R10_R11_REALTIME). Overnight, the band flashes R24 (1 Hz
// HR, NO PPG arrays), so on normal nights there is NO R21 in R2 and resp stays
// null. This pipeline computes a number ONLY when there is enough contiguous R21
// data AND the respiratory periodicity is strong — otherwise confidence 0 and it
// is never surfaced (the /today + /day/sleep gate requires resp_conf ≥ 0.5).
// This mirrors the HRV stance: never fabricate. BETA until validated on real PPG.

const DAY = 86400

interface RespEnv { DB: D1Database; RAW_BUCKET: R2Bucket }

const hexToBytes = (hex: string): Uint8Array => {
  const clean = hex.trim()
  const m = clean.match(/.{1,2}/g)
  if (!m) return new Uint8Array(0)
  return new Uint8Array(m.map((b) => parseInt(b, 16)))
}

// Decode one R21 record's green PPG channel (channel A @20, 100×u16 LE). Returns
// the record's epoch ts + the green samples, or null if it isn't a usable R21.
// Layout from the reference client — ts@7 (u32), channel A @20 (100 u16).
function decodeR21Green(hex: string): { ts: number; green: number[] } | null {
  const b = hexToBytes(hex)
  if (b.length < 620) return null
  if (b[1] !== 21) return null // rec_type 21
  const view = new DataView(b.buffer, b.byteOffset, b.byteLength)
  const ts = view.getUint32(7, true)
  if (!(ts > 0)) return null
  const green: number[] = []
  for (let i = 0; i < 100; i++) {
    const o = 20 + 2 * i
    if (o + 2 <= b.length) green.push(view.getUint16(o, true))
  }
  return green.length >= 50 ? { ts, green } : null
}

const mean = (a: number[]) => a.reduce((s, v) => s + v, 0) / (a.length || 1)

/**
 * Estimate respiratory rate (breaths/min) from a window of R21 records via
 * autocorrelation of the per-record green level (RIIV proxy). Returns
 * {resp_rate, confidence}; confidence 0 when the data is insufficient or the
 * periodicity is weak (→ never surfaced). Conservative by design.
 */
export function estimateResp(records: { ts: number; green: number[] }[]): { resp_rate: number | null; confidence: number } {
  if (records.length < 120) return { resp_rate: null, confidence: 0 }
  // One respiration-relevant sample per record = mean green level, by ts.
  const byTs = new Map<number, number[]>()
  for (const r of records) {
    const arr = byTs.get(r.ts)
    if (arr) arr.push(mean(r.green)); else byTs.set(r.ts, [mean(r.green)])
  }
  const pts = [...byTs.entries()].map(([ts, vs]) => ({ ts, v: mean(vs) })).sort((a, b) => a.ts - b.ts)
  const spanSec = pts[pts.length - 1].ts - pts[0].ts
  if (pts.length < 120 || spanSec < 360) return { resp_rate: null, confidence: 0 }

  // Resample to a uniform 1 Hz grid (records are ~1/s during the live stream).
  const n = Math.min(spanSec, 1800) // cap at 30 min of signal
  const grid: number[] = new Array(n)
  let j = 0
  for (let i = 0; i < n; i++) {
    const t = pts[0].ts + i
    while (j + 1 < pts.length && pts[j + 1].ts <= t) j++
    grid[i] = pts[j].v
  }
  // Detrend: subtract a 15 s moving average (kills the slow DC drift, keeps breath band).
  const win = 15
  const detr: number[] = new Array(n)
  for (let i = 0; i < n; i++) {
    let lo = Math.max(0, i - win), hi = Math.min(n - 1, i + win), s = 0
    for (let k = lo; k <= hi; k++) s += grid[k]
    detr[i] = grid[i] - s / (hi - lo + 1)
  }
  const varAll = mean(detr.map((x) => x * x))
  if (varAll <= 0) return { resp_rate: null, confidence: 0 }

  // Autocorrelation; respiratory band = lags 2..10 s (6..30 breaths/min).
  let bestLag = 0, bestR = 0
  for (let lag = 2; lag <= 10; lag++) {
    let s = 0
    for (let i = 0; i + lag < n; i++) s += detr[i] * detr[i + lag]
    const r = s / ((n - lag) * varAll)
    if (r > bestR) { bestR = r; bestLag = lag }
  }
  if (bestLag === 0) return { resp_rate: null, confidence: 0 }
  const resp = Math.round((60 / bestLag) * 10) / 10
  // Confidence = autocorrelation peak strength × coverage. Gate at ≥0.5 elsewhere.
  const coverage = Math.min(1, n / 900) // ~15 min of clean signal → full coverage
  const confidence = Math.round(Math.max(0, Math.min(1, bestR)) * coverage * 1000) / 1000
  return { resp_rate: resp, confidence }
}

// List R2 raw objects whose key time-range overlaps [from,to]. Keys look like
// raw/<user>/<device>/<putMs>-<minTs>-<maxTs>.txt (see ingest.ts).
async function rawKeysInWindow(bucket: R2Bucket, userId: string, from: number, to: number): Promise<string[]> {
  const out: string[] = []
  let cursor: string | undefined
  do {
    const listing = await bucket.list({ prefix: `raw/${userId}/`, cursor, limit: 1000 })
    for (const o of listing.objects) {
      const m = o.key.match(/-(\d+)-(\d+)\.txt$/)
      if (!m) { out.push(o.key); continue } // unparseable → include (cheap safety)
      const minTs = parseInt(m[1]), maxTs = parseInt(m[2])
      if (maxTs >= from && minTs <= to) out.push(o.key)
    }
    cursor = listing.truncated ? listing.cursor : undefined
  } while (cursor)
  return out
}

/** Compute + store resp rate for one night. Idempotent. */
async function computeNight(env: RespEnv, userId: string, date: string, from: number, to: number): Promise<{ resp_rate: number | null; confidence: number; r21: number }> {
  const keys = await rawKeysInWindow(env.RAW_BUCKET, userId, from, to)
  const records: { ts: number; green: number[] }[] = []
  for (const key of keys) {
    const obj = await env.RAW_BUCKET.get(key)
    if (!obj) continue
    const text = await obj.text()
    for (const line of text.split('\n')) {
      const r = decodeR21Green(line)
      if (r && r.ts >= from && r.ts <= to) records.push(r)
    }
  }
  const { resp_rate, confidence } = estimateResp(records)
  // Only persist a real reading. A null pass (absent/sparse R21 — the band only
  // emits R21 during live streaming, so most nights have none) must NOT erase a
  // prior good night: the night is over, its resp doesn't change after the fact.
  // A never-written night stays null by default, so the display gate
  // (resp_conf >= 0.5) remains authoritative either way. COALESCE can't be used
  // here because confidence is 0 (not null) when absent, which would still clobber.
  if (resp_rate != null) {
    await env.DB.prepare(
      'UPDATE daily SET resp_rate = ?, resp_conf = ? WHERE user_id = ? AND date = ?',
    ).bind(resp_rate, confidence, userId, date).run()
  }
  return { resp_rate, confidence, r21: records.length }
}

/**
 * runRespRate — for each of the last `days` nights with a sleep row, re-decode
 * R21 from R2 and store resp_rate/resp_conf on daily. Also refreshes the resp
 * baseline (median of nights with conf ≥ 0.5). Heavy (R2 reads) → nightly cron /
 * admin only, NEVER inline ingest.
 */
export async function runRespRate(env: RespEnv, userId: string, days = 3, onlyDate?: string): Promise<{ nights: number; computed: number }> {
  // Per-(user,day) fan-out: onlyDate → just that night; else the trailing `days`.
  const since = onlyDate ?? new Date(Date.now() - days * DAY * 1000).toISOString().slice(0, 10)
  const dateClause = onlyDate ? 'date = ?' : 'date >= ?'
  const { results: nights } = await env.DB.prepare(
    `SELECT date, onset_ts, wake_ts FROM sleep WHERE user_id = ? AND ${dateClause} AND onset_ts IS NOT NULL AND wake_ts IS NOT NULL ORDER BY date DESC`,
  ).bind(userId, since).all<{ date: string; onset_ts: number; wake_ts: number }>()

  let computed = 0
  const valid: number[] = []
  for (const n of nights ?? []) {
    const r = await computeNight(env, userId, n.date, n.onset_ts, n.wake_ts + 60)
    if (r.resp_rate != null && r.confidence >= 0.5) { computed++; valid.push(r.resp_rate) }
  }
  if (valid.length >= 3) {
    valid.sort((a, b) => a - b)
    const med = valid[Math.floor(valid.length / 2)]
    await env.DB.prepare('UPDATE baselines SET resp_rate = ? WHERE user_id = ?')
      .bind(Math.round(med * 10) / 10, userId).run()
  }
  return { nights: (nights ?? []).length, computed }
}
