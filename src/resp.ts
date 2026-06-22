// resp.ts — respiratory rate from the optical PPG (R21) green channel's slow
// amplitude/baseline modulation (RIIV), which tracks breathing. THE only honest
// optical source of breaths/min on this hardware.
//
// [drop-r2] The PPG waveform is no longer archived to R2. Instead, ingest stores the
// per-second mean-green RIIV proxy in the D1 minute blob (MinuteRec.green) — the ONLY
// value this algorithm consumes — and resp is computed from D1 at the wake-close via
// respFromMinuteGreen(). estimateResp (the validated autocorrelation core) is unchanged.
//
// REALITY: R21 is emitted ONLY during the live realtime stream; overnight the band
// flashes R24 (1 Hz HR, no PPG), so on normal nights there is no green series and resp
// stays null (RSA-from-RR resp, computed in biometrics_minute, covers those nights).
// Computes a number ONLY with enough contiguous signal AND strong periodicity; else
// confidence 0 and it's never surfaced (display gate requires resp_conf ≥ 0.5).

const mean = (a: number[]) => a.reduce((s, v) => s + v, 0) / (a.length || 1)

/**
 * Estimate respiratory rate (breaths/min) from a window of per-record green levels
 * via autocorrelation of the RIIV proxy. Returns {resp_rate, confidence}; confidence 0
 * when data is insufficient or periodicity is weak (→ never surfaced). Conservative.
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

/**
 * PPG-resp from the D1 minute store. Flattens each minute's stored per-second green
 * RIIV proxy into a time-ordered series and runs estimateResp over it. Synthesizes a
 * 1 Hz timestamp per sample (ts_min + index) — estimateResp resamples by ts span, so
 * the exact sub-minute placement is immaterial. Returns {null, 0} when no usable PPG
 * (the normal-night case → resp stays whatever RSA-from-RR produced).
 */
export function respFromMinuteGreen(recs: { ts_min: number; green?: number[] }[]): { resp_rate: number | null; confidence: number } {
  const records: { ts: number; green: number[] }[] = []
  for (const m of recs) {
    if (!m.green || !m.green.length) continue
    for (let i = 0; i < m.green.length; i++) records.push({ ts: m.ts_min + i, green: [m.green[i]] })
  }
  if (!records.length) return { resp_rate: null, confidence: 0 }
  return estimateResp(records)
}
