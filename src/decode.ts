// decode.ts — verified WHOOP record decoders, mirroring the reference client / PROTOCOL.md.
//
// Each decoded record emits a DecodedSample:
//   { ts, hr, activity, steps_inc, wrist_on, rec_type }
// where `activity` is the actigraphy signal = stddev of |accel(g)| over the
// 100-sample IMU window (R10 only; 0 for HR-only records).
//
// Offsets (PROTOCOL.md):
//   R10  (rec_type 10, pkt 0x2F live/0x2B): ts@7, hr@17, counter@3,
//        accel arrays @85/285/485, gyro @688/888/1088, scale ÷4096 (4096 LSB/g).
//   0x28 (live compact HR): ts@2 (u32 LE), hr@8 (u8), wrist via hr>0.
//   0x2B: same layout as R10 (live R10).
//   R24  (rec_type 24): header ts@7, counter@3; spo2@72, skin_temp@70/4, resting_hr@88 (RELATIVE-only, not surfaced here).
//   0x33: live IMU stream — RAW-ONLY, no sample emitted (low decode confidence).
//
// NEVER decode HRV / RR-intervals. (R17 is BANNED.)

import { parse_r24 } from 'openstrap-protocol/ts/records'

export interface DecodedSample {
  ts: number          // unix seconds
  hr: number          // bpm (0 = off-wrist / no reading)
  activity: number    // motion magnitude (stddev of |accel(g)|), 0 if no IMU
  steps_inc: number   // steps detected in this record's IMU window (R10 only)
  wrist_on: boolean   // worn proxy (hr>0; authoritative wear is WRIST_ON/OFF events)
  rec_type: number    // 10 | 24 | 28
}

export const hexToBytes = (hex: string): Uint8Array =>
  new Uint8Array(hex.trim().match(/.{1,2}/g)!.map((b) => parseInt(b, 16)))

// Decode the R10 IMU arrays into (activity, steps) over the 100-sample window.
//   activity = stddev of per-sample |accel|(g)  — actigraphy intensity.
//   steps    = count of GAIT CYCLES only when the window is genuinely rhythmic.
//
// The band has no pedometer field — steps are estimated from wrist IMU (ESTIMATE
// tier). The naive "count every peak" approach over-counts badly: any arm gesture,
// typing, or vehicle bump clears a fixed threshold. Instead we require RHYTHM:
// walking produces a periodic acceleration signal, so we (1) detrend the magnitude
// to remove gravity + slow drift, (2) measure how periodic it is via normalized
// autocorrelation over plausible step lags, and (3) only count steps when that
// periodicity is strong AND the limb is actually moving. Steps in the window =
// number of gait cycles = n / (dominant lag). Non-rhythmic motion → 0 steps.
// (Same autocorrelation idea we use for respiratory rate; standard wrist pedometry.)
function r10Motion(view: DataView, len: number): { activity: number; steps: number } {
  if (len < 685) return { activity: 0, steps: 0 }
  const ACC = 1 / 4096
  const arr = (off: number): number[] => {
    const out: number[] = []
    for (let i = 0; i < 100; i++) {
      const o = off + 2 * i
      if (o + 2 <= len) out.push(view.getInt16(o, true))
    }
    return out
  }
  const ax = arr(85), ay = arr(285), az = arr(485) // accel X/Y/Z
  const n = Math.min(ax.length, ay.length, az.length)
  if (n === 0) return { activity: 0, steps: 0 }
  const mags: number[] = []
  for (let i = 0; i < n; i++) {
    mags.push(Math.hypot(ax[i] * ACC, ay[i] * ACC, az[i] * ACC))
  }
  const mean = mags.reduce((s, v) => s + v, 0) / n
  const variance = mags.reduce((s, v) => s + (v - mean) ** 2, 0) / n
  const std = Math.sqrt(variance)
  const activity = Math.round(std * 1000) / 1000

  // Limb must actually be oscillating — quiet wrist (typing/holding) → no steps.
  const ACTIVITY_FLOOR = 0.05 // g RMS of the detrended signal
  if (std < ACTIVITY_FLOOR || n < 24) return { activity, steps: 0 }

  // Detrend: remove a centered moving average (gravity + slow drift), leaving the
  // gait oscillation around 0.
  const W = 9
  const x: number[] = new Array(n);
  for (let i = 0; i < n; i++) {
    let s = 0, c = 0
    for (let j = Math.max(0, i - W); j <= Math.min(n - 1, i + W); j++) { s += mags[j]; c++ }
    x[i] = mags[i] - s / c
  }
  const x0 = x.reduce((s, v) => s + v, 0) / n
  let denom = 0
  for (let i = 0; i < n; i++) denom += (x[i] - x0) ** 2
  if (denom <= 1e-9) return { activity, steps: 0 }

  // Normalized autocorrelation over plausible step lags. Cadence ~1.4–3.0 steps/s;
  // with a ~25 Hz IMU window that's ~8–18 samples/step. We scan a generous band
  // and rely on the periodicity strength to confirm gait.
  const MIN_LAG = 7, MAX_LAG = 40
  let bestLag = 0, bestR = 0
  for (let lag = MIN_LAG; lag <= Math.min(MAX_LAG, n - 1); lag++) {
    let num = 0
    for (let i = 0; i < n - lag; i++) num += (x[i] - x0) * (x[i + lag] - x0)
    const r = num / denom
    if (r > bestR) { bestR = r; bestLag = lag }
  }

  // Strong, sustained periodicity ⇒ walking/running; count the gait cycles.
  const RHYTHM_THRESH = 0.45
  if (bestLag === 0 || bestR < RHYTHM_THRESH) return { activity, steps: 0 }
  const steps = Math.round(n / bestLag)
  return { activity, steps }
}

/**
 * Decode one hex record into a DecodedSample, or null if it carries no
 * surfaceable sample (0x33 IMU stream, malformed, or unknown type).
 */
export function decodeRecord(hex: string): DecodedSample | null {
  let b: Uint8Array
  try {
    b = hexToBytes(hex)
  } catch {
    return null
  }
  if (b.length < 4) return null
  const view = new DataView(b.buffer, b.byteOffset, b.byteLength)
  const pktType = b[0]
  const recType = b[1]

  // 0x28 — live compact HR: ts@2 (u32 LE), hr@8 (u8). NO RR-intervals.
  if (pktType === 0x28) {
    if (b.length < 9) return null
    const ts = view.getUint32(2, true)
    const hr = b[8]
    return { ts, hr, activity: 0, steps_inc: 0, wrist_on: hr > 0, rec_type: 28 }
  }

  // 0x33 — live IMU stream: raw-only (kept in R2, no sample emitted).
  if (pktType === 0x33) return null

  if (b.length < 18) return null

  // R24 — type-24 historical telemetry.
  if (recType === 24) {
    const d = parse_r24(b)
    if (!d) return null
    return { ts: d.ts_epoch, hr: d.hr, activity: 0, steps_inc: 0, wrist_on: d.hr > 0, rec_type: 24 }
  }

  // R10 / 0x2B — ts@7, hr@17, IMU arrays → activity.
  if (recType === 10) {
    const ts = view.getUint32(7, true)
    const hr = b[17]
    const m = r10Motion(view, b.length)
    return { ts, hr, activity: m.activity, steps_inc: m.steps, wrist_on: hr > 0, rec_type: 10 }
  }

  return null
}

/** Decode a batch of hex records, returning all surfaceable samples. */
export function decodeBatch(records: string[]): DecodedSample[] {
  const out: DecodedSample[] = []
  for (const hex of records) {
    const s = decodeRecord(hex)
    if (s) out.push(s)
  }
  return out
}
