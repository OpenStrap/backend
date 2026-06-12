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
//   steps    = peaks in the gravity-removed magnitude within a step cadence
//              band — real motion-derived step count (ESTIMATE tier: wrist
//              accelerometry is inherently approximate). accel scale ÷4096 is
//              physics-validated (|a|≈1g at rest).
// A "step" = a magnitude peak above ACCEL_THRESH g that is ≥ MIN_GAP samples
// from the previous peak (caps cadence ~ a few Hz so arm swings ≠ double-count).
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
  const mean = mags.reduce((s, v) => s + v, 0) / mags.length
  const variance = mags.reduce((s, v) => s + (v - mean) ** 2, 0) / mags.length
  const activity = Math.round(Math.sqrt(variance) * 1000) / 1000

  // Step counting: peaks in the gravity-removed signal (mag − mean).
  const ACCEL_THRESH = 0.18 // g above baseline to count as a footfall
  const MIN_GAP = 6 // min samples between peaks (cadence cap)
  let steps = 0
  let lastPeak = -MIN_GAP;
  for (let i = 1; i < n - 1; i++) {
    const d = mags[i] - mean
    if (d > ACCEL_THRESH && mags[i] >= mags[i - 1] && mags[i] > mags[i + 1] &&
        i - lastPeak >= MIN_GAP) {
      steps++
      lastPeak = i
    }
  }
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
