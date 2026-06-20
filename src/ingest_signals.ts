// ingest_signals.ts — [v2] per-minute STEPS (AN-2554 pedometer) + RR, computed at
// ingest from the batch's raw frames, so the heavy jobs never re-read R2.
//
// For each minute the batch touches:
//   • steps = calcSteps over that minute's accelerometer magnitude signal.
//   • rr    = beat-to-beat intervals (ms) from the minute's R24 (historical) AND
//             live (0x28 compact HR + R10) records — all hard-gated to 300–2000 ms.
//
// R24 DETECTION mirrors decodeRecord EXACTLY: recType = b[1] === 24 (NOT gated on
// packet type) — so historical R24 RR is never silently dropped.
//
// IDEMPOTENCY: both merge into the minute row with "keep the fuller value" (steps =
// MAX, rr = longer blob), so a re-uploaded batch can't double-count and a fuller
// batch wins.

import { calcSteps, cleanRr } from 'openstrap-analytics'
import { frameAccel, hexToBytes, realtimeRr } from 'openstrap-protocol/ts/live'
import { parse_r24 } from 'openstrap-protocol/ts/records'

export interface MinuteSignal {
  steps: number
  rr: number[]
  // Optical aggregates from wrist-on R24 (running sums + count). RELATIVE raw ADCs;
  // the close path turns red/IR → SpO₂ index and temp → skin-temp index.
  opt_n?: number
  red_sum?: number
  ir_sum?: number
  temp_sum?: number
}

interface AccelFrame { idx: number; ts: number; mags: number[] }
interface OpticalAcc { n: number; red: number; ir: number; temp: number }

/** Build per-minute {steps, rr, optical} from a batch of hex records. Pure; no I/O. */
export function perMinuteSignals(records: string[]): Map<number, MinuteSignal> {
  const accelByMin = new Map<number, Map<string, AccelFrame>>()
  const rrByMin = new Map<number, number[]>()
  const optByMin = new Map<number, OpticalAcc>()

  for (const hex of records) {
    let b: Uint8Array
    try { b = hexToBytes(hex) } catch { continue }
    if (b.length < 2) continue

    // R24 (recType @ b[1] === 24): RR intervals via the protocol decoder.
    if (b[1] === 24 && b.length >= 89) {
      const r = parse_r24(b)
      if (r && r.ts_epoch > 0) {
        const m = Math.floor(r.ts_epoch / 60) * 60
        const arr = rrByMin.get(m) ?? []
        for (const v of r.rr_intervals_ms) arr.push(v) // raw; gated by analytics cleanRr below
        rrByMin.set(m, arr)
        // Optical: wrist-on only (hr>0 — off-wrist optical is meaningless). Sum red/IR/temp
        // raw ADCs + count → per-minute means at the close, baseline-relative index there.
        if (r.hr > 0 && r.spo2_ir_raw > 0) {
          const o = optByMin.get(m) ?? { n: 0, red: 0, ir: 0, temp: 0 }
          o.n += 1; o.red += r.spo2_red_raw; o.ir += r.spo2_ir_raw; o.temp += r.skin_temp_raw
          optByMin.set(m, o)
        }
      }
      continue
    }

    // LIVE RR (un-banned: unit confirmed ms, cross-validated vs reference implementation). 0x28
    // compact HR + R10 carry beat-to-beat intervals; gated by analytics cleanRr below,
    // so an unvalidated 0x28 offset can only drop values, never store a bogus interval.
    const rr = realtimeRr(hex)
    if (rr) {
      const m = Math.floor(rr.ts / 60) * 60
      const arr = rrByMin.get(m) ?? []
      for (const v of rr.rr_ms) arr.push(v) // raw; cleaned below
      rrByMin.set(m, arr)
    }

    // Accel-bearing frames (0x33 IMU stream, R10) → magnitude samples for the pedometer.
    const f = frameAccel(hex)
    if (f && f.ts > 0) {
      const m = Math.floor(f.ts / 60) * 60
      let mm = accelByMin.get(m)
      if (!mm) { mm = new Map(); accelByMin.set(m, mm) }
      mm.set(`${f.ts}:${f.idx}`, { ts: f.ts, idx: f.idx, mags: f.mags })
    }
  }

  const out = new Map<number, MinuteSignal>()
  const minutes = new Set<number>([...accelByMin.keys(), ...rrByMin.keys(), ...optByMin.keys()])
  for (const m of minutes) {
    let steps = 0
    const frames = accelByMin.get(m)
    if (frames && frames.size > 0) {
      const ordered = [...frames.values()].sort((a, b) => a.ts - b.ts || a.idx - b.idx)
      const sig: number[] = []
      for (const fr of ordered) for (const v of fr.mags) sig.push(v)
      steps = calcSteps([sig])
    }
    // Single library gate (300–2000 ms + ectopic |Δ|>200ms drop) — logic lives in
    // analytics, not duplicated here.
    const o = optByMin.get(m)
    out.set(m, {
      steps, rr: cleanRr(rrByMin.get(m) ?? []),
      ...(o ? { opt_n: o.n, red_sum: o.red, ir_sum: o.ir, temp_sum: o.temp } : {}),
    })
  }
  return out
}

/** Encode an RR list (ms) to a compact little-endian int16 blob. */
export function encodeRr(rr: number[]): Uint8Array | null {
  if (!rr.length) return null
  const buf = new Uint8Array(rr.length * 2)
  const view = new DataView(buf.buffer)
  for (let i = 0; i < rr.length; i++) view.setInt16(i * 2, Math.max(0, Math.min(32767, Math.round(rr[i]))), true)
  return buf
}

/** Decode a minute.rr blob back to an RR list (ms). */
export function decodeRr(blob: ArrayBuffer | Uint8Array | null | undefined): number[] {
  if (!blob) return []
  const u8 = blob instanceof Uint8Array ? blob : new Uint8Array(blob)
  const view = new DataView(u8.buffer, u8.byteOffset, u8.byteLength)
  const out: number[] = []
  for (let i = 0; i + 2 <= u8.byteLength; i += 2) out.push(view.getInt16(i, true))
  return out
}
