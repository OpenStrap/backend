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

import { calcSteps, cleanRr, extractHarFeaturesFromSmv, classifyActivityWindow } from 'openstrap-analytics'
import { frameAccel, hexToBytes, realtimeRr } from 'openstrap-protocol/ts/live'
import { parse_r24 } from 'openstrap-protocol/ts/records'

const HAR_FS = 100      // live IMU ≈ 100 Hz (R10 100 samples/s, 0x33 stream)
const HAR_WIN = 400     // 4 s Mannini window
const HAR_MIN = 128     // need ≥~1.3 s for a defensible spectrum

/** Classify a minute's magnitude (SMV) signal into a dominant HAR class via the
 *  Mannini feature extractor + threshold classifier. Returns undefined when there's
 *  too little high-rate accel (e.g. flash 1 Hz minutes carry none). */
function classifyMinute(sig: number[]): string | undefined {
  if (sig.length < HAR_MIN) return undefined
  const counts: Record<string, number> = {}
  for (let i = 0; i + HAR_MIN <= sig.length; i += HAR_WIN) {
    const w = sig.slice(i, Math.min(i + HAR_WIN, sig.length))
    const { cls } = classifyActivityWindow(extractHarFeaturesFromSmv(w, HAR_FS))
    counts[cls] = (counts[cls] ?? 0) + 1
  }
  let best: string | undefined, bestN = -1
  for (const k of Object.keys(counts)) if (counts[k] > bestN) { bestN = counts[k]; best = k }
  return best
}

export interface MinuteSignal {
  steps: number
  rr: number[]
  // Optical aggregates from wrist-on R24 (running sums + count). RELATIVE raw ADCs;
  // the close path turns red/IR → SpO₂ index and temp → skin-temp index.
  opt_n?: number
  red_sum?: number
  ir_sum?: number
  temp_sum?: number
  // PPG RIIV proxy: per-second mean of the R21 green channel, time-ordered. The ONLY
  // value estimateResp consumes — so storing it lets resp compute from D1 at the close
  // with no raw R2. Present only during live optical sessions (R21 is live-only).
  green?: number[]
  // Dominant HAR activity class for the minute, from the live high-rate accel (Mannini
  // classifier). One tiny label — drives workout typing. Live minutes only.
  act_class?: string
}

interface AccelFrame { idx: number; ts: number; mags: number[] }
interface OpticalAcc { n: number; red: number; ir: number; temp: number }

// One R21 record's green PPG channel → per-second mean (RIIV proxy). Mirrors
// resp.ts decodeR21Green: rec_type 21, ts@7 (u32 LE), channel A @20 (100×u16 LE).
function r21GreenMean(b: Uint8Array): { ts: number; mean: number } | null {
  if (b.length < 620 || b[1] !== 21) return null
  const view = new DataView(b.buffer, b.byteOffset, b.byteLength)
  const ts = view.getUint32(7, true)
  if (!(ts > 0)) return null
  let sum = 0, n = 0
  for (let i = 0; i < 100; i++) {
    const o = 20 + 2 * i
    if (o + 2 <= b.length) { sum += view.getUint16(o, true); n++ }
  }
  return n >= 50 ? { ts, mean: sum / n } : null
}

/** Build per-minute {steps, rr, optical} from a batch of hex records. Pure; no I/O. */
export function perMinuteSignals(records: string[]): Map<number, MinuteSignal> {
  const accelByMin = new Map<number, Map<string, AccelFrame>>()
  const rrByMin = new Map<number, number[]>()
  const optByMin = new Map<number, OpticalAcc>()
  const greenByMin = new Map<number, { ts: number; v: number }[]>()

  for (const hex of records) {
    let b: Uint8Array
    try { b = hexToBytes(hex) } catch { continue }
    if (b.length < 2) continue

    // R21 PPG (rec_type 21): per-second mean green → RIIV proxy for resp (D1, no R2).
    const g = r21GreenMean(b)
    if (g) {
      const m = Math.floor(g.ts / 60) * 60
      const arr = greenByMin.get(m) ?? []
      arr.push({ ts: g.ts, v: g.mean })
      greenByMin.set(m, arr)
      continue
    }

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
  const minutes = new Set<number>([...accelByMin.keys(), ...rrByMin.keys(), ...optByMin.keys(), ...greenByMin.keys()])
  for (const m of minutes) {
    let steps = 0
    let act_class: string | undefined
    const frames = accelByMin.get(m)
    if (frames && frames.size > 0) {
      const ordered = [...frames.values()].sort((a, b) => a.ts - b.ts || a.idx - b.idx)
      const sig: number[] = []
      for (const fr of ordered) for (const v of fr.mags) sig.push(v)
      steps = calcSteps([sig])
      // Classify the minute's motion at ingest (the ONLY place the high-rate accel
      // exists) → store just the dominant class label, never the raw samples.
      act_class = classifyMinute(sig)
    }
    // PPG green: time-ordered per-second means for the minute (RIIV series → resp at close).
    const gArr = greenByMin.get(m)
    const green = gArr && gArr.length
      ? gArr.sort((a, b) => a.ts - b.ts).map((x) => x.v)
      : undefined
    // Single library gate (300–2000 ms + ectopic |Δ|>200ms drop) — logic lives in
    // analytics, not duplicated here.
    const o = optByMin.get(m)
    out.set(m, {
      steps, rr: cleanRr(rrByMin.get(m) ?? []),
      ...(o ? { opt_n: o.n, red_sum: o.red, ir_sum: o.ir, temp_sum: o.temp } : {}),
      ...(green ? { green } : {}),
      ...(act_class ? { act_class } : {}),
    })
  }
  return out
}
