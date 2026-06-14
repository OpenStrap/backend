// steps_imu.ts — honest step counting from the wrist IMU, re-decoded from the raw
// frames in R2. The band has NO pedometer field, so steps are derived from the
// accelerometer with the published AN-2554 (ADXL367) time-domain pedometer:
//   sum-of-abs accel → 4-tap low-pass → centered-window max/min peak detection →
//   dynamic threshold ± sensitivity/2 → 8 consecutive "possible steps" to confirm
//   (the regularity gate that rejects waving/typing/handling — validated to read 0
//   at rest). Params scaled to our ~100 Hz IMU; a calibration gain corrects the
//   normal ~10% wrist undercount (locked against a 100-step ground-truth walk).
//
// IMU arrives on TWO channels (whichever the strap is in): R10 (pkt 0x2B/0x2F,
// rec 0x0A — 100 accel samples/axis @85/285/485) and the 0x33 IMU stream (10
// accel + 10 gyro samples/frame: X[0:10],Y[10:20],Z[20:30] from offset 24, frame
// index @14, 10 frames/s). Both are 1/4096 g and ~100 Hz once assembled.
//
// Heavy (R2 reads) → cron / admin only, NEVER inline ingest. Owns daily.steps
// (written AFTER analytics in the cron so it's authoritative).

const DAY = 86400

interface StepsEnv { DB: D1Database; RAW_BUCKET: R2Bucket }

// ── locked AN-2554 parameters (calibrated on a 100-step ground-truth walk) ──
const FS = 100            // assembled IMU sample rate (Hz)
const FILTER = 8          // low-pass moving-average taps
const WINDOW = 33         // centered peak window (~0.33 s @100 Hz)
const SENS = 0.10         // g — dead-zone around the dynamic threshold
const THR_ORDER = 4       // dynamic-threshold smoothing buffer
const CONFIRM = 8         // consecutive possible steps before counting (rejects non-gait)
const MAXMIN_TIMEOUT = 120 // samples to find a min after a max (~1.2 s)
const GAIN = 1.11         // calibration: raw 90 → ~100 on the ground-truth walk

const hexToBytes = (hex: string): Uint8Array => {
  const m = hex.trim().match(/.{1,2}/g)
  return m ? new Uint8Array(m.map((b) => parseInt(b, 16))) : new Uint8Array(0)
}

// Decode one frame's accel into ordered magnitude samples. Returns {ts, idx, mags}
// (idx = sub-second frame order; 0 for R10) or null if not an accel frame.
function frameAccel(hex: string): { ts: number; idx: number; mags: number[] } | null {
  const b = hexToBytes(hex)
  if (b.length < 32) return null
  const view = new DataView(b.buffer, b.byteOffset, b.byteLength)
  const pkt = b[0], rec = b[1]
  // 0x33 IMU stream: ts@4, idx@14, 10 accel samples (X[0:10],Y,Z) from offset 24.
  if (pkt === 0x33 && b.length >= 84) {
    const ts = view.getUint32(4, true)
    const idx = view.getUint16(14, true)
    const mags: number[] = []
    for (let i = 0; i < 10; i++) {
      const x = view.getInt16(24 + 2 * i, true)
      const y = view.getInt16(24 + 2 * (10 + i), true)
      const z = view.getInt16(24 + 2 * (20 + i), true)
      mags.push(Math.sqrt(x * x + y * y + z * z) / 4096)
    }
    return ts > 0 ? { ts, idx, mags } : null
  }
  // R10: rec 0x0A, ts@7, accel X@85/Y@285/Z@485 (100 int16 each).
  if (rec === 0x0a && b.length >= 685) {
    const ts = view.getUint32(7, true)
    const mags: number[] = []
    for (let i = 0; i < 100; i++) {
      const x = view.getInt16(85 + 2 * i, true)
      const y = view.getInt16(285 + 2 * i, true)
      const z = view.getInt16(485 + 2 * i, true)
      mags.push(Math.sqrt(x * x + y * y + z * z) / 4096)
    }
    return ts > 0 ? { ts, idx: 0, mags } : null
  }
  return null
}

// AN-2554 time-domain pedometer over one contiguous magnitude signal.
function pedometer(sig: number[]): number {
  const n = sig.length
  if (n < WINDOW) return 0
  // low-pass: trailing moving average
  const lp = new Array<number>(n)
  let acc = 0
  for (let i = 0; i < n; i++) {
    acc += sig[i]
    if (i >= FILTER) acc -= sig[i - FILTER]
    lp[i] = acc / Math.min(i + 1, FILTER)
  }
  const half = WINDOW >> 1
  // centered-window extrema candidates
  const cand: { i: number; max: boolean; v: number }[] = []
  for (let i = half; i < n - half; i++) {
    let isMax = true, isMin = true
    const v = lp[i]
    for (let j = i - half; j <= i + half; j++) {
      if (lp[j] > v) isMax = false
      if (lp[j] < v) isMin = false
      if (!isMax && !isMin) break
    }
    if (isMax) cand.push({ i, max: true, v })
    else if (isMin) cand.push({ i, max: false, v })
  }
  // dynamic threshold + 8-step regularity
  const dyn: number[] = []
  let dynVal = sig.reduce((s, v) => s + v, 0) / n
  let steps = 0, poss = 0, regulation = false
  let state: 'max' | 'min' = 'max'
  let curMax = 0, curMaxIdx = -1
  for (const c of cand) {
    if (state === 'max') {
      if (c.max) { curMax = c.v; curMaxIdx = c.i; state = 'min' }
    } else {
      if (c.max) { if (c.v > curMax) { curMax = c.v; curMaxIdx = c.i } continue }
      if (c.i - curMaxIdx > MAXMIN_TIMEOUT) { state = 'max'; poss = 0; regulation = false; continue }
      const mx = curMax, mn = c.v
      if (mx > dynVal + SENS / 2 && mn < dynVal - SENS / 2) {
        if (mx - mn > SENS) { dyn.push((mx + mn) / 2); if (dyn.length > THR_ORDER) dyn.shift(); dynVal = dyn.reduce((s, v) => s + v, 0) / dyn.length }
        poss++
        if (regulation) steps++
        else if (poss >= CONFIRM) { steps += poss; regulation = true }
      } else { poss = 0; regulation = false }
      state = 'max'
    }
  }
  return steps
}

async function rawKeysInWindow(bucket: R2Bucket, userId: string, from: number, to: number): Promise<string[]> {
  const out: string[] = []
  let cursor: string | undefined
  do {
    const listing = await bucket.list({ prefix: `raw/${userId}/`, cursor, limit: 1000 })
    for (const o of listing.objects) {
      const m = o.key.match(/-(\d+)-(\d+)\.txt$/)
      if (!m) { out.push(o.key); continue }
      if (parseInt(m[2]) >= from && parseInt(m[1]) <= to) out.push(o.key)
    }
    cursor = listing.truncated ? listing.cursor : undefined
  } while (cursor)
  return out
}

// Steps for one UTC day: assemble per-minute accel (ordered by ts,idx), run the
// pedometer per minute, sum, apply the calibration gain.
async function computeDaySteps(env: StepsEnv, userId: string, dayStart: number): Promise<number> {
  const from = dayStart, to = dayStart + DAY
  const keys = await rawKeysInWindow(env.RAW_BUCKET, userId, from, to)
  // DEDUP by (ts, idx): R2 upload windows overlap, so the same frame appears in
  // multiple objects — without this, samples (and steps) are double-counted.
  const seen = new Map<string, { ts: number; idx: number; mags: number[] }>()
  for (const key of keys) {
    const obj = await env.RAW_BUCKET.get(key)
    if (!obj) continue
    const text = await obj.text()
    for (const line of text.split('\n')) {
      if (!line) continue
      const f = frameAccel(line)
      if (!f || f.ts < from || f.ts >= to) continue
      seen.set(`${f.ts}:${f.idx}`, f)
    }
  }
  const byMin = new Map<number, { ts: number; idx: number; mags: number[] }[]>()
  for (const f of seen.values()) {
    const m = Math.floor(f.ts / 60) * 60
    const arr = byMin.get(m); if (arr) arr.push(f); else byMin.set(m, [f])
  }
  let total = 0
  for (const frames of byMin.values()) {
    frames.sort((a, b) => a.ts - b.ts || a.idx - b.idx)
    const sig: number[] = []
    for (const f of frames) for (const v of f.mags) sig.push(v)
    total += pedometer(sig)
  }
  return Math.round(total * GAIN)
}

/**
 * runStepsImu — recompute steps for the last `days` UTC days from R2 IMU and store
 * on daily.steps. Authoritative source of steps (analytics no longer needs to be).
 */
export async function runStepsImu(env: StepsEnv, userId: string, days = 1): Promise<{ days: number; total: number }> {
  const now = Math.floor(Date.now() / 1000)
  let grand = 0
  for (let d = 0; d < days; d++) {
    const dayStart = Math.floor((now - d * DAY) / DAY) * DAY
    const date = new Date(dayStart * 1000).toISOString().slice(0, 10)
    const steps = await computeDaySteps(env, userId, dayStart)
    await env.DB.prepare('INSERT OR IGNORE INTO daily(user_id, date) VALUES(?,?)').bind(userId, date).run()
    await env.DB.prepare('UPDATE daily SET steps = ? WHERE user_id = ? AND date = ?')
      .bind(steps, userId, date).run()
    grand += steps
  }
  return { days, total: grand }
}
