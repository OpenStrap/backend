// rollup.ts — fold decoded samples into per-minute buckets.
//
// Bucket key = floor(ts/60)*60. Per minute:
//   hr_avg/min/max/n over hr>0 samples, mean activity,
//   wrist_on = any sample worn.
//
// The minute upsert merges deterministically with any existing row so that
// re-uploading the same samples converges to the same values (idempotent
// ingest). We store enough to recompute averages exactly: the upsert SQL
// folds new (sum,n) into the stored running aggregate — see ingest.ts.

import type { DecodedSample } from './decode'

export interface MinuteBucket {
  ts_min: number
  hr_sum: number   // sum of hr over hr>0 samples (for exact running mean)
  hr_min: number
  hr_max: number
  hr_n: number     // count of hr>0 samples
  act_sum: number  // sum of activity (for exact running mean over all samples)
  act_n: number    // count of all samples (activity contributors)
  steps: number    // summed detected steps this minute
  wrist_on: number // 0/1
}

/**
 * Fold decoded samples into per-minute buckets. Returns one MinuteBucket per
 * distinct minute, with running sums (not yet averaged) so the D1 upsert can
 * merge with stored aggregates deterministically.
 */
export function rollupMinutes(samples: DecodedSample[]): MinuteBucket[] {
  const buckets = new Map<number, MinuteBucket>()
  for (const s of samples) {
    if (!Number.isFinite(s.ts) || s.ts <= 0) continue
    const ts_min = Math.floor(s.ts / 60) * 60
    let bk = buckets.get(ts_min)
    if (!bk) {
      bk = { ts_min, hr_sum: 0, hr_min: 0, hr_max: 0, hr_n: 0, act_sum: 0, act_n: 0, steps: 0, wrist_on: 0 }
      buckets.set(ts_min, bk)
    }
    if (s.hr > 0) {
      bk.hr_sum += s.hr
      bk.hr_n += 1
      bk.hr_min = bk.hr_min === 0 ? s.hr : Math.min(bk.hr_min, s.hr)
      bk.hr_max = Math.max(bk.hr_max, s.hr)
    }
    bk.act_sum += s.activity
    bk.act_n += 1
    bk.steps += s.steps_inc
    if (s.wrist_on) bk.wrist_on = 1
  }
  return [...buckets.values()].sort((a, b) => a.ts_min - b.ts_min)
}
