// spotcheck.ts — on-demand live HRV reading.
//
//   POST /spotcheck { records: [hex, ...] }  → { rmssd, sdnn, pnn50, mean_hr, n_beats, ok }
//
// The edge enables wrist-gated optical + realtime records for ~60s, collects the
// raw live frames, and posts them here. We decode beat-to-beat RR from each frame
// (protocol realtimeRr — the SAME decoder the ingest path uses) and compute HRV
// with the analytics time-domain method (cleanRr gate + RMSSD/SDNN/pNN50).
//
// Heavy lifting stays server-side; the client just collects bytes and renders.
// Stateless: nothing is written to D1/R2 (a spot check is ephemeral by design).

import type { Context } from 'hono'
import { realtimeRr } from 'openstrap-protocol/ts/live'
import { timeDomainHrv } from 'openstrap-analytics'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

export async function postSpotCheck(c: Ctx) {
  const body = await c.req.json<{ records?: unknown }>().catch(() => ({ records: [] }))
  const recs = Array.isArray(body.records)
    ? body.records.filter((h): h is string => typeof h === 'string').slice(0, 5000)
    : []
  if (recs.length === 0) return c.json({ error: 'records[] required' }, 400)

  // Decode RR (ms) from every live frame that carries it (0x28 / R10).
  const rr: number[] = []
  for (const hex of recs) {
    try {
      const r = realtimeRr(hex)
      if (r && Array.isArray(r.rr_ms) && r.rr_ms.length) rr.push(...r.rr_ms)
    } catch { /* skip undecodable frame */ }
  }

  const hrv = timeDomainHrv(rr)
  return c.json({
    n_frames: recs.length,
    n_beats: hrv.n_beats,
    rmssd: hrv.rmssd,
    sdnn: hrv.sdnn,
    pnn50: hrv.pnn50,
    mean_hr: hrv.mean_hr,
    // honest gate: timeDomainHrv needs ≥20 clean beats, else everything is null
    ok: hrv.rmssd != null,
  })
}
