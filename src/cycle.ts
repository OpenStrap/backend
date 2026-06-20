// cycle.ts — menstrual cycle tracking endpoints.
//
//   POST   /cycle/log   {date:'YYYY-MM-DD', kind?:'start'|'end'|'spotting', note?}  → upsert
//   DELETE /cycle/log?date=YYYY-MM-DD                                               → remove
//   GET    /cycle                                                                   → prediction + logs + biometric overlay
//
// The prediction (calcCycle) is LOG-ANCHORED — driven by logged period starts,
// never inferred from biometrics. The biometric overlay (skin-temp / RHR / HRV
// across the current cycle) is descriptive enrichment pulled from stored `daily`.
// All JWT, scoped by user_id. Honest: an ESTIMATE, not medical guidance.

import type { Context } from 'hono'
import { calcCycle } from 'openstrap-analytics'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

const isDate = (s: unknown): s is string =>
  typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s)
const today = () => new Date(Date.now()).toISOString().slice(0, 10)
const KINDS = new Set(['start', 'end', 'spotting'])

/** Log (or update) a period event for a date. */
export async function postCycleLog(c: Ctx) {
  const userId = c.get('userId')
  const body = await c.req.json<{ date?: string; kind?: string; note?: string }>()
  if (!isDate(body.date)) return c.json({ error: 'date must be YYYY-MM-DD' }, 400)
  const kind = KINDS.has(body.kind ?? '') ? (body.kind as string) : 'start'
  const note = (body.note ?? '').toString().slice(0, 500)
  await c.env.DB.prepare(
    'INSERT INTO cycle_log (user_id, date, kind, note, updated_at) VALUES (?,?,?,?,?) ' +
    'ON CONFLICT(user_id, date) DO UPDATE SET kind=excluded.kind, note=excluded.note, updated_at=excluded.updated_at',
  ).bind(userId, body.date, kind, note, Math.floor(Date.now() / 1000)).run()
  return c.json({ ok: true, date: body.date, kind, note })
}

/** Remove a logged event. */
export async function deleteCycleLog(c: Ctx) {
  const userId = c.get('userId')
  const date = (c.req.query('date') || '').trim()
  if (!isDate(date)) return c.json({ error: 'date=YYYY-MM-DD required' }, 400)
  await c.env.DB.prepare('DELETE FROM cycle_log WHERE user_id = ? AND date = ?')
    .bind(userId, date).run()
  return c.json({ ok: true, deleted: true })
}

/** Current cycle position + prediction + logged events + biometric overlay.
 *  Only computed for users who explicitly opted in (users.track_cycle). */
export async function getCycle(c: Ctx) {
  const userId = c.get('userId')

  // Consent gate: no cycle computation unless the user turned tracking on.
  const pref = await c.env.DB.prepare('SELECT track_cycle FROM users WHERE id = ?')
    .bind(userId).first<{ track_cycle: number | null }>()
  if (!pref || !pref.track_cycle) {
    return c.json({ enabled: false, note: 'Cycle tracking is off. Enable it in your profile to start.' })
  }

  const logsRes = await c.env.DB.prepare(
    'SELECT date, kind, note FROM cycle_log WHERE user_id = ? ORDER BY date DESC LIMIT 60',
  ).bind(userId).all<{ date: string; kind: string; note: string | null }>()
  const logs = logsRes.results ?? []
  const starts = logs.filter((l) => l.kind === 'start').map((l) => l.date)

  const cycle = calcCycle(starts, today())

  // Biometric overlay for the current cycle (descriptive). Pull stored daily
  // signals from the cycle start onward — cheap point range on (user_id, date).
  let overlay: Array<{ date: string; skin_temp_idx: number | null; resting_hr: number | null; hrv_rmssd: number | null }> = []
  if (cycle.last_start) {
    const ov = await c.env.DB.prepare(
      'SELECT date, skin_temp_idx, resting_hr, hrv_rmssd FROM daily ' +
      'WHERE user_id = ? AND date >= ? ORDER BY date ASC',
    ).bind(userId, cycle.last_start).all<any>()
    overlay = (ov.results ?? []).map((r: any) => ({
      date: r.date,
      skin_temp_idx: r.skin_temp_idx ?? null,
      resting_hr: r.resting_hr ?? null,
      hrv_rmssd: r.hrv_rmssd ?? null,
    }))
  }

  return c.json({ ...cycle, logs, overlay })
}
