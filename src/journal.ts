// journal.ts — daily behavior tags + a transparent correlation engine.
//
// The user tags days (caffeine, alcohol, late_meal, stress, travel, screens,
// meds, sick, …) and optionally writes a note. /journal/insights then reports,
// per tag, how the user's OWN metrics differ on tagged vs untagged days —
// honest descriptive stats (mean delta + n), never causal claims.
//
//   POST /journal            {date:'YYYY-MM-DD', tags:[...], note?}  → upsert
//   GET  /journal?range=30d                                          → rows
//   GET  /journal/insights?range=90d                                 → correlations
// All JWT, scoped by user_id.

import type { Context } from 'hono'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

const DAY = 86400
const RANGES: Record<string, number> = { '7d': 7, '30d': 30, '90d': 90, '365d': 365 }

// Metrics we correlate tags against (column, label, lower-is-better?).
const CORR_METRICS: { col: string; label: string; lowerBetter: boolean; table: 'daily' | 'sleep' }[] = [
  { col: 'resting_hr', label: 'Resting HR', lowerBetter: true, table: 'daily' },
  { col: 'readiness', label: 'Readiness', lowerBetter: false, table: 'daily' },
  { col: 'strain', label: 'Strain', lowerBetter: false, table: 'daily' },
  { col: 'efficiency', label: 'Sleep efficiency', lowerBetter: false, table: 'sleep' },
  { col: 'duration_min', label: 'Sleep duration', lowerBetter: false, table: 'sleep' },
]

const startDate = (days: number) =>
  new Date((Math.floor(Date.now() / 1000) - days * DAY) * 1000).toISOString().slice(0, 10)

const isDate = (s: unknown): s is string =>
  typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s)

/** Upsert a day's tags + note. Empty tags + empty note deletes the entry. */
export async function postJournal(c: Ctx) {
  const userId = c.get('userId')
  const body = await c.req.json<{ date?: string; tags?: unknown; note?: string }>()
  if (!isDate(body.date)) return c.json({ error: 'date must be YYYY-MM-DD' }, 400)
  const tags = Array.isArray(body.tags)
    ? body.tags.map((t) => String(t).toLowerCase().trim()).filter(Boolean).slice(0, 24)
    : []
  const note = (body.note ?? '').toString().slice(0, 1000)
  if (tags.length === 0 && note.trim() === '') {
    await c.env.DB.prepare('DELETE FROM journal WHERE user_id = ? AND date = ?')
      .bind(userId, body.date).run()
    return c.json({ ok: true, deleted: true })
  }
  await c.env.DB.prepare(
    'INSERT INTO journal (user_id, date, tags, note, updated_at) VALUES (?,?,?,?,?) ' +
    'ON CONFLICT(user_id, date) DO UPDATE SET tags=excluded.tags, note=excluded.note, updated_at=excluded.updated_at',
  ).bind(userId, body.date, JSON.stringify(tags), note, Math.floor(Date.now() / 1000)).run()
  return c.json({ ok: true, date: body.date, tags, note })
}

/** List journal rows over a range (newest first). */
export async function getJournal(c: Ctx) {
  const userId = c.get('userId')
  const days = RANGES[(c.req.query('range') || '30d').toLowerCase()] ?? 30
  const { results } = await c.env.DB.prepare(
    'SELECT date, tags, note FROM journal WHERE user_id = ? AND date >= ? ORDER BY date DESC',
  ).bind(userId, startDate(days)).all<any>()
  return c.json((results ?? []).map((r: any) => ({
    date: r.date,
    tags: safeTags(r.tags),
    note: r.note ?? '',
  })))
}

function safeTags(s: any): string[] {
  if (Array.isArray(s)) return s
  try { const v = JSON.parse(s); return Array.isArray(v) ? v : [] } catch { return [] }
}

const mean = (xs: number[]) => xs.reduce((s, v) => s + v, 0) / xs.length

/**
 * Correlation insights: for each tag seen on ≥3 days, compare the mean of each
 * metric on tagged vs untagged days within the window. Reports the delta, its
 * direction (better/worse for the user), and n. Descriptive only.
 */
export async function getJournalInsights(c: Ctx) {
  const userId = c.get('userId')
  const days = RANGES[(c.req.query('range') || '90d').toLowerCase()] ?? 90
  const since = startDate(days)

  const { results: jrows } = await c.env.DB.prepare(
    'SELECT date, tags FROM journal WHERE user_id = ? AND date >= ?',
  ).bind(userId, since).all<any>()
  const tagsByDate = new Map<string, string[]>()
  const tagDayCount = new Map<string, number>()
  for (const r of jrows ?? []) {
    const tags = safeTags(r.tags)
    tagsByDate.set(r.date, tags)
    for (const t of new Set(tags)) tagDayCount.set(t, (tagDayCount.get(t) ?? 0) + 1)
  }

  const { results: daily } = await c.env.DB.prepare(
    'SELECT date, resting_hr, recovery AS readiness, strain FROM daily WHERE user_id = ? AND date >= ?',
  ).bind(userId, since).all<any>()
  const { results: sleep } = await c.env.DB.prepare(
    'SELECT date, efficiency, duration_min FROM sleep WHERE user_id = ? AND date >= ?',
  ).bind(userId, since).all<any>()
  const dailyByDate = new Map<string, any>()
  for (const r of daily ?? []) dailyByDate.set(r.date, r)
  const sleepByDate = new Map<string, any>()
  for (const r of sleep ?? []) sleepByDate.set(r.date, r)

  const rowFor = (date: string, table: string) =>
    table === 'sleep' ? sleepByDate.get(date) : dailyByDate.get(date)

  const insights: any[] = []
  for (const [tag, dayN] of tagDayCount) {
    if (dayN < 3) continue // not enough signal to say anything
    const effects: any[] = []
    for (const m of CORR_METRICS) {
      const withVals: number[] = []
      const withoutVals: number[] = []
      // Iterate over all dates that have the metric.
      const src = m.table === 'sleep' ? sleepByDate : dailyByDate
      for (const [date, row] of src) {
        const v = row[m.col]
        if (v == null) continue
        const has = (tagsByDate.get(date) ?? []).includes(tag)
        ;(has ? withVals : withoutVals).push(Number(v))
      }
      if (withVals.length < 3 || withoutVals.length < 3) continue
      const a = mean(withVals)
      const b = mean(withoutVals)
      const delta = a - b
      const pct = b !== 0 ? (delta / Math.abs(b)) * 100 : 0
      const better = m.lowerBetter ? delta < 0 : delta > 0
      effects.push({
        metric: m.col,
        label: m.label,
        with_avg: Math.round(a * 100) / 100,
        without_avg: Math.round(b * 100) / 100,
        delta: Math.round(delta * 100) / 100,
        delta_pct: Math.round(pct * 10) / 10,
        better,
        n_with: withVals.length,
      })
    }
    // Surface effects sorted by magnitude; keep tags that produced any.
    effects.sort((x, y) => Math.abs(y.delta_pct) - Math.abs(x.delta_pct))
    if (effects.length > 0) insights.push({ tag, days: dayN, effects })
  }
  insights.sort((a, b) => b.days - a.days)
  return c.json({ range: `${days}d`, insights })
}
