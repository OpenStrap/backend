// notifications.ts — read/ack API over the server-generated notification feed.
// The engine (analytics.buildNotifications) writes rows; the client pulls them
// here, presents them (local notification + in-app inbox), and acks reads. All
// JWT, user-scoped. NO push dependency — pull-based, self-host-friendly.

import type { Context } from 'hono'

type Ctx = Context<{ Bindings: { DB: D1Database }; Variables: { userId: string } }>

const DAY = 86400

// GET /notifications — recent feed (last 7 days) + unread count, newest first.
export async function getNotifications(c: Ctx) {
  const userId = c.get('userId')
  const since = Math.floor(Date.now() / 1000) - 7 * DAY
  const { results } = await c.env.DB.prepare(
    'SELECT id, date, kind, category, priority, title, body, window, quiet_ok, created_at, read_at ' +
    'FROM notifications WHERE user_id = ? AND created_at >= ? ' +
    'ORDER BY (read_at IS NOT NULL), priority DESC, created_at DESC LIMIT 50',
  ).bind(userId, since).all<any>()
  const rows = (results ?? []).map((r: any) => ({
    id: r.id, date: r.date, kind: r.kind, category: r.category,
    priority: r.priority, title: r.title, body: r.body, window: r.window,
    quiet_ok: !!r.quiet_ok, created_at: r.created_at,
    read: r.read_at != null,
  }))
  const unread = rows.filter((r) => !r.read).length
  return c.json({ unread, notifications: rows })
}

// POST /notifications/read { ids?: string[] } — mark some (or all) as read.
export async function markNotificationsRead(c: Ctx) {
  const userId = c.get('userId')
  const body = await c.req.json<{ ids?: string[] }>().catch(() => ({} as any))
  const now = Math.floor(Date.now() / 1000)
  if (Array.isArray(body.ids) && body.ids.length > 0) {
    const ids = body.ids.slice(0, 100)
    const ph = ids.map(() => '?').join(',')
    await c.env.DB.prepare(
      `UPDATE notifications SET read_at = ? WHERE user_id = ? AND read_at IS NULL AND id IN (${ph})`,
    ).bind(now, userId, ...ids).run()
  } else {
    await c.env.DB.prepare(
      'UPDATE notifications SET read_at = ? WHERE user_id = ? AND read_at IS NULL',
    ).bind(now, userId).run()
  }
  return c.json({ ok: true })
}
