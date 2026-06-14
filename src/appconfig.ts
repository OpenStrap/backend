// appconfig.ts — a single admin-controlled config row (id = 1) holding two things:
//   1. OTA update pointer  — latest version/build + signed-APK URL + a mandatory
//      floor (min_build). The app polls this to self-update on Android.
//   2. Home-screen alert banner — a message the admin can push to every client
//      (level info|warn|critical, optional tap-through URL).
//
// GET /app/status is PUBLIC (no JWT): OTA + a "service down" notice must reach
// clients even when their session is expired. Writes are admin-token-gated.

// Columns the admin may set via POST /admin/config (whitelist — nothing else).
const SETTABLE = [
  'latest_version', 'latest_build', 'apk_url', 'release_notes', 'min_build',
  'banner_active', 'banner_id', 'banner_title', 'banner_text', 'banner_level',
  'banner_action_url',
] as const

async function readConfig(db: D1Database): Promise<any> {
  return db.prepare('SELECT * FROM app_config WHERE id = 1').first<any>()
}

/** GET /app/status (public) → { update, banner } for the app to act on. */
export async function getAppStatus(c: any) {
  const row = await readConfig(c.env.DB)
  if (!row) return c.json({ update: null, banner: null })

  const update = row.latest_build != null
    ? {
        latest_version: row.latest_version ?? null,
        latest_build: row.latest_build,
        apk_url: row.apk_url ?? null,
        notes: row.release_notes ?? null,
        min_build: row.min_build ?? 0, // clients below this MUST update
      }
    : null

  const banner = row.banner_active
    ? {
        id: row.banner_id || String(row.updated_at ?? ''), // stable key for client-side dismiss
        title: row.banner_title ?? null,
        text: row.banner_text ?? '',
        level: row.banner_level ?? 'info',     // info | warn | critical
        action_url: row.banner_action_url ?? null,
      }
    : null

  return c.json({ update, banner })
}

/** GET /admin/config → the raw row (admin convenience). */
export async function adminGetConfig(c: any) {
  return c.json((await readConfig(c.env.DB)) ?? {})
}

/**
 * POST /admin/config — partial upsert. Send only the fields you want to change,
 * e.g. {"banner_active":1,"banner_text":"Heads up: maintenance at 2am","banner_level":"warn"}
 * or {"latest_version":"0.3.0","latest_build":4,"apk_url":"https://…","min_build":3}.
 * To clear the banner: {"banner_active":0}.
 */
export async function adminSetConfig(c: any) {
  const body: Record<string, any> = await c.req.json().catch(() => ({}))
  // Ensure the singleton row exists before we UPDATE it.
  await c.env.DB.prepare('INSERT OR IGNORE INTO app_config (id) VALUES (1)').run()

  const sets: string[] = []
  const vals: any[] = []
  for (const key of SETTABLE) {
    if (key in body) { sets.push(`${key} = ?`); vals.push(body[key] ?? null) }
  }
  sets.push('updated_at = ?'); vals.push(Math.floor(Date.now() / 1000))
  await c.env.DB.prepare(`UPDATE app_config SET ${sets.join(', ')} WHERE id = 1`).bind(...vals).run()

  return c.json(await readConfig(c.env.DB))
}
