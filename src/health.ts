// health.ts — OPT-IN, separately-consented health-data contribution + withdrawal.
//
// When the user turns on "share my health data" (the second onboarding toggle),
// the app uploads its ENTIRE local .db (raw 1 Hz + derived) here. The blob goes to
// R2 (HEALTH_BUCKET) at health/{device_id}/{ts}.db(.gz); D1 holds only the index
// row + integrity metadata. Anchored to the anonymous device_id (user_id when
// signed in). This is a DISTINCT consent scope from telemetry — sharing crash
// logs never implies sharing health data.
//
// Metadata rides query params (the body is the raw binary blob):
//   POST /health/upload?device_id=&user_id=&consent_version=&sha256=&gz=1&app_version=
//
// There is intentionally NO public deletion endpoint: these are anonymous device
// ids with no verifiable owner, so an open delete keyed on a guessable id would be
// a spam-wipe vector. Erasure, if ever needed, must be admin-gated.

import type { Context } from 'hono'

type Bindings = {
  DB: D1Database
  HEALTH_BUCKET: R2Bucket
  RATE_LIMITER?: { limit(opts: { key: string }): Promise<{ success: boolean }> }
}
type Ctx = Context<{ Bindings: Bindings; Variables: Record<string, never> }>

const nowSec = () => Math.floor(Date.now() / 1000)
const str = (v: any): string | null => (typeof v === 'string' && v.length ? v : null)
const int = (v: any): number | null => {
  const n = typeof v === 'string' ? parseInt(v) : v
  return typeof n === 'number' && isFinite(n) ? Math.round(n) : null
}

// 100 MB ceiling — a full multi-month .db is a few MB gzipped; this is abuse-only.
const MAX_BYTES = 100 * 1024 * 1024

async function isRevoked(db: D1Database, deviceId: string, scope: string): Promise<boolean> {
  const row = await db.prepare(
    'SELECT granted FROM consents WHERE device_id = ? AND scope = ?',
  ).bind(deviceId, scope).first<{ granted: number }>()
  return row != null && !row.granted
}

// POST /health/upload — store the uploaded .db blob in R2 + index it in D1.
export async function postHealthUpload(c: Ctx) {
  const deviceId = str(c.req.query('device_id'))
  if (!deviceId) return c.json({ error: 'device_id required' }, 400)

  if (c.env.RATE_LIMITER) {
    const { success } = await c.env.RATE_LIMITER.limit({ key: `hu:${deviceId}` })
    if (!success) return c.json({ error: 'rate limited' }, 429)
  }
  if (await isRevoked(c.env.DB, deviceId, 'health_data')) {
    return c.json({ error: 'health-data sharing not consented' }, 403)
  }

  const body = await c.req.arrayBuffer()
  if (!body || body.byteLength === 0) return c.json({ error: 'empty body' }, 400)
  if (body.byteLength > MAX_BYTES) return c.json({ error: 'too large' }, 413)

  const ts = nowSec()
  const gz = c.req.query('gz') === '1'
  const key = `health/${deviceId}/${ts}.db${gz ? '.gz' : ''}`

  // FREE-TIER GUARD: the .db is cumulative — the newest upload supersedes every
  // older one — so keep only the latest per device. This bounds R2 storage to
  // ~(active devices × one .db) instead of growing unbounded past the 10 GB free
  // limit, and avoids paid overage. Drop the device's prior blobs + index rows.
  const prior = await c.env.DB.prepare(
    'SELECT r2_key FROM health_uploads WHERE device_id = ?',
  ).bind(deviceId).all<{ r2_key: string }>()
  const priorKeys = (prior.results ?? []).map((r) => r.r2_key)
  if (priorKeys.length) {
    await c.env.HEALTH_BUCKET.delete(priorKeys)
    await c.env.DB.prepare('DELETE FROM health_uploads WHERE device_id = ?').bind(deviceId).run()
  }

  await c.env.HEALTH_BUCKET.put(key, body, {
    httpMetadata: { contentType: gz ? 'application/gzip' : 'application/octet-stream' },
    customMetadata: {
      device_id: deviceId,
      user_id: str(c.req.query('user_id')) ?? '',
      sha256: str(c.req.query('sha256')) ?? '',
    },
  })
  await c.env.DB.prepare(
    'INSERT INTO health_uploads (device_id, user_id, r2_key, bytes, sha256, app_version, consent_version, ts, created_at) ' +
    'VALUES (?,?,?,?,?,?,?,?,?)',
  ).bind(
    deviceId, str(c.req.query('user_id')), key, body.byteLength,
    str(c.req.query('sha256')), str(c.req.query('app_version')),
    int(c.req.query('consent_version')), ts, ts,
  ).run()

  return c.json({ ok: true, key, bytes: body.byteLength, ts })
}
