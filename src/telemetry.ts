// telemetry.ts — OPT-IN, consent-gated observability ingest + the consent ledger.
//
// The local-first app has no account by default, so these endpoints are anchored
// to an anonymous, app-generated install id (`device_id`), with a `user_id` only
// when the install happens to be signed in (a returning v2 user). No JWT — a
// crash report must land even when nothing is authenticated — but each request is
// rate-limited by device_id and dropped if the device has explicitly revoked
// telemetry consent. NO health metrics flow here; that's the separate
// health_uploads channel (health.ts), with its own consent scope.
//
// Everything is gated client-side by the two toggles on the onboarding consent
// screen; we additionally honor an explicit server-side revoke and stamp every
// row with the terms_version the user agreed to.

import type { Context } from 'hono'

type Bindings = {
  DB: D1Database
  RATE_LIMITER?: { limit(opts: { key: string }): Promise<{ success: boolean }> }
}
type Ctx = Context<{ Bindings: Bindings; Variables: Record<string, never> }>

const nowSec = () => Math.floor(Date.now() / 1000)
const str = (v: any): string | null => (typeof v === 'string' && v.length ? v.slice(0, 8000) : null)
const int = (v: any): number | null => (typeof v === 'number' && isFinite(v) ? Math.round(v) : null)

// A telemetry batch: a current device snapshot applied to every event in the batch.
interface DeviceSnapshot {
  app_version?: string; app_build?: number; platform?: string; os_version?: string
  oem?: string; model?: string; ble_state?: string; battery_pct?: number
  band_battery_pct?: number; band_serial?: string; band_firmware?: string
}
interface TelemetryEvent {
  kind?: string; level?: string; message?: string; stacktrace?: string
  context?: any; ts?: number
}

// True if the device has an explicit `granted = 0` row for [scope] (a deliberate
// opt-out we must honor). Absent row → the client asserts consent in-band.
async function isRevoked(db: D1Database, deviceId: string, scope: string): Promise<boolean> {
  const row = await db.prepare(
    'SELECT granted FROM consents WHERE device_id = ? AND scope = ?',
  ).bind(deviceId, scope).first<{ granted: number }>()
  return row != null && !row.granted
}

// POST /telemetry — ingest a batch of crash/error/device/event records.
export async function postTelemetry(c: Ctx) {
  const body = await c.req.json<{
    device_id?: string; user_id?: string; consent_version?: number
    device?: DeviceSnapshot; events?: TelemetryEvent[]
  }>().catch(() => ({} as any))

  const deviceId = str(body.device_id)
  if (!deviceId) return c.json({ error: 'device_id required' }, 400)

  // Abuse guard (free, edge-local). Best-effort: missing binding → allow.
  if (c.env.RATE_LIMITER) {
    const { success } = await c.env.RATE_LIMITER.limit({ key: `tlm:${deviceId}` })
    if (!success) return c.json({ error: 'rate limited' }, 429)
  }

  // Honor an explicit revoke (silent success — the client may be mid-flush).
  if (await isRevoked(c.env.DB, deviceId, 'telemetry')) return c.json({ ok: true, stored: 0 })

  const events: TelemetryEvent[] = Array.isArray(body.events) ? body.events.slice(0, 50) : []
  if (events.length === 0) return c.json({ ok: true, stored: 0 })

  const d = body.device ?? {}
  const userId = str(body.user_id)
  const cv = int(body.consent_version)
  const recv = nowSec()

  const stmt = c.env.DB.prepare(
    'INSERT INTO telemetry (device_id, user_id, kind, level, message, stacktrace, context, ' +
    'app_version, app_build, platform, os_version, oem, model, ble_state, battery_pct, ' +
    'band_battery_pct, band_serial, band_firmware, consent_version, ts, created_at) ' +
    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
  )
  const batch = events.map((e) => stmt.bind(
    deviceId, userId,
    str(e.kind) ?? 'event', str(e.level), str(e.message), str(e.stacktrace),
    e.context != null ? JSON.stringify(e.context).slice(0, 16000) : null,
    str(d.app_version), int(d.app_build), str(d.platform), str(d.os_version),
    str(d.oem), str(d.model), str(d.ble_state), int(d.battery_pct),
    int(d.band_battery_pct), str(d.band_serial), str(d.band_firmware),
    cv, int(e.ts) ?? recv, recv,
  ))
  await c.env.DB.batch(batch)
  return c.json({ ok: true, stored: batch.length })
}

// POST /consent — record a grant/revoke for a consent scope (the onboarding
// toggles call this). Upsert so the latest state always wins.
export async function postConsent(c: Ctx) {
  const body = await c.req.json<{
    device_id?: string; scope?: string; granted?: boolean | number
    terms_version?: number; user_id?: string
  }>().catch(() => ({} as any))

  const deviceId = str(body.device_id)
  const scope = str(body.scope)
  if (!deviceId || (scope !== 'telemetry' && scope !== 'health_data')) {
    return c.json({ error: 'device_id and scope (telemetry|health_data) required' }, 400)
  }
  const granted = body.granted ? 1 : 0
  await c.env.DB.prepare(
    'INSERT INTO consents (device_id, scope, granted, terms_version, user_id, updated_at) ' +
    'VALUES (?,?,?,?,?,?) ON CONFLICT(device_id, scope) DO UPDATE SET ' +
    'granted=excluded.granted, terms_version=excluded.terms_version, ' +
    'user_id=COALESCE(excluded.user_id, consents.user_id), updated_at=excluded.updated_at',
  ).bind(deviceId, scope, granted, int(body.terms_version), str(body.user_id), nowSec()).run()
  return c.json({ ok: true, scope, granted: !!granted })
}
