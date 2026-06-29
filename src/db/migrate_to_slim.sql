-- migrate_to_slim.sql — transition an EXISTING prod D1 to the slim companion schema.
--
-- Run once against the live v2 DB. It drops the entire heavy pipeline's storage,
-- adds the Terms columns to app_config, and creates the new consent/telemetry/
-- health tables. The KEEP tables (users, otps, refresh_tokens, app_config, daily,
-- sleep, sessions, baselines) are left intact so existing users can still IMPORT.
--
-- Fresh deploys should just run schema.sql instead (it already has the slim shape).
-- Idempotent where SQLite allows (DROP IF EXISTS / CREATE IF NOT EXISTS); the
-- ALTERs are guarded-by-convention (skip if you've already run this once).

-- ── drop the heavy pipeline's tables ──────────────────────────────────────────
DROP TABLE IF EXISTS minute_day;
DROP TABLE IF EXISTS minute_hot;
DROP TABLE IF EXISTS minute;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS read_cache;
DROP TABLE IF EXISTS analytics_cursor;
DROP TABLE IF EXISTS rate_limit;
DROP TABLE IF EXISTS journal;
DROP TABLE IF EXISTS cycle_log;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS sleep_periods;  -- import uses /sleep (single period); v2 endpoint is gone

-- ── extend app_config with the Terms/Privacy pointer ──────────────────────────
-- (Run only on first migration; SQLite errors if the column already exists — that
-- error is harmless, it means this migration already applied.)
ALTER TABLE app_config ADD COLUMN terms_version INTEGER DEFAULT 1;
ALTER TABLE app_config ADD COLUMN terms_url TEXT;
ALTER TABLE app_config ADD COLUMN privacy_url TEXT;
ALTER TABLE app_config ADD COLUMN terms_summary TEXT;

-- ── new tables (consent + opt-in ingest) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS consents(
  device_id     TEXT NOT NULL,
  scope         TEXT NOT NULL,
  granted       INTEGER NOT NULL,
  terms_version INTEGER,
  user_id       TEXT,
  updated_at    INTEGER NOT NULL,
  PRIMARY KEY(device_id, scope)
);

CREATE TABLE IF NOT EXISTS telemetry(
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id       TEXT NOT NULL,
  user_id         TEXT,
  kind            TEXT NOT NULL,
  level           TEXT,
  message         TEXT,
  stacktrace      TEXT,
  context         TEXT,
  app_version     TEXT,
  app_build       INTEGER,
  platform        TEXT,
  os_version      TEXT,
  oem             TEXT,
  model           TEXT,
  ble_state       TEXT,
  battery_pct     INTEGER,
  band_battery_pct INTEGER,
  band_serial     TEXT,
  band_firmware   TEXT,
  consent_version INTEGER,
  ts              INTEGER NOT NULL,
  created_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_telemetry_device ON telemetry(device_id, created_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_kind ON telemetry(kind, created_at);

CREATE TABLE IF NOT EXISTS health_uploads(
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id       TEXT NOT NULL,
  user_id         TEXT,
  r2_key          TEXT NOT NULL,
  bytes           INTEGER,
  sha256          TEXT,
  app_version     TEXT,
  consent_version INTEGER,
  ts              INTEGER NOT NULL,
  created_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_health_uploads_device ON health_uploads(device_id, created_at);
