-- openstrap-backend D1 schema — SLIM companion build (local-first era).
--
-- The OpenStrap app is now local-first: all decode + analytics happen on-device.
-- This backend is a thin companion that only:
--   1. serves the OTA update pointer + an admin announcement banner (app_config),
--   2. authenticates returning v2 users (OTP + JWT) so they can IMPORT their old
--      cloud-derived history (the read-only `users`/`daily`/`sleep`/`sessions`
--      tables, populated by the legacy pipeline — never written to anymore),
--   3. ingests OPT-IN, consent-gated signals: crash/error + device telemetry, and
--      full local-`.db` health-data uploads (anchored to an anonymous device id,
--      or a user id when the install is signed in).
--
-- The entire heavy pipeline (minute storage, ingest, derivation, cron, queues) is
-- GONE — see migrate_to_slim.sql to drop those tables on an existing prod DB.
-- Idempotent: safe to re-run.

-- ── AUTH (kept — needed by the existing-user import sign-in) ───────────────────
CREATE TABLE IF NOT EXISTS users(
  id TEXT PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  name TEXT,
  age INTEGER,
  height_cm REAL,
  weight_kg REAL,
  sex TEXT,                       -- 'm' | 'f' | NULL
  step_goal INTEGER,
  track_cycle INTEGER DEFAULT 0,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS otps(
  email TEXT PRIMARY KEY,
  code_hash TEXT NOT NULL,
  expires_at INTEGER,
  attempts INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS refresh_tokens(
  token_hash TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_refresh_user ON refresh_tokens(user_id);

-- ── DERIVED, READ-ONLY (kept — the import surface) ─────────────────────────────
-- These hold the legacy cloud's already-derived summaries. The slim backend only
-- READS them (GET /profile /strain /sleep /sessions). Nothing writes them anymore;
-- once every v2 user has migrated to local-first they can be dropped entirely.
CREATE TABLE IF NOT EXISTS daily(
  user_id TEXT, date TEXT,
  strain REAL, resting_hr INTEGER, readiness REAL, recovery REAL,
  calories REAL, wear_min REAL, steps INTEGER,
  hr_zones TEXT, acwr REAL, fitness_trend TEXT, anomaly TEXT,
  coach TEXT, stress TEXT, nocturnal TEXT,
  resp_rate REAL, resp_conf REAL,
  hrv_rmssd REAL, hrv_conf REAL, hrv_sdnn REAL, hrv_lfhf REAL, hrv_si REAL,
  illness TEXT, sleep_stress TEXT, drivers TEXT,
  skin_temp_idx REAL, spo2_idx REAL,
  vo2max REAL, fitness REAL, fatigue REAL, form REAL,
  monotony REAL, hrv_cv REAL, nocturnal_dip_pct REAL, irregular TEXT,
  strain_curve TEXT, hr_max INTEGER, hr_min INTEGER, hr_avg INTEGER,
  confidence REAL, flags TEXT, updated_at INTEGER,
  PRIMARY KEY(user_id, date)
);

CREATE TABLE IF NOT EXISTS sleep(
  user_id TEXT, date TEXT,
  onset_ts INTEGER, wake_ts INTEGER, duration_min REAL,
  efficiency REAL, light_min REAL, deep_min REAL, rem_min REAL, regularity REAL,
  confidence REAL, flags TEXT, updated_at INTEGER,
  PRIMARY KEY(user_id, date)
);

CREATE TABLE IF NOT EXISTS sessions(
  user_id TEXT, id TEXT,
  start_ts INTEGER, end_ts INTEGER, type TEXT,
  avg_hr INTEGER, max_hr INTEGER, strain REAL, calories REAL, hrr60 INTEGER, zones TEXT,
  confidence REAL, status TEXT, source TEXT, title TEXT,
  segments TEXT, detected_type TEXT, type_confidence REAL, type_source TEXT,
  PRIMARY KEY(user_id, id)
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, start_ts);

CREATE TABLE IF NOT EXISTS baselines(
  user_id TEXT PRIMARY KEY,
  resting_hr REAL, max_hr REAL, sleep_need_min REAL,
  skin_temp REAL, chronic_strain REAL,
  sleeping_hr REAL, resp_rate REAL,
  hrv_rmssd REAL, skin_temp_raw REAL, spo2_raw REAL, hrv_si REAL,
  updated_at INTEGER
);

-- ── APP CONFIG (kept + extended) ──────────────────────────────────────────────
-- Singleton row (id = 1): OTA update pointer + announcement banner + the current
-- Terms/Privacy pointer (so the consent screen always shows the live version).
-- Served (public) by GET /app/status; written by admin.
CREATE TABLE IF NOT EXISTS app_config(
  id                INTEGER PRIMARY KEY CHECK (id = 1),
  latest_version    TEXT,
  latest_build      INTEGER,
  apk_url           TEXT,
  release_notes     TEXT,
  min_build         INTEGER DEFAULT 0,
  banner_active     INTEGER DEFAULT 0,
  banner_id         TEXT,
  banner_title      TEXT,
  banner_text       TEXT,
  banner_level      TEXT DEFAULT 'info',
  banner_action_url TEXT,
  -- Terms & Privacy (drives the onboarding consent gate). The client records which
  -- terms_version it displayed/accepted; ingested rows stamp that version.
  terms_version     INTEGER DEFAULT 1,
  terms_url         TEXT,
  privacy_url       TEXT,
  terms_summary     TEXT,         -- short human text shown inline on the consent screen
  updated_at        INTEGER
);
INSERT OR IGNORE INTO app_config (id) VALUES (1);

-- ── CONSENT LEDGER (new) ──────────────────────────────────────────────────────
-- One row per (device, scope) recording the latest grant/revoke + the terms version
-- agreed to. The client sends this when the user flips a toggle on the consent
-- screen. Source of truth for "may we keep / accept this device's data?".
CREATE TABLE IF NOT EXISTS consents(
  device_id     TEXT NOT NULL,    -- stable anonymous install id (app-generated UUID)
  scope         TEXT NOT NULL,    -- 'telemetry' | 'health_data'
  granted       INTEGER NOT NULL, -- 1 = opted in, 0 = revoked
  terms_version INTEGER,
  user_id       TEXT,             -- set only when the install is signed in
  updated_at    INTEGER NOT NULL,
  PRIMARY KEY(device_id, scope)
);

-- ── TELEMETRY (new) ───────────────────────────────────────────────────────────
-- Opt-in crash/error logs + device/runtime snapshots. Anchored to an anonymous
-- device_id (user_id only if signed in). NO health metrics here — that is the
-- separate, separately-consented health_uploads channel.
CREATE TABLE IF NOT EXISTS telemetry(
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id       TEXT NOT NULL,
  user_id         TEXT,
  kind            TEXT NOT NULL,  -- 'error' | 'crash' | 'event' | 'device'
  level           TEXT,           -- 'error' | 'warn' | 'info'
  message         TEXT,           -- error text / event name
  stacktrace      TEXT,
  context         TEXT,           -- JSON { pipeline_stage, screen, ... }
  -- device / runtime snapshot
  app_version     TEXT,
  app_build       INTEGER,
  platform        TEXT,           -- 'android' | 'ios'
  os_version      TEXT,
  oem             TEXT,           -- manufacturer
  model           TEXT,           -- device model
  ble_state       TEXT,           -- 'connected' | 'scanning' | 'off' | ...
  battery_pct     INTEGER,        -- phone battery
  band_battery_pct INTEGER,       -- strap battery
  band_serial     TEXT,
  band_firmware   TEXT,
  consent_version INTEGER,
  ts              INTEGER NOT NULL,  -- client event time (unix s)
  created_at      INTEGER NOT NULL   -- server receive time (unix s)
);
CREATE INDEX IF NOT EXISTS idx_telemetry_device ON telemetry(device_id, created_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_kind ON telemetry(kind, created_at);

-- ── HEALTH-DATA UPLOADS (new) ─────────────────────────────────────────────────
-- The full local .db (raw + derived) the user consensually contributes. The blob
-- lives in R2 (HEALTH_BUCKET) at health/{device_id}/{ts}.db(.gz); this is the index
-- + integrity metadata. Withdrawal (DELETE /data) removes both row and object.
CREATE TABLE IF NOT EXISTS health_uploads(
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id       TEXT NOT NULL,
  user_id         TEXT,
  r2_key          TEXT NOT NULL,
  bytes           INTEGER,
  sha256          TEXT,           -- client-supplied integrity hash (optional)
  app_version     TEXT,
  consent_version INTEGER,
  ts              INTEGER NOT NULL,
  created_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_health_uploads_device ON health_uploads(device_id, created_at);
