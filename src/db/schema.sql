-- openstrap-backend D1 schema (BACKEND_SPEC §2). Idempotent.
-- Raw 1Hz frames live in R2 (re-decodable); D1 holds per-minute rollups + tiny
-- derived tables. No `samples` table.

-- ── AUTH ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users(
  id TEXT PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  name TEXT,
  age INTEGER,
  height_cm REAL,
  weight_kg REAL,
  sex TEXT,                       -- 'm' | 'f' | NULL (NULL → sex-neutral calories)
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

-- ── TIMESERIES ROLLUP (≤1440 rows/day/user; pruned to R2 after 90 days) ───────
-- hr_sum / act_sum / act_n are running aggregates kept so the ingest upsert can
-- merge new samples into the stored minute EXACTLY (deterministic idempotency).
-- hr_avg / activity are the derived display values (kept in sync on every write).
CREATE TABLE IF NOT EXISTS minute(
  user_id TEXT NOT NULL,
  ts_min INTEGER NOT NULL,        -- unix sec floored to minute
  hr_avg INTEGER,
  hr_min INTEGER,
  hr_max INTEGER,
  hr_n INTEGER,
  hr_sum INTEGER DEFAULT 0,
  activity REAL,
  act_sum REAL DEFAULT 0,
  act_n INTEGER DEFAULT 0,
  steps INTEGER DEFAULT 0,        -- real detected steps (accel peak-count)
  wrist_on INTEGER DEFAULT 0,
  PRIMARY KEY(user_id, ts_min)
);
CREATE INDEX IF NOT EXISTS idx_minute_user_ts ON minute(user_id, ts_min);

CREATE TABLE IF NOT EXISTS events(
  user_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  hex TEXT NOT NULL,
  event_id INTEGER,
  ts INTEGER,
  PRIMARY KEY(user_id, device_id, hex)
);
CREATE INDEX IF NOT EXISTS idx_events_user_ts ON events(user_id, ts);

-- Behavior journal — daily tags + note (correlation engine source).
CREATE TABLE IF NOT EXISTS journal(
  user_id TEXT NOT NULL,
  date TEXT NOT NULL,             -- YYYY-MM-DD
  tags TEXT,                      -- JSON array of lowercase tag strings
  note TEXT,
  updated_at INTEGER,
  PRIMARY KEY(user_id, date)
);

-- ── DERIVED (permanent, tiny) ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily(
  user_id TEXT, date TEXT,
  strain REAL, resting_hr INTEGER, readiness REAL,
  calories REAL, wear_min REAL, steps INTEGER,   -- calories = ACTIVE (est.); steps = detected (est.)
  hr_zones TEXT, acwr REAL, fitness_trend TEXT, anomaly TEXT,
  coach TEXT,                          -- deterministic coach plan (JSON)
  stress TEXT,                         -- arousal monitor summary (JSON)
  nocturnal TEXT,                      -- nocturnal-heart summary (JSON)
  resp_rate REAL, resp_conf REAL,      -- nightly respiratory rate (PPG; GATED)
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
  confidence REAL,
  PRIMARY KEY(user_id, id)
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, start_ts);

CREATE TABLE IF NOT EXISTS baselines(
  user_id TEXT PRIMARY KEY,
  resting_hr REAL, max_hr REAL, sleep_need_min REAL,
  skin_temp REAL, chronic_strain REAL,
  sleeping_hr REAL, resp_rate REAL,    -- nocturnal-HR + respiratory baselines
  updated_at INTEGER
);

-- Personalized notifications — deterministic per-user nudges (server-generated,
-- client pulls + presents). id = `${date}:${kind}` (idempotent regeneration).
CREATE TABLE IF NOT EXISTS notifications(
  user_id TEXT, id TEXT,
  date TEXT, kind TEXT, category TEXT, priority INTEGER,
  title TEXT, body TEXT, window TEXT, quiet_ok INTEGER,
  created_at INTEGER, read_at INTEGER,
  PRIMARY KEY(user_id, id)
);
CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id, created_at);

-- ── CONTROL ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_cursor(
  user_id TEXT PRIMARY KEY,
  last_min_ts INTEGER DEFAULT 0,
  dirty INTEGER DEFAULT 1,
  last_run INTEGER DEFAULT 0
);

-- Per-user ingest rate-limit token bucket (RESILIENCE §7).
CREATE TABLE IF NOT EXISTS rate_limit(
  user_id TEXT PRIMARY KEY,
  tokens REAL NOT NULL,
  updated_at INTEGER NOT NULL
);
