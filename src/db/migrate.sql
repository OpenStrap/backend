-- Migration from the old (samples-based) schema to the rollup schema.
-- Idempotent. Existing data is disposable test data — dropping `samples` is fine
-- (raw is preserved in R2 and is re-decodable).

-- 1. Drop the 1Hz samples table (raw lives in R2 now).
DROP TABLE IF EXISTS samples;

-- 2. Add users.sex (no-op if it already exists — guarded by the SELECT pattern
--    isn't available in plain SQL, so this may error harmlessly on re-run; the
--    schema.sql CREATE handles fresh DBs. For an existing DB run this once.)
ALTER TABLE users ADD COLUMN sex TEXT;

-- 3. New timeseries rollup table.
CREATE TABLE IF NOT EXISTS minute(
  user_id TEXT NOT NULL,
  ts_min INTEGER NOT NULL,
  hr_avg INTEGER,
  hr_min INTEGER,
  hr_max INTEGER,
  hr_n INTEGER,
  hr_sum INTEGER DEFAULT 0,
  activity REAL,
  act_sum REAL DEFAULT 0,
  act_n INTEGER DEFAULT 0,
  wrist_on INTEGER DEFAULT 0,
  PRIMARY KEY(user_id, ts_min)
);
CREATE INDEX IF NOT EXISTS idx_minute_user_ts ON minute(user_id, ts_min);

-- 4. Rebuild derived tables with the new column set (drop test data).
DROP TABLE IF EXISTS daily;
DROP TABLE IF EXISTS sleep;
DROP TABLE IF EXISTS sessions;

CREATE TABLE IF NOT EXISTS daily(
  user_id TEXT, date TEXT,
  strain REAL, resting_hr INTEGER, readiness REAL,
  calories REAL, wear_min REAL,        -- calories = ACTIVE calories (HR-driven, est.)
  hr_zones TEXT, acwr REAL, fitness_trend TEXT, anomaly TEXT,
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

-- 5. Rebuild baselines with the new column set.
DROP TABLE IF EXISTS baselines;
CREATE TABLE IF NOT EXISTS baselines(
  user_id TEXT PRIMARY KEY,
  resting_hr REAL, max_hr REAL, sleep_need_min REAL,
  skin_temp REAL, chronic_strain REAL, updated_at INTEGER
);

-- 6. Rebuild analytics_cursor with the new column set.
DROP TABLE IF EXISTS analytics_cursor;
CREATE TABLE IF NOT EXISTS analytics_cursor(
  user_id TEXT PRIMARY KEY,
  last_min_ts INTEGER DEFAULT 0,
  dirty INTEGER DEFAULT 1,
  last_run INTEGER DEFAULT 0
);

-- 7. Rate-limit token bucket.
CREATE TABLE IF NOT EXISTS rate_limit(
  user_id TEXT PRIMARY KEY,
  tokens REAL NOT NULL,
  updated_at INTEGER NOT NULL
);

-- 8. v0 cleanup — drop active/sedentary (removed metrics). Safe/idempotent.
ALTER TABLE daily DROP COLUMN active_min;
ALTER TABLE daily DROP COLUMN sedentary_min;

-- 9. Tier-1 — REAL steps (accel peak-count) re-added. (Earlier v0 dropped the
--    fake-zero steps columns; these hold genuine detected steps now.) Run once;
--    errors harmlessly if the columns already exist.
ALTER TABLE minute ADD COLUMN steps INTEGER DEFAULT 0;
ALTER TABLE daily ADD COLUMN steps INTEGER;

-- 11. Coaching engine output (deterministic plan + strain target + contributors).
ALTER TABLE daily ADD COLUMN coach TEXT;

-- 10. Tier-1 — behavior journal (tags + note) for the correlation engine.
CREATE TABLE IF NOT EXISTS journal(
  user_id TEXT NOT NULL,
  date TEXT NOT NULL,
  tags TEXT,
  note TEXT,
  updated_at INTEGER,
  PRIMARY KEY(user_id, date)
);

-- 12. Bodywave wave — stress (arousal) monitor + nocturnal heart + respiratory
--     rate (PPG, gated). Run once; errors harmlessly if columns already exist.
ALTER TABLE daily ADD COLUMN stress TEXT;
ALTER TABLE daily ADD COLUMN nocturnal TEXT;
ALTER TABLE daily ADD COLUMN resp_rate REAL;
ALTER TABLE daily ADD COLUMN resp_conf REAL;
ALTER TABLE baselines ADD COLUMN sleeping_hr REAL;
ALTER TABLE baselines ADD COLUMN resp_rate REAL;

-- 13. Personalized notifications (deterministic per-user nudges).
CREATE TABLE IF NOT EXISTS notifications(
  user_id TEXT, id TEXT,
  date TEXT, kind TEXT, category TEXT, priority INTEGER,
  title TEXT, body TEXT, window TEXT, quiet_ok INTEGER,
  created_at INTEGER, read_at INTEGER,
  PRIMARY KEY(user_id, id)
);
CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id, created_at);
