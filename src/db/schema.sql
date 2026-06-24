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
  step_goal INTEGER,              -- user's daily step goal; NULL → client default (8000)
  track_cycle INTEGER DEFAULT 0,  -- explicit opt-in to menstrual cycle tracking (0/1)
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

-- ── TIMESERIES (day-packed) ───────────────────────────────────────────────────
-- [feat/wake-trigger] minute_day is the HOT minute store: ONE row per (user, ymd),
-- value = gzipped JSON MinuteRec[] (one entry per touched minute; RR rides the blob
-- as number[]). Ingest read-merge-writes ~1 row/day instead of ~1,440. Days older
-- than the hot window are sealed to gzipped R2 objects and the D1 row dropped; the
-- 10-day prune drops ~1 row/day. See minute_store.ts. (migrate_v14)
CREATE TABLE IF NOT EXISTS minute_day(
  user_id TEXT NOT NULL,
  ymd TEXT NOT NULL,              -- 'YYYY-MM-DD' (UTC day of the minute)
  blob BLOB NOT NULL,             -- gzipped JSON MinuteRec[] (bound param; << 2MB cap)
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(user_id, ymd)
) WITHOUT ROWID;
-- seal/prune scan by day across all users (WHERE ymd < cutoff) — index the bare ymd.
CREATE INDEX IF NOT EXISTS idx_minute_day_ymd ON minute_day(ymd);

-- TTL read-cache for Tier 1/2 on-read metrics (no watermark; time-based only).
-- key e.g. 'today:strain:2026-06-19'; today→60s, past days immutable-until-prune.
-- See cache.ts. (migrate_v12)
CREATE TABLE IF NOT EXISTS read_cache(
  user_id TEXT NOT NULL,
  key TEXT NOT NULL,
  payload TEXT NOT NULL,          -- JSON
  computed_at INTEGER NOT NULL,
  PRIMARY KEY(user_id, key)
) WITHOUT ROWID;

-- ── LEGACY ROLLUP (deprecated; empty on fresh DBs) ────────────────────────────
-- The pre-v14 row-per-minute store. Superseded by minute_day above; left in place so
-- old deployments keep working, but ingest no longer writes here. Safe to drop once
-- no instance predates v14.
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

-- Menstrual cycle log — user-logged period events (the anchor for calcCycle).
CREATE TABLE IF NOT EXISTS cycle_log(
  user_id TEXT NOT NULL,
  date TEXT NOT NULL,             -- YYYY-MM-DD
  kind TEXT NOT NULL,             -- 'start' | 'end' | 'spotting'
  note TEXT,
  updated_at INTEGER,
  PRIMARY KEY(user_id, date)
);

-- ── DERIVED (permanent, tiny) ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily(
  user_id TEXT, date TEXT,
  strain REAL, resting_hr INTEGER, readiness REAL,  -- readiness DEPRECATED (heuristic) → see recovery
  recovery REAL,                       -- HRV recovery 0..100 (Plews lnRMSSD z) — replaces readiness
  calories REAL, wear_min REAL, steps INTEGER,   -- calories = ACTIVE (est.); steps = detected (est.)
  hr_zones TEXT, acwr REAL, fitness_trend TEXT, anomaly TEXT,
  coach TEXT,                          -- deterministic coach plan (JSON)
  stress TEXT,                         -- HRV stress (Baevsky SI + LF/HF) (JSON, with drivers)
  nocturnal TEXT,                      -- nocturnal-heart summary (JSON)
  resp_rate REAL, resp_conf REAL,      -- nightly respiratory rate (RSA from RR / PPG)
  hrv_rmssd REAL, hrv_conf REAL,       -- nocturnal HRV (RMSSD, ms) from beat-to-beat RR
  hrv_sdnn REAL, hrv_lfhf REAL, hrv_si REAL,  -- SDNN + LF/HF (Lomb–Scargle) + Baevsky SI
  illness TEXT,                        -- Mahalanobis illness signal (JSON, with drivers)
  sleep_stress TEXT,                   -- nocturnal arousal / sleep-stress (JSON, with drivers)
  drivers TEXT,                        -- per-metric driver graph for the day (JSON)
  skin_temp_idx REAL, spo2_idx REAL,   -- RELATIVE: raw ADC night value − personal baseline
  vo2max REAL,                         -- VO₂max estimate (Uth–Sørensen, ESTIMATE)
  fitness REAL, fatigue REAL, form REAL,  -- Banister CTL / ATL / TSB from daily strain
  monotony REAL,                       -- Foster training monotony (7d mean/SD strain)
  hrv_cv REAL,                         -- coefficient of variation of nightly RMSSD (%)
  nocturnal_dip_pct REAL,              -- nocturnal HR dip (fraction) — trendable copy
  irregular TEXT,                      -- irregular-rhythm SCREEN (JSON, Poincaré) — not a diagnosis
  strain_curve TEXT,                   -- precomputed intra-day cumulative-strain curve (JSON) so /day/strain is a pure read
  hr_max INTEGER, hr_min INTEGER, hr_avg INTEGER,  -- precomputed day HR stats (pure-read /day/strain)
  -- NOTE: `readiness` (above) is REPURPOSED for the composite Readiness index
  -- (HRV ∩ sleep ∩ dip ∩ arousal), written by biometrics.ts.
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

-- Sleep v2 (multi-period; naps = shorter sleeps). Additive; v1 `sleep` above is
-- the single main-period table and stays as-is. See migrate_v6_sleep_periods.sql.
CREATE TABLE IF NOT EXISTS sleep_periods(
  user_id TEXT, id TEXT,
  date TEXT,
  onset_ts INTEGER, wake_ts INTEGER,
  duration_min REAL, in_bed_min REAL, efficiency REAL,
  light_min REAL, deep_min REAL, rem_min REAL,
  is_main INTEGER,
  confidence REAL, updated_at INTEGER,
  PRIMARY KEY(user_id, id)
);
CREATE INDEX IF NOT EXISTS idx_sleep_periods_date ON sleep_periods(user_id, date);
CREATE INDEX IF NOT EXISTS idx_sleep_periods_onset ON sleep_periods(user_id, onset_ts);

CREATE TABLE IF NOT EXISTS sessions(
  user_id TEXT, id TEXT,
  start_ts INTEGER, end_ts INTEGER, type TEXT,
  avg_hr INTEGER, max_hr INTEGER, strain REAL, calories REAL, hrr60 INTEGER, zones TEXT,
  confidence REAL,
  status TEXT,   -- 'live' | 'done'
  source TEXT,   -- 'manual' (user started) | 'auto' (minute backstop) | 'auto_live' (live-stream detected)
  title TEXT,    -- optional user label
  segments TEXT,        -- JSON [{start_ts,end_ts,type,confidence}] phases (multi-activity workouts)
  detected_type TEXT,   -- the HAR classifier's call at detection (calibration ledger)
  type_confidence REAL, -- ESTIMATE confidence in the workout type
  type_source TEXT,     -- 'model' | 'confirmed' | 'corrected'
  PRIMARY KEY(user_id, id)
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, start_ts);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(user_id, status);

CREATE TABLE IF NOT EXISTS baselines(
  user_id TEXT PRIMARY KEY,
  resting_hr REAL, max_hr REAL, sleep_need_min REAL,
  skin_temp REAL, chronic_strain REAL,
  sleeping_hr REAL, resp_rate REAL,    -- nocturnal-HR + respiratory baselines
  hrv_rmssd REAL, skin_temp_raw REAL, spo2_raw REAL,  -- HRV + raw temp/red-ADC baselines
  hrv_si REAL,                         -- personal Baevsky-SI baseline (for relative stress)
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
  last_run INTEGER DEFAULT 0,
  steps_cursor_ts INTEGER,    -- incremental steps: last settled minute counted (unix s)
  steps_cursor_day TEXT,      -- UTC day the steps accumulator is for
  bio_last_date TEXT,         -- event-driven biometrics: last sleep-date already triggered
  -- [feat/wake-trigger] incremental sleep/wake state machine (migrate_v13). The */N
  -- cron reads these: skip awake-and-closed users, peek the asleep ones, fire close_day
  -- once per physiological day.
  sleep_phase TEXT,           -- 'awake' | 'asleep' | NULL
  phase_since INTEGER,        -- unix s of last transition
  last_close_date TEXT        -- YYYY-MM-DD of last day-close
);

-- Per-user ingest rate-limit token bucket (RESILIENCE §7).
CREATE TABLE IF NOT EXISTS rate_limit(
  user_id TEXT PRIMARY KEY,
  tokens REAL NOT NULL,
  updated_at INTEGER NOT NULL
);

-- Admin-controlled app config: OTA update pointer + home-screen alert banner.
-- Singleton row (id = 1). Served (public) by GET /app/status, written by admin.
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
  updated_at        INTEGER
);
INSERT OR IGNORE INTO app_config (id) VALUES (1);

-- ── STRAVA (bidirectional sync) ───────────────────────────────────────────────
-- OAuth tokens per user. access_token is short-lived; refresh via refresh_token.
CREATE TABLE IF NOT EXISTS strava_tokens(
  user_id TEXT PRIMARY KEY,
  athlete_id INTEGER,
  access_token TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  expires_at INTEGER NOT NULL,     -- unix seconds; refresh when now >= this
  scope TEXT,
  connected_at INTEGER
);
-- Activities pulled from Strava (Strava → Whoop), kept for the in-app overlay.
CREATE TABLE IF NOT EXISTS strava_activities(
  user_id TEXT NOT NULL,
  activity_id INTEGER NOT NULL,    -- Strava's activity id
  start_ts INTEGER,                -- unix seconds (UTC)
  elapsed_sec INTEGER,
  type TEXT,
  name TEXT,
  distance_m REAL,
  avg_hr REAL, max_hr REAL,
  raw TEXT,                        -- the JSON summary, for later enrichment
  pulled_at INTEGER,
  PRIMARY KEY(user_id, activity_id)
);
CREATE INDEX IF NOT EXISTS idx_strava_act_user ON strava_activities(user_id, start_ts);
-- Which OpenStrap sessions we've pushed to Strava (Whoop → Strava dedupe).
CREATE TABLE IF NOT EXISTS strava_pushed(
  user_id TEXT NOT NULL,
  session_key TEXT NOT NULL,
  activity_id INTEGER,
  pushed_at INTEGER,
  PRIMARY KEY(user_id, session_key)
);
