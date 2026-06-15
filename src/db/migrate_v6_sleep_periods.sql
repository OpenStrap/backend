-- v6 — Sleep v2 (multi-period). Naps are not a special case: every consolidated
-- sleep period is a first-class row with its own breakdown. Purely ADDITIVE — the
-- v1 `sleep` table and its endpoints are untouched. id = `${user_id}:${onset_ts}`.
CREATE TABLE IF NOT EXISTS sleep_periods(
  user_id TEXT, id TEXT,
  date TEXT,                       -- the sleep "day" this period was derived under
  onset_ts INTEGER, wake_ts INTEGER,
  duration_min REAL, in_bed_min REAL, efficiency REAL,
  light_min REAL, deep_min REAL, rem_min REAL,
  is_main INTEGER,                 -- 1 = longest period of the day (UI hint only)
  confidence REAL, updated_at INTEGER,
  PRIMARY KEY(user_id, id)
);
CREATE INDEX IF NOT EXISTS idx_sleep_periods_date ON sleep_periods(user_id, date);
CREATE INDEX IF NOT EXISTS idx_sleep_periods_onset ON sleep_periods(user_id, onset_ts);
