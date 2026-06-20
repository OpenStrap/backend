-- v15 — menstrual cycle log (user-logged period events; anchor for calcCycle).
-- Additive & safe on existing DBs.
CREATE TABLE IF NOT EXISTS cycle_log(
  user_id TEXT NOT NULL,
  date TEXT NOT NULL,             -- YYYY-MM-DD
  kind TEXT NOT NULL,             -- 'start' | 'end' | 'spotting'
  note TEXT,
  updated_at INTEGER,
  PRIMARY KEY(user_id, date)
);
