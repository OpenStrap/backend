-- [feat/wake-trigger] v14 — day-packed minute store: ONE row per (user, ymd), value =
-- gzipped JSON array of per-minute records. Replaces the row-per-minute `minute` table
-- as the hot store (ingest RMWs one row/day instead of ~1,440). Sealed days move to R2.
-- (The legacy `minute` table is left in place but unused; it stays empty on fresh DBs.)
CREATE TABLE IF NOT EXISTS minute_day (
  user_id TEXT NOT NULL,
  ymd TEXT NOT NULL,            -- 'YYYY-MM-DD' (UTC day of the minute)
  blob BLOB NOT NULL,          -- gzipped JSON MinuteRec[] (bound param; << 2MB cap)
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(user_id, ymd)
) WITHOUT ROWID;
