-- [feat/wake-trigger] v12 — TTL read-cache for Tier 1/2 on-read metrics (no watermark).
CREATE TABLE IF NOT EXISTS read_cache (
  user_id TEXT NOT NULL,
  key TEXT NOT NULL,          -- e.g. 'today:strain:2026-06-19'
  payload TEXT NOT NULL,      -- JSON
  computed_at INTEGER NOT NULL,
  PRIMARY KEY(user_id, key)
) WITHOUT ROWID;
