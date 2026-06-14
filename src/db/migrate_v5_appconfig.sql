-- migrate_v5_appconfig.sql — admin-controlled app config: OTA update pointer +
-- home-screen alert banner. Singleton row (id = 1). Additive; safe to re-run
-- against an existing DB (older app builds simply never call /app/status).
CREATE TABLE IF NOT EXISTS app_config(
  id                INTEGER PRIMARY KEY CHECK (id = 1),
  latest_version    TEXT,        -- semver shown to the user, e.g. "0.3.0"
  latest_build      INTEGER,     -- monotonic build number; app compares against its own
  apk_url           TEXT,        -- signed-APK download URL (GitHub release asset)
  release_notes     TEXT,        -- what's new (shown in the update prompt)
  min_build         INTEGER DEFAULT 0,  -- clients below this are forced to update
  banner_active     INTEGER DEFAULT 0,  -- 0/1: show the home-screen alert banner
  banner_id         TEXT,        -- stable id so a dismissed banner stays dismissed
  banner_title      TEXT,
  banner_text       TEXT,
  banner_level      TEXT DEFAULT 'info',  -- info | warn | critical (critical = not dismissible)
  banner_action_url TEXT,        -- optional tap-through link
  updated_at        INTEGER
);
INSERT OR IGNORE INTO app_config (id) VALUES (1);
