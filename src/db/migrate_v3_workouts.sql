-- Migration: workouts. The `sessions` table already holds auto-detected efforts;
-- we add lifecycle columns so it also backs manually-started + live workouts.
--   status: 'live' (in progress) | 'done'
--   source: 'manual' (user pressed ▶) | 'auto' (detectSessions found it)
--   title:  optional user label
-- Run once (D1 ADD COLUMN errors if the column already exists — harmless on retry).
ALTER TABLE sessions ADD COLUMN status TEXT;
ALTER TABLE sessions ADD COLUMN source TEXT;
ALTER TABLE sessions ADD COLUMN title TEXT;
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(user_id, status);
