-- [feat/wake-trigger] v13 — incremental sleep/wake state machine columns on the cursor.
-- The */N cron's ONLY job reads these: skip awake-and-closed users (cursor-only),
-- peek recent minutes for the asleep ones, fire close_day once per physiological day.
ALTER TABLE analytics_cursor ADD COLUMN sleep_phase TEXT;       -- 'awake' | 'asleep' | NULL
ALTER TABLE analytics_cursor ADD COLUMN phase_since INTEGER;    -- unix s of last transition
ALTER TABLE analytics_cursor ADD COLUMN last_close_date TEXT;   -- YYYY-MM-DD of last day-close
