-- v10 — event-driven biometrics. Track the last sleep-date for which the sweep
-- already fired biometrics, so "a fresh night just finished" triggers recovery/HRV
-- once (not every sweep). The nightly cron stays as the catch-all. Additive column.
ALTER TABLE analytics_cursor ADD COLUMN bio_last_date TEXT;
