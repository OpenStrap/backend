-- v9 — incremental steps cursor. Lets the 30-min sweep recompute steps over only
-- the newly-SETTLED minutes since the last run (a few R2 objects) instead of
-- re-reading 2 days every time — keeping AN-2554 accuracy at ~free R2 cost.
-- Additive nullable columns on the existing analytics_cursor row.
ALTER TABLE analytics_cursor ADD COLUMN steps_cursor_ts INTEGER;   -- last settled minute counted (unix s)
ALTER TABLE analytics_cursor ADD COLUMN steps_cursor_day TEXT;     -- UTC day the accumulator is for
