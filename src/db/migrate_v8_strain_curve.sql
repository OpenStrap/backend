-- v8 — precompute the intra-day strain curve + day HR stats in the cron so
-- /day/strain becomes a PURE READ (no live recompute on the endpoint). Additive
-- nullable columns; existing rows read as NULL until the next analytics run.
ALTER TABLE daily ADD COLUMN strain_curve TEXT;
ALTER TABLE daily ADD COLUMN hr_max INTEGER;
ALTER TABLE daily ADD COLUMN hr_min INTEGER;
ALTER TABLE daily ADD COLUMN hr_avg INTEGER;
