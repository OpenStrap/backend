-- migrate_v4_fitness.sql — Trends & Fitness release. Additive only; safe to run
-- against an existing DB (older app builds keep working). The deprecated `readiness`
-- column is REPURPOSED for the new composite Readiness index, so it is NOT re-added.
ALTER TABLE daily ADD COLUMN vo2max REAL;
ALTER TABLE daily ADD COLUMN fitness REAL;
ALTER TABLE daily ADD COLUMN fatigue REAL;
ALTER TABLE daily ADD COLUMN form REAL;
ALTER TABLE daily ADD COLUMN monotony REAL;
ALTER TABLE daily ADD COLUMN hrv_cv REAL;
ALTER TABLE daily ADD COLUMN nocturnal_dip_pct REAL;
ALTER TABLE daily ADD COLUMN irregular TEXT;
