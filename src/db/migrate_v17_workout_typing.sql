-- migrate_v17 — workout typing + calibration ledger.
--
-- Auto-detected workouts now carry a motion-based TYPE (Mannini HAR classifier, run at
-- ingest on the live high-rate accel; the per-minute class rides minute_day.act_class)
-- plus a graceful PHASE breakdown for multi-activity sessions. We also record what the
-- model said (detected_type) vs what the user confirmed/corrected (type_source) so the
-- app can show how often the classifier was right and we know when it needs retraining.
--
-- Additive; safe to re-run (column-exists errors are ignored by the migrate runner).

ALTER TABLE sessions ADD COLUMN segments TEXT;          -- JSON [{start_ts,end_ts,type,confidence}] phases
ALTER TABLE sessions ADD COLUMN detected_type TEXT;     -- the classifier's call at detection (immutable record)
ALTER TABLE sessions ADD COLUMN type_confidence REAL;   -- ESTIMATE confidence in the type
ALTER TABLE sessions ADD COLUMN type_source TEXT;       -- 'model' | 'confirmed' | 'corrected'
