-- Migration: HRV-based metrics + cross-metric drivers.
-- Adds the columns the new analytics write (recovery/HRV/stress/illness/sleep-stress
-- + the "what affected what" driver JSON). Idempotent-ish: D1 has no IF NOT EXISTS
-- for ADD COLUMN, so run once; re-running errors on existing columns (harmless).

-- daily: HRV-derived metrics (computed in biometrics.ts from real RR) + drivers.
ALTER TABLE daily ADD COLUMN recovery REAL;          -- HRV recovery 0..100 (Plews lnRMSSD z); replaces readiness
ALTER TABLE daily ADD COLUMN hrv_sdnn REAL;          -- nocturnal SDNN (ms)
ALTER TABLE daily ADD COLUMN hrv_lfhf REAL;          -- LF/HF balance
ALTER TABLE daily ADD COLUMN hrv_si REAL;            -- Baevsky Stress Index (for personal-relative stress)
ALTER TABLE daily ADD COLUMN illness TEXT;           -- Mahalanobis illness signal (JSON, with drivers)
ALTER TABLE daily ADD COLUMN sleep_stress TEXT;      -- nocturnal arousal (JSON, with drivers)
ALTER TABLE daily ADD COLUMN drivers TEXT;           -- per-metric driver graph for the day (JSON {metric:[Driver]})

-- baselines: personal Baevsky-SI baseline for personal-relative stress scoring.
ALTER TABLE baselines ADD COLUMN hrv_si REAL;
