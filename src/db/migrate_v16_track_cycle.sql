-- v16 — explicit opt-in for menstrual cycle tracking. Cycle data is only computed
-- and shown when the user turns this on themselves (consent, not inferred from sex).
-- Additive & safe on existing DBs.
ALTER TABLE users ADD COLUMN track_cycle INTEGER DEFAULT 0;
