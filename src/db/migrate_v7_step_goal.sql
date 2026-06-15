-- v7 — user-settable daily step goal. Additive nullable column; NULL means the
-- client uses its own default (8000). Non-breaking: existing rows read as NULL.
ALTER TABLE users ADD COLUMN step_goal INTEGER;
