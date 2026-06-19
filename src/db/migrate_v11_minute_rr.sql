-- [feat/wake-trigger] v11 — beat-to-beat RR per minute, so HRV/recovery (Tier 4)
-- computes from D1 at the wake-close with ZERO R2 re-decode. Additive & safe.
ALTER TABLE minute ADD COLUMN rr BLOB;
