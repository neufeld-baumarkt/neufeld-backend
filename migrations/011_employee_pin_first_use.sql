ALTER TABLE core.filiale_pins
  ADD COLUMN IF NOT EXISTS first_name TEXT,
  ADD COLUMN IF NOT EXISTS last_name TEXT,
  ADD COLUMN IF NOT EXISTS must_change_pin BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE core.filiale_pins
SET updated_at = COALESCE(updated_at, created_at, NOW())
WHERE updated_at IS NULL;
