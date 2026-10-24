ALTER TABLE core.filiale_pins
  ALTER COLUMN filiale_id DROP NOT NULL;

DROP INDEX IF EXISTS core.ux_filiale_pins_filiale_display;

CREATE INDEX IF NOT EXISTS idx_filiale_pins_active
  ON core.filiale_pins (is_active, id);
