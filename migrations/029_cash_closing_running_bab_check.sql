ALTER TABLE core.cash_drawer_closings
  ADD COLUMN IF NOT EXISTS bab_running_expected_cents INTEGER,
  ADD COLUMN IF NOT EXISTS bab_running_eurocontrol_cents INTEGER,
  ADD COLUMN IF NOT EXISTS bab_running_difference_cents INTEGER,
  ADD COLUMN IF NOT EXISTS bab_running_matches BOOLEAN;

ALTER TABLE core.cash_drawer_closings
  DROP CONSTRAINT IF EXISTS ck_cash_closing_running_bab_check;

ALTER TABLE core.cash_drawer_closings
  ADD CONSTRAINT ck_cash_closing_running_bab_check CHECK (
    (bab_running_expected_cents IS NULL AND bab_running_eurocontrol_cents IS NULL
      AND bab_running_difference_cents IS NULL AND bab_running_matches IS NULL)
    OR
    (bab_running_expected_cents IS NOT NULL AND bab_running_eurocontrol_cents IS NOT NULL
      AND bab_running_difference_cents IS NOT NULL AND bab_running_matches IS NOT NULL
      AND bab_running_expected_cents >= 0 AND bab_running_eurocontrol_cents >= 0
      AND bab_running_difference_cents = bab_running_eurocontrol_cents - bab_running_expected_cents
      AND bab_running_matches = (bab_running_difference_cents = 0))
  );
