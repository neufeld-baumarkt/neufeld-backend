ALTER TABLE core.cash_safe_settings
  ADD COLUMN IF NOT EXISTS fixed_drawer_cents INTEGER NOT NULL DEFAULT 50000 CHECK (fixed_drawer_cents >= 0),
  ADD COLUMN IF NOT EXISTS little_bertha_cents INTEGER NOT NULL DEFAULT 100000 CHECK (little_bertha_cents >= 0);

UPDATE core.cash_safe_settings
SET fixed_drawer_cents=50000,little_bertha_cents=100000,updated_at=NOW()
WHERE branch_id IN (SELECT id FROM public.filialen WHERE LOWER(TRIM(name))='vreden');

ALTER TABLE core.cash_safe_counts
  ADD COLUMN IF NOT EXISTS count_mode TEXT NOT NULL DEFAULT 'full_count' CHECK (count_mode IN ('confirmation','full_count')),
  ADD COLUMN IF NOT EXISTS previous_evening_count_id BIGINT REFERENCES core.cash_safe_counts(id),
  ADD COLUMN IF NOT EXISTS fixed_drawer_cents INTEGER NOT NULL DEFAULT 50000 CHECK (fixed_drawer_cents >= 0),
  ADD COLUMN IF NOT EXISTS little_bertha_cents INTEGER NOT NULL DEFAULT 100000 CHECK (little_bertha_cents >= 0),
  ADD COLUMN IF NOT EXISTS loose_money_cents INTEGER NOT NULL DEFAULT 0 CHECK (loose_money_cents >= 0),
  ADD COLUMN IF NOT EXISTS coin_roll_counts JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(coin_roll_counts)='object'),
  ADD COLUMN IF NOT EXISTS coin_roll_total_cents INTEGER NOT NULL DEFAULT 0 CHECK (coin_roll_total_cents >= 0),
  ADD COLUMN IF NOT EXISTS banknote_counts JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(banknote_counts)='object'),
  ADD COLUMN IF NOT EXISTS banknote_total_cents INTEGER NOT NULL DEFAULT 0 CHECK (banknote_total_cents >= 0),
  ADD COLUMN IF NOT EXISTS eurocontrol_total_cents INTEGER CHECK (eurocontrol_total_cents >= 0),
  ADD COLUMN IF NOT EXISTS first_employee_role TEXT,
  ADD COLUMN IF NOT EXISTS second_employee_role TEXT;
