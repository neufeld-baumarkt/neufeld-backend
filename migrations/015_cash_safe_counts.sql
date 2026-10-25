CREATE TABLE IF NOT EXISTS core.cash_safe_settings (
  branch_id INTEGER PRIMARY KEY REFERENCES public.filialen(id),
  standard_balance_cents INTEGER NOT NULL CHECK (standard_balance_cents >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO core.cash_safe_settings (branch_id,standard_balance_cents)
SELECT id,250000 FROM public.filialen WHERE LOWER(TRIM(name))='vreden'
ON CONFLICT (branch_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS core.cash_safe_counts (
  id BIGSERIAL PRIMARY KEY,
  branch_id INTEGER NOT NULL REFERENCES public.filialen(id),
  business_date DATE NOT NULL DEFAULT ((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date),
  count_type TEXT NOT NULL CHECK (count_type IN ('morning','handover','closing')),
  counter_pin_id BIGINT NOT NULL REFERENCES core.filiale_pins(id),
  recorder_pin_id BIGINT NOT NULL REFERENCES core.filiale_pins(id),
  previous_count_id BIGINT REFERENCES core.cash_safe_counts(id),
  standard_balance_cents INTEGER NOT NULL,
  base_balance_cents INTEGER NOT NULL,
  added_loose_change_cents INTEGER NOT NULL DEFAULT 0,
  balance_before_skim_cents INTEGER NOT NULL,
  skimmed_cents INTEGER NOT NULL DEFAULT 0 CHECK (skimmed_cents >= 0 AND MOD(skimmed_cents,500)=0),
  expected_balance_cents INTEGER NOT NULL,
  counted_total_cents INTEGER NOT NULL,
  difference_cents INTEGER NOT NULL,
  denomination_counts JSONB NOT NULL CHECK (jsonb_typeof(denomination_counts)='object'),
  status TEXT NOT NULL DEFAULT 'completed' CHECK (status='completed'),
  completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by_user_id INTEGER REFERENCES public.users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (counter_pin_id <> recorder_pin_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_cash_safe_counts_slot
  ON core.cash_safe_counts (branch_id,business_date,count_type);
CREATE INDEX IF NOT EXISTS ix_cash_safe_counts_history
  ON core.cash_safe_counts (branch_id,completed_at DESC,id DESC);

CREATE OR REPLACE FUNCTION core.reject_cash_safe_count_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'Abgeschlossene Tresorzählungen sind unveränderbar.';
END;
$$;

DROP TRIGGER IF EXISTS cash_safe_counts_immutable ON core.cash_safe_counts;
CREATE TRIGGER cash_safe_counts_immutable
BEFORE UPDATE OR DELETE ON core.cash_safe_counts
FOR EACH ROW EXECUTE FUNCTION core.reject_cash_safe_count_mutation();
