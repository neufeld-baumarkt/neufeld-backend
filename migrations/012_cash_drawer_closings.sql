ALTER TABLE core.cash_drawer_closings
  ADD COLUMN IF NOT EXISTS business_date DATE,
  ADD COLUMN IF NOT EXISTS opening_present BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS proceeded_without_opening BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS full_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS retained_coin_cents INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS loose_change_cents INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS retained_note_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS bank_note_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS bank_amount_cents INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS eurocontrol_total_cents INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS notification_required BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS notification_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS recount_attempts JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS created_by_user_id INTEGER REFERENCES public.users(id);

UPDATE core.cash_drawer_closings
SET business_date=((closed_at AT TIME ZONE 'Europe/Berlin')::date)
WHERE business_date IS NULL;

ALTER TABLE core.cash_drawer_closings
  ALTER COLUMN business_date SET DEFAULT ((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date),
  ALTER COLUMN business_date SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_cash_drawer_closings_opening
  ON core.cash_drawer_closings (opening_count_id)
  WHERE opening_count_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_cash_drawer_closings_day
  ON core.cash_drawer_closings (branch_id,business_date DESC,drawer_number,closed_at DESC);

CREATE OR REPLACE FUNCTION core.reject_cash_drawer_closing_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'Abgeschlossene Kassenabrechnungen sind unveränderbar. Korrekturen müssen als neuer Datensatz erfolgen.';
END;
$$;

DROP TRIGGER IF EXISTS cash_drawer_closings_immutable ON core.cash_drawer_closings;
CREATE TRIGGER cash_drawer_closings_immutable
BEFORE UPDATE OR DELETE ON core.cash_drawer_closings
FOR EACH ROW EXECUTE FUNCTION core.reject_cash_drawer_closing_mutation();
