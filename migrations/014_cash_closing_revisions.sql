ALTER TABLE core.cash_drawer_closings
  ADD COLUMN IF NOT EXISTS closed_by_pin_id BIGINT REFERENCES core.filiale_pins(id),
  ADD COLUMN IF NOT EXISTS record_kind TEXT NOT NULL DEFAULT 'original',
  ADD COLUMN IF NOT EXISTS corrects_closing_id BIGINT REFERENCES core.cash_drawer_closings(id),
  ADD COLUMN IF NOT EXISTS correction_reason TEXT,
  ADD COLUMN IF NOT EXISTS revision_number INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS core.cash_business_days (
  id BIGSERIAL PRIMARY KEY,
  branch_id INTEGER NOT NULL REFERENCES public.filialen(id),
  business_date DATE NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','finalized')),
  finalized_at TIMESTAMPTZ,
  finalized_by_user_id INTEGER REFERENCES public.users(id),
  final_report JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (branch_id,business_date)
);

INSERT INTO core.cash_business_days (branch_id,business_date)
SELECT DISTINCT branch_id,business_date FROM core.cash_drawer_closings
ON CONFLICT (branch_id,business_date) DO NOTHING;

INSERT INTO core.cash_business_days (branch_id,business_date)
SELECT DISTINCT branch_id,business_date FROM core.cash_drawer_opening_counts
ON CONFLICT (branch_id,business_date) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='cash_drawer_closings_record_kind_check'
    AND conrelid='core.cash_drawer_closings'::regclass) THEN
    ALTER TABLE core.cash_drawer_closings
      ADD CONSTRAINT cash_drawer_closings_record_kind_check
      CHECK (record_kind IN ('original','correction'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='cash_drawer_closings_correction_reason_check'
    AND conrelid='core.cash_drawer_closings'::regclass) THEN
    ALTER TABLE core.cash_drawer_closings
      ADD CONSTRAINT cash_drawer_closings_correction_reason_check
      CHECK (record_kind='original' OR (corrects_closing_id IS NOT NULL AND LENGTH(TRIM(correction_reason))>=10));
  END IF;
END $$;

DROP INDEX IF EXISTS core.ux_cash_drawer_closings_opening;
CREATE UNIQUE INDEX IF NOT EXISTS ux_cash_drawer_closings_opening
  ON core.cash_drawer_closings (opening_count_id)
  WHERE opening_count_id IS NOT NULL AND record_kind='original';

CREATE UNIQUE INDEX IF NOT EXISTS ux_cash_drawer_closings_corrects
  ON core.cash_drawer_closings (corrects_closing_id)
  WHERE corrects_closing_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_cash_drawer_closing_revisions
  ON core.cash_drawer_closings (branch_id,business_date DESC,record_kind,closed_at DESC);
