DROP INDEX IF EXISTS core.ux_cash_safe_counts_slot;
CREATE UNIQUE INDEX IF NOT EXISTS ux_cash_safe_counts_slot
  ON core.cash_safe_counts (branch_id,business_date,count_type)
  WHERE count_type IN ('morning','handover');

CREATE INDEX IF NOT EXISTS ix_cash_safe_counts_closing_revisions
  ON core.cash_safe_counts (branch_id,business_date DESC,completed_at DESC,id DESC)
  WHERE count_type='closing';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='cash_business_days_final_report_object') THEN
    ALTER TABLE core.cash_business_days ADD CONSTRAINT cash_business_days_final_report_object
      CHECK (final_report IS NULL OR jsonb_typeof(final_report)='object') NOT VALID;
  END IF;
END $$;

CREATE OR REPLACE FUNCTION core.reject_finalized_cash_business_day_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.status='finalized' THEN
    RAISE EXCEPTION 'Abgeschlossene Geschäftstage sind unveränderbar.';
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS cash_business_days_finalized_immutable ON core.cash_business_days;
CREATE TRIGGER cash_business_days_finalized_immutable
BEFORE UPDATE OR DELETE ON core.cash_business_days
FOR EACH ROW EXECUTE FUNCTION core.reject_finalized_cash_business_day_mutation();
