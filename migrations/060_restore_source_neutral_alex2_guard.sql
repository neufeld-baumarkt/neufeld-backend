BEGIN;

-- Migration 049 is intentionally replayed by the Alex-2 startup maintenance.
-- Since EuroControl became a canonical fallback, the replay must no longer
-- restore the historic Qlik-only trigger function.
CREATE OR REPLACE FUNCTION controlling.enforce_payment_day_qlik_fact()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_day_canonical_fact_guard$
BEGIN
  IF NEW.canonical_source_system IS NULL
     OR NEW.canonical_source_value_id IS NULL
     OR NEW.qlik_amount_cents IS NULL THEN
    RAISE EXCEPTION
      'Aktive Alex-2-Tageswerte benoetigen eine vollstaendige kanonische Herkunft.'
      USING ERRCODE = '23514';
  END IF;

  IF NEW.canonical_source_system='QLIK' AND (
       NEW.qlik_import_run_id IS NULL
       OR NEW.qlik_derived_value_id IS NULL
       OR NEW.qlik_derived_value_id<>NEW.canonical_source_value_id
     ) THEN
    RAISE EXCEPTION
      'Qlik-Tageswerte benoetigen eine vollstaendige Qlik-Herkunft.'
      USING ERRCODE = '23514';
  END IF;

  IF NEW.canonical_source_system='EUROCONTROL' AND (
       NEW.qlik_import_run_id IS NOT NULL OR NEW.qlik_derived_value_id IS NOT NULL
     ) THEN
    RAISE EXCEPTION
      'EuroControl-Tageswerte duerfen keine Qlik-Herkunft vortaeuschen.'
      USING ERRCODE = '23514';
  END IF;

  IF NEW.soll_betrag IS NULL
     OR round(NEW.soll_betrag * 100)::bigint <> NEW.qlik_amount_cents THEN
    RAISE EXCEPTION
      'SOLL muss centgenau dem kanonischen Fachwert entsprechen.'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END
$payment_day_canonical_fact_guard$;

DROP TRIGGER IF EXISTS payment_days_qlik_fact_guard
  ON controlling.payment_days;
CREATE TRIGGER payment_days_qlik_fact_guard
BEFORE INSERT OR UPDATE OF
  soll_betrag, qlik_amount_cents, qlik_import_run_id, qlik_derived_value_id,
  canonical_source_system, canonical_source_value_id
ON controlling.payment_days
FOR EACH ROW EXECUTE FUNCTION controlling.enforce_payment_day_qlik_fact();

COMMIT;
