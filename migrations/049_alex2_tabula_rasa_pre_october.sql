BEGIN;

ALTER TABLE controlling.module2_maintenance_operations
  ADD COLUMN IF NOT EXISTS details jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE TEMP TABLE alex2_cleanup_run_guard ON COMMIT DROP AS
WITH inserted AS (
  INSERT INTO controlling.module2_maintenance_operations (operation_key, details)
  VALUES (
    '2026-11-16_ALEX2_TABULA_RASA_PRE_OCTOBER_V1',
    jsonb_build_object(
      'source', 'MIGRATION_049',
      'resetBefore', '2026-10-01',
      'qlikStartsAt', '2026-06-01',
      'mode', 'HARD_DELETE_AND_REBUILD_FROM_QLIK_SOURCE'
    )
  )
  ON CONFLICT (operation_key) DO NOTHING
  RETURNING operation_key
)
SELECT operation_key FROM inserted;

-- Every historical day referenced by any October-or-later transfer match is
-- protected. This keeps future months outside the cleanup graph.
CREATE TEMP TABLE alex2_cleanup_protected_days ON COMMIT DROP AS
SELECT DISTINCT day.id
FROM controlling.payment_days day
JOIN controlling.payment_transfer_match_items item
  ON item.payment_day_id = day.id
JOIN controlling.payment_transfer_matches match
  ON match.id = item.match_id
JOIN controlling.payment_transfers transfer
  ON transfer.id = match.transfer_id
WHERE day.datum < DATE '2026-10-01'
  AND transfer.buchungsdatum >= DATE '2026-10-01';

CREATE TEMP TABLE alex2_cleanup_reset_matches ON COMMIT DROP AS
SELECT match.id, match.transfer_id
FROM controlling.payment_transfer_matches match
JOIN controlling.payment_transfers transfer
  ON transfer.id = match.transfer_id
WHERE transfer.buchungsdatum < DATE '2026-10-01';

CREATE TEMP TABLE alex2_cleanup_reset_transfers ON COMMIT DROP AS
SELECT transfer.id
FROM controlling.payment_transfers transfer
WHERE transfer.buchungsdatum < DATE '2026-10-01';

CREATE TEMP TABLE alex2_cleanup_reset_days ON COMMIT DROP AS
SELECT
  day.id,
  day.datum
FROM controlling.payment_days day
WHERE day.datum < DATE '2026-10-01'
  AND NOT EXISTS (
    SELECT 1
    FROM alex2_cleanup_protected_days protected
    WHERE protected.id = day.id
  );

-- A carry linking the reset graph to October or later would make hard deletion
-- unsafe. The migration stops instead of guessing how to split it.
DO $cross_boundary_guard$
BEGIN
  IF EXISTS (SELECT 1 FROM alex2_cleanup_run_guard) AND EXISTS (
    SELECT 1
    FROM controlling.payment_transfer_difference_carries carry
    JOIN controlling.payment_transfer_matches source_match
      ON source_match.id = carry.source_match_id
    JOIN controlling.payment_transfers source_transfer
      ON source_transfer.id = source_match.transfer_id
    JOIN controlling.payment_transfer_matches settlement_match
      ON settlement_match.id = carry.settlement_match_id
    JOIN controlling.payment_transfers settlement_transfer
      ON settlement_transfer.id = settlement_match.transfer_id
    WHERE (
      (source_transfer.buchungsdatum < DATE '2026-10-01'
       AND settlement_transfer.buchungsdatum >= DATE '2026-10-01')
      OR
      (settlement_transfer.buchungsdatum < DATE '2026-10-01'
       AND source_transfer.buchungsdatum >= DATE '2026-10-01')
    )
  ) THEN
    RAISE EXCEPTION
      'Tabula rasa abgebrochen: monatsuebergreifender Differenzuebertrag vorhanden.';
  END IF;
END
$cross_boundary_guard$;

UPDATE controlling.module2_maintenance_operations operation
SET details = operation.details || jsonb_build_object(
  'protectedPaymentDays', (SELECT count(*) FROM alex2_cleanup_protected_days),
  'deletedPaymentDays', (SELECT count(*) FROM alex2_cleanup_reset_days),
  'deletedTransfers', (SELECT count(*) FROM alex2_cleanup_reset_transfers),
  'deletedMatches', (SELECT count(*) FROM alex2_cleanup_reset_matches)
)
WHERE operation.operation_key = '2026-11-16_ALEX2_TABULA_RASA_PRE_OCTOBER_V1'
  AND EXISTS (SELECT 1 FROM alex2_cleanup_run_guard);

-- These guards protect normal runtime operation. Migration 049 is the single
-- recorded maintenance operation allowed to remove contaminated history.
DO $disable_event_guards$
BEGIN
  IF EXISTS (SELECT 1 FROM alex2_cleanup_run_guard) THEN
    ALTER TABLE controlling.payment_transfer_events
      DISABLE TRIGGER payment_transfer_events_append_only;
    ALTER TABLE controlling.payment_review_events
      DISABLE TRIGGER payment_review_events_append_only;
    ALTER TABLE controlling.payment_day_component_review_events
      DISABLE TRIGGER payment_day_component_review_events_append_only;
  END IF;
END
$disable_event_guards$;

DELETE FROM controlling.payment_transfer_difference_carries carry
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND (
    EXISTS (
      SELECT 1 FROM alex2_cleanup_reset_matches reset_match
      WHERE reset_match.id = carry.source_match_id
    )
    OR EXISTS (
      SELECT 1 FROM alex2_cleanup_reset_matches reset_match
      WHERE reset_match.id = carry.settlement_match_id
    )
  );

DELETE FROM controlling.payment_transfer_events event
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1 FROM alex2_cleanup_reset_transfers reset_transfer
    WHERE reset_transfer.id = event.transfer_id
  );

DELETE FROM controlling.payment_transfer_match_items item
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1 FROM alex2_cleanup_reset_matches reset_match
    WHERE reset_match.id = item.match_id
  );

DELETE FROM controlling.payment_transfer_matches match
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1 FROM alex2_cleanup_reset_matches reset_match
    WHERE reset_match.id = match.id
  );

DELETE FROM controlling.payment_transfers transfer
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1 FROM alex2_cleanup_reset_transfers reset_transfer
    WHERE reset_transfer.id = transfer.id
  );

DELETE FROM controlling.payment_day_component_review_events event
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1
    FROM controlling.payment_day_component_reviews component
    JOIN alex2_cleanup_reset_days reset_day
      ON reset_day.id = component.payment_day_id
    WHERE component.id = event.component_review_id
  );

DELETE FROM controlling.payment_day_component_reviews component
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1 FROM alex2_cleanup_reset_days reset_day
    WHERE reset_day.id = component.payment_day_id
  );

DELETE FROM controlling.payment_review_events event
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1 FROM alex2_cleanup_reset_days reset_day
    WHERE reset_day.id = event.payment_day_id
  );

DELETE FROM controlling.payment_day_qlik_sources source
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1 FROM alex2_cleanup_reset_days reset_day
    WHERE reset_day.id = source.payment_day_id
  );

DELETE FROM controlling.payment_days day
WHERE EXISTS (SELECT 1 FROM alex2_cleanup_run_guard)
  AND EXISTS (
    SELECT 1 FROM alex2_cleanup_reset_days reset_day
    WHERE reset_day.id = day.id
  );

DO $enable_event_guards$
BEGIN
  IF EXISTS (SELECT 1 FROM alex2_cleanup_run_guard) THEN
    ALTER TABLE controlling.payment_day_component_review_events
      ENABLE TRIGGER payment_day_component_review_events_append_only;
    ALTER TABLE controlling.payment_review_events
      ENABLE TRIGGER payment_review_events_append_only;
    ALTER TABLE controlling.payment_transfer_events
      ENABLE TRIGGER payment_transfer_events_append_only;
  END IF;
END
$enable_event_guards$;

-- Future active payment-day facts must always carry complete Qlik provenance,
-- and their displayed SOLL amount must be the exact cent value from Qlik.
CREATE OR REPLACE FUNCTION controlling.enforce_payment_day_qlik_fact()
RETURNS trigger
LANGUAGE plpgsql
AS $payment_day_qlik_fact_guard$
BEGIN
  IF NEW.qlik_amount_cents IS NULL
     OR NEW.qlik_import_run_id IS NULL
     OR NEW.qlik_derived_value_id IS NULL THEN
    RAISE EXCEPTION
      'Aktive Alex-2-Tageswerte duerfen ausschliesslich aus Qlik stammen.'
      USING ERRCODE = '23514';
  END IF;

  IF NEW.soll_betrag IS NULL
     OR round(NEW.soll_betrag * 100)::bigint <> NEW.qlik_amount_cents THEN
    RAISE EXCEPTION
      'SOLL Qlik muss centgenau dem veredelten Qlik-Wert entsprechen.'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END
$payment_day_qlik_fact_guard$;

DROP TRIGGER IF EXISTS payment_days_qlik_fact_guard
  ON controlling.payment_days;
CREATE TRIGGER payment_days_qlik_fact_guard
BEFORE INSERT OR UPDATE OF
  soll_betrag, qlik_amount_cents, qlik_import_run_id, qlik_derived_value_id
ON controlling.payment_days
FOR EACH ROW EXECUTE FUNCTION controlling.enforce_payment_day_qlik_fact();

-- Payment type is the business fact; provider is derived and never selectable.
CREATE OR REPLACE FUNCTION controlling.enforce_transfer_provider_mapping()
RETURNS trigger
LANGUAGE plpgsql
AS $transfer_provider_guard$
DECLARE
  payment_type_code text;
  expected_provider text;
BEGIN
  SELECT code INTO payment_type_code
  FROM controlling.payment_types
  WHERE id = NEW.payment_type_id;

  expected_provider := CASE
    WHEN payment_type_code = 'EC_CASH' THEN 'TELECASH'
    WHEN payment_type_code = 'KREDITKARTE' THEN 'FIRSTDATA'
    ELSE NULL
  END;

  IF expected_provider IS NOT NULL AND NEW.provider <> expected_provider THEN
    RAISE EXCEPTION
      'Ungueltige Anbieterzuordnung: % muss % verwenden.',
      payment_type_code, expected_provider
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END
$transfer_provider_guard$;

DROP TRIGGER IF EXISTS payment_transfers_provider_guard
  ON controlling.payment_transfers;
CREATE TRIGGER payment_transfers_provider_guard
BEFORE INSERT OR UPDATE OF payment_type_id, provider
ON controlling.payment_transfers
FOR EACH ROW EXECUTE FUNCTION controlling.enforce_transfer_provider_mapping();

COMMIT;
