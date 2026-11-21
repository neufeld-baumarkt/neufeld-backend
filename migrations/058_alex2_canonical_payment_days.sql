-- Materialisiert den kanonischen Qlik/EuroControl-Fachwert in Alex Modul 2.
-- Qlik bleibt primaere Quelle; EuroControl wird nur bei fehlendem Qlik-Wert verwendet.

BEGIN;

ALTER TABLE controlling.payment_review_events
  DROP CONSTRAINT IF EXISTS payment_review_events_event_type_check;
ALTER TABLE controlling.payment_review_events
  ADD CONSTRAINT payment_review_events_event_type_check CHECK (event_type IN (
    'QLIK_SYNCHRONISIERT', 'EUROCONTROL_SYNCHRONISIERT',
    'IN_PRUEFUNG', 'BESTAETIGT', 'KORRIGIERT', 'VERTAGT',
    'AUSGLEICH_AUSSTEHEND', 'EXTERN_BESTAETIGT', 'RUECKGESETZT',
    'TRANSFER_KORRIGIERT'
  )) NOT VALID;

ALTER TABLE controlling.payment_days
  ADD COLUMN IF NOT EXISTS canonical_source_system varchar(20),
  ADD COLUMN IF NOT EXISTS canonical_source_value_id uuid;

UPDATE controlling.payment_days
SET canonical_source_system='QLIK',
    canonical_source_value_id=qlik_derived_value_id
WHERE qlik_derived_value_id IS NOT NULL
  AND canonical_source_system IS NULL
  AND canonical_source_value_id IS NULL;

ALTER TABLE controlling.payment_days
  DROP CONSTRAINT IF EXISTS payment_days_canonical_source_check,
  DROP CONSTRAINT IF EXISTS payment_days_qlik_provenance_check;

ALTER TABLE controlling.payment_days
  ADD CONSTRAINT payment_days_canonical_source_check CHECK (
    (canonical_source_system IS NULL AND canonical_source_value_id IS NULL)
    OR (canonical_source_system IN ('QLIK', 'EUROCONTROL') AND canonical_source_value_id IS NOT NULL)
  ),
  ADD CONSTRAINT payment_days_qlik_provenance_check CHECK (
    (canonical_source_system IS NULL AND (
      (qlik_amount_cents IS NULL AND qlik_import_run_id IS NULL)
      OR (qlik_amount_cents IS NOT NULL AND qlik_import_run_id IS NOT NULL)
    ))
    OR (canonical_source_system='QLIK'
        AND qlik_amount_cents IS NOT NULL
        AND qlik_import_run_id IS NOT NULL
        AND qlik_derived_value_id=canonical_source_value_id)
    OR (canonical_source_system='EUROCONTROL'
        AND qlik_amount_cents IS NOT NULL
        AND qlik_import_run_id IS NULL
        AND qlik_derived_value_id IS NULL)
  );

CREATE INDEX IF NOT EXISTS idx_payment_days_canonical_source
  ON controlling.payment_days (canonical_source_system, canonical_source_value_id)
  WHERE canonical_source_system IS NOT NULL;

COMMENT ON COLUMN controlling.payment_days.canonical_source_system IS
  'Ausgewaehlte Primaerquelle des Fachwerts: QLIK oder EUROCONTROL.';
COMMENT ON COLUMN controlling.payment_days.canonical_source_value_id IS
  'ID des ausgewaehlten Werts in der jeweiligen append-only Quelltabelle.';
COMMENT ON COLUMN controlling.payment_days.qlik_amount_cents IS
  'Legacy-Spaltenname: kanonischer SOLL-Centbetrag aus Qlik oder EuroControl.';

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

CREATE OR REPLACE VIEW controlling.canonical_daily_metric_values AS
WITH qlik_ranked AS (
  SELECT derived.id, derived.branch_id, derived.customer_number,
         derived.business_date, metric.metric_code, derived.numeric_value,
         derived.availability_status, derived.quality_status,
         derived.source_fingerprint, derived.calculated_at,
         row_number() OVER (
           PARTITION BY derived.branch_id, derived.business_date, metric.metric_code
           ORDER BY derived.calculated_at DESC, derived.id DESC
         ) AS source_rank
  FROM controlling.qlik_derived_daily_values derived
  JOIN controlling.qlik_metric_definitions metric
    ON metric.id=derived.metric_definition_id
  WHERE derived.availability_status='VORHANDEN'
    AND derived.quality_status<>'ABGELEHNT'
), eurocontrol_ranked AS (
  SELECT derived.id, derived.branch_id, derived.customer_number,
         derived.business_date, derived.metric_code, derived.numeric_value,
         derived.availability_status, derived.quality_status,
         derived.source_fingerprint, derived.created_at,
         row_number() OVER (
           PARTITION BY derived.branch_id, derived.business_date, derived.metric_code
           ORDER BY derived.created_at DESC, derived.id DESC
         ) AS source_rank
  FROM controlling.eurocontrol_derived_daily_values derived
  WHERE derived.availability_status='VORHANDEN'
    AND derived.quality_status='VALIDATED'
), qlik AS (
  SELECT * FROM qlik_ranked WHERE source_rank=1
), eurocontrol AS (
  SELECT * FROM eurocontrol_ranked WHERE source_rank=1
)
SELECT COALESCE(qlik.branch_id, eurocontrol.branch_id) AS branch_id,
       COALESCE(qlik.customer_number, eurocontrol.customer_number) AS customer_number,
       COALESCE(qlik.business_date, eurocontrol.business_date) AS business_date,
       COALESCE(qlik.metric_code, eurocontrol.metric_code) AS metric_code,
       COALESCE(qlik.numeric_value, eurocontrol.numeric_value) AS numeric_value,
       CASE WHEN qlik.id IS NOT NULL THEN 'QLIK' ELSE 'EUROCONTROL' END AS selected_source,
       CASE
         WHEN qlik.id IS NOT NULL AND eurocontrol.id IS NOT NULL
              AND qlik.numeric_value=eurocontrol.numeric_value THEN 'BOTH_EQUAL'
         WHEN qlik.id IS NOT NULL AND eurocontrol.id IS NOT NULL THEN 'BOTH_CONFLICT'
         WHEN qlik.id IS NOT NULL THEN 'QLIK_PRIMARY'
         ELSE 'EUROCONTROL_FALLBACK'
       END AS source_status,
       qlik.numeric_value AS qlik_numeric_value,
       eurocontrol.numeric_value AS eurocontrol_numeric_value,
       qlik.id AS qlik_value_id,
       eurocontrol.id AS eurocontrol_value_id,
       COALESCE(qlik.source_fingerprint, eurocontrol.source_fingerprint) AS selected_fingerprint,
       COALESCE(qlik.id, eurocontrol.id) AS id
FROM qlik
FULL OUTER JOIN eurocontrol
  ON eurocontrol.branch_id=qlik.branch_id
 AND eurocontrol.business_date=qlik.business_date
 AND eurocontrol.metric_code=qlik.metric_code;

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    GRANT SELECT ON controlling.canonical_daily_metric_values TO sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
