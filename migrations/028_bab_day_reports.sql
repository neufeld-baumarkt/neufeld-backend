-- BaB-Aufschluesselung fuer bestehende Tagesberichte und Alex-Modul 3 (EZ/AZ).
-- Die bestehenden final_report-Dokumente enthalten bereits alle Ausgangswerte.
BEGIN;

ALTER TABLE core.cash_business_days
  DISABLE TRIGGER cash_business_days_finalized_immutable;

WITH source AS (
  SELECT
    id,
    COALESCE(
      (final_report->>'eurocontrolCashTotalCents')::bigint,
      (final_report->'totals'->>'cashRevenueCents')::bigint
    ) AS target_cents,
    COALESCE((final_report->'totals'->>'additionalSafeSkimCents')::bigint, 0) AS skim_cents
  FROM core.cash_business_days
  WHERE status='finalized'
    AND final_report IS NOT NULL
    AND NOT (final_report ? 'babReport')
    AND COALESCE(final_report->>'eurocontrolCashTotalCents', final_report->'totals'->>'cashRevenueCents') IS NOT NULL
)
UPDATE core.cash_business_days day
SET final_report=jsonb_set(
  day.final_report,
  '{babReport}',
  jsonb_build_object(
    'eurocontrolTargetCents', source.target_cents,
    'roundedCents', (source.target_cents / 500) * 500,
    'safeSkimCents', source.skim_cents,
    'bookedCents', ((source.target_cents / 500) * 500) + source.skim_cents
  ),
  true
)
FROM source
WHERE day.id=source.id;

ALTER TABLE core.cash_business_days
  ENABLE TRIGGER cash_business_days_finalized_immutable;

COMMIT;
