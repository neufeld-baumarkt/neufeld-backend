BEGIN;

ALTER TABLE cashflow.buchungen
  ADD COLUMN IF NOT EXISTS qlik_aggregate_value_id uuid,
  ADD COLUMN IF NOT EXISTS qlik_gebucht_am timestamptz,
  ADD COLUMN IF NOT EXISTS plan_quelle text,
  ADD COLUMN IF NOT EXISTS plan_erstellt_von text;

DO $constraint$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='cashflow_buchungen_qlik_aggregate_fkey'
      AND conrelid='cashflow.buchungen'::regclass
  ) THEN
    ALTER TABLE cashflow.buchungen
      ADD CONSTRAINT cashflow_buchungen_qlik_aggregate_fkey
      FOREIGN KEY (qlik_aggregate_value_id)
      REFERENCES controlling.qlik_derived_daily_aggregates(id);
  END IF;
END
$constraint$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_cashflow_buchungen_qlik_aggregate
  ON cashflow.buchungen (qlik_aggregate_value_id)
  WHERE qlik_aggregate_value_id IS NOT NULL;

ALTER TABLE cashflow.buchungen
  DROP CONSTRAINT IF EXISTS cashflow_buchungen_qlik_booking_check;
ALTER TABLE cashflow.buchungen
  ADD CONSTRAINT cashflow_buchungen_qlik_booking_check CHECK (
    qlik_aggregate_value_id IS NULL
    OR (
      quelle='qlik_gesamtumsatz_alle_filialen'
      AND status='gebucht'
      AND planbetrag IS NOT NULL
      AND plan_quelle IS NOT NULL
      AND qlik_gebucht_am IS NOT NULL
    )
  );

COMMENT ON COLUMN cashflow.buchungen.qlik_aggregate_value_id IS
  'Append-only Qlik-Aggregat, das den gebuchten Alex-1-Istwert belegt.';
COMMENT ON COLUMN cashflow.buchungen.qlik_gebucht_am IS
  'Zeitpunkt der automatischen Alex-1-Buchung aus einem vollstaendigen Qlik-Aggregat.';
COMMENT ON COLUMN cashflow.buchungen.plan_quelle IS
  'Urspruengliche Quelle der bewusst manuell geplanten Alex-1-Einnahme.';
COMMENT ON COLUMN cashflow.buchungen.plan_erstellt_von IS
  'Urspruenglicher Ersteller der bewusst manuell geplanten Alex-1-Einnahme.';

COMMIT;
