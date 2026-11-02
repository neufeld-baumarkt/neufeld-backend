BEGIN;

ALTER TABLE cashflow.buchungen
  ADD COLUMN IF NOT EXISTS buchungsgruppe_id uuid,
  ADD COLUMN IF NOT EXISTS rechnungsbetrag_brutto numeric(12,2),
  ADD COLUMN IF NOT EXISTS skonto_prozent numeric(4,2),
  ADD COLUMN IF NOT EXISTS skonto_genutzt boolean,
  ADD COLUMN IF NOT EXISTS zahlungsdatum date,
  ADD COLUMN IF NOT EXISTS zahlbetrag numeric(12,2);

ALTER TABLE cashflow.buchungen
  DROP CONSTRAINT IF EXISTS cashflow_buchungen_skonto_prozent_check;

ALTER TABLE cashflow.buchungen
  ADD CONSTRAINT cashflow_buchungen_skonto_prozent_check
  CHECK (skonto_prozent IS NULL OR skonto_prozent IN (0, 2, 3));

CREATE INDEX IF NOT EXISTS idx_cashflow_buchungen_buchungsgruppe
  ON cashflow.buchungen (buchungsgruppe_id)
  WHERE buchungsgruppe_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cashflow_buchungen_offene_ueberweisungen
  ON cashflow.buchungen (status, datum, jahr, kw)
  WHERE zahlungsart = 'ueberweisung';

COMMIT;
