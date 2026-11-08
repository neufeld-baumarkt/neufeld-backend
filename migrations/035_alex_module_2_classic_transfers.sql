BEGIN;

ALTER TABLE controlling.payment_transfers
  DROP CONSTRAINT IF EXISTS payment_transfers_provider_check;

ALTER TABLE controlling.payment_transfers
  ADD CONSTRAINT payment_transfers_provider_check
    CHECK (provider IN ('FIRSTDATA', 'TELECASH', 'UNBEKANNT', 'CLASSIC'));

CREATE UNIQUE INDEX IF NOT EXISTS ux_payment_transfers_classic_day
  ON controlling.payment_transfers (payment_type_id, buchungsdatum)
  WHERE provider = 'CLASSIC';

COMMIT;
