BEGIN;

CREATE INDEX IF NOT EXISTS idx_payment_days_module2_month_read
  ON controlling.payment_days (
    jahr,
    monat,
    payment_type_id,
    datum DESC,
    filiale
  );

CREATE INDEX IF NOT EXISTS idx_payment_transfers_module2_month_read
  ON controlling.payment_transfers (
    (EXTRACT(YEAR FROM buchungsdatum)::integer),
    (EXTRACT(MONTH FROM buchungsdatum)::integer),
    payment_type_id,
    provider,
    buchungsdatum DESC
  );

COMMIT;
