BEGIN;

CREATE INDEX IF NOT EXISTS idx_payment_days_module2_positive_sales
  ON controlling.payment_days (jahr, monat, payment_type_id, datum)
  WHERE qlik_amount_cents > 0;

COMMIT;
