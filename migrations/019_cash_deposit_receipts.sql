CREATE TABLE IF NOT EXISTS core.cash_deposit_receipts (
  id BIGSERIAL PRIMARY KEY,
  branch_id INTEGER NOT NULL REFERENCES public.filialen(id),
  deposit_date DATE NOT NULL,
  deposit_time TIME NOT NULL,
  transaction_number VARCHAR(40) NOT NULL,
  terminal_number VARCHAR(40) NOT NULL,
  purpose VARCHAR(120),
  banknote_counts JSONB NOT NULL,
  receipt_total_cents BIGINT NOT NULL CHECK (receipt_total_cents > 0),
  ocr_confidence NUMERIC(5,2) NOT NULL DEFAULT 0 CHECK (ocr_confidence BETWEEN 0 AND 100),
  archive_mode VARCHAR(30) NOT NULL CHECK (archive_mode = 'manual_onedrive'),
  confirmed_by_pin_id BIGINT NOT NULL REFERENCES core.filiale_pins(id),
  confirmed_by_name VARCHAR(200) NOT NULL,
  confirmed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  image_retained BOOLEAN NOT NULL DEFAULT FALSE CHECK (image_retained = FALSE),
  UNIQUE (branch_id,terminal_number,transaction_number)
);

CREATE TABLE IF NOT EXISTS core.cash_deposit_receipt_business_days (
  receipt_id BIGINT NOT NULL REFERENCES core.cash_deposit_receipts(id),
  business_day_id BIGINT NOT NULL REFERENCES core.cash_business_days(id),
  PRIMARY KEY (receipt_id,business_day_id),
  UNIQUE (business_day_id)
);

CREATE INDEX IF NOT EXISTS ix_cash_deposit_receipts_branch_date
  ON core.cash_deposit_receipts (branch_id,deposit_date DESC,deposit_time DESC);

CREATE OR REPLACE FUNCTION core.reject_cash_deposit_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'Einzahlungsbelege sind unveränderbar.';
END;
$$;

DROP TRIGGER IF EXISTS cash_deposit_receipts_immutable ON core.cash_deposit_receipts;
CREATE TRIGGER cash_deposit_receipts_immutable
BEFORE UPDATE OR DELETE ON core.cash_deposit_receipts
FOR EACH ROW EXECUTE FUNCTION core.reject_cash_deposit_receipt_mutation();

DROP TRIGGER IF EXISTS cash_deposit_receipt_links_immutable ON core.cash_deposit_receipt_business_days;
CREATE TRIGGER cash_deposit_receipt_links_immutable
BEFORE UPDATE OR DELETE ON core.cash_deposit_receipt_business_days
FOR EACH ROW EXECUTE FUNCTION core.reject_cash_deposit_receipt_mutation();
