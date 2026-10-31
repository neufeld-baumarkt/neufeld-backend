CREATE TABLE IF NOT EXISTS controlling.cash_deposit_receipts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  filiale varchar(50) NOT NULL,
  deposit_date date NOT NULL,
  deposit_time time NOT NULL,
  transaction_number varchar(40) NOT NULL,
  terminal_number varchar(40) NOT NULL,
  purpose varchar(120),
  banknote_counts jsonb NOT NULL,
  receipt_total_cents integer NOT NULL CHECK (receipt_total_cents > 0),
  ocr_confidence numeric(5,2) NOT NULL DEFAULT 0 CHECK (ocr_confidence BETWEEN 0 AND 100),
  archive_mode varchar(30) NOT NULL CHECK (archive_mode = 'manual_onedrive'),
  confirmed_by_employee_id bigint NOT NULL REFERENCES core.filiale_pins(id),
  confirmed_by_employee_name varchar(200) NOT NULL,
  confirmed_at timestamptz NOT NULL DEFAULT now(),
  image_retained boolean NOT NULL DEFAULT false CHECK (image_retained = false),
  UNIQUE (filiale, terminal_number, transaction_number)
);

CREATE INDEX IF NOT EXISTS idx_cash_deposit_receipts_branch_date
  ON controlling.cash_deposit_receipts (filiale, deposit_date DESC, deposit_time DESC);

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    REVOKE UPDATE, DELETE ON controlling.cash_deposit_receipts FROM sandbox_app;
    GRANT SELECT, INSERT ON controlling.cash_deposit_receipts TO sandbox_app;
  END IF;
END
$runtime_grants$;
