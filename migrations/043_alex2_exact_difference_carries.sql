BEGIN;

CREATE TABLE IF NOT EXISTS controlling.payment_transfer_difference_carries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_match_id uuid NOT NULL UNIQUE REFERENCES controlling.payment_transfer_matches(id),
  amount_cents bigint NOT NULL CHECK (amount_cents > 0),
  status varchar(20) NOT NULL DEFAULT 'OFFEN' CHECK (status IN ('OFFEN', 'AUSGEGLICHEN')),
  settlement_match_id uuid UNIQUE REFERENCES controlling.payment_transfer_matches(id),
  created_by integer REFERENCES public.users(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  settled_by integer REFERENCES public.users(id),
  settled_at timestamptz,
  CONSTRAINT payment_transfer_difference_carries_settlement_check CHECK (
    (status = 'OFFEN' AND settlement_match_id IS NULL AND settled_by IS NULL AND settled_at IS NULL)
    OR
    (status = 'AUSGEGLICHEN' AND settlement_match_id IS NOT NULL AND settled_by IS NOT NULL AND settled_at IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_payment_transfer_difference_carries_open_amount
  ON controlling.payment_transfer_difference_carries (amount_cents, created_at DESC)
  WHERE status = 'OFFEN';

ALTER TABLE controlling.payment_days
  DROP CONSTRAINT IF EXISTS payment_days_review_status_check;

ALTER TABLE controlling.payment_days
  ADD CONSTRAINT payment_days_review_status_check
    CHECK (review_status IN (
      'OFFEN', 'IN_PRUEFUNG', 'BESTAETIGT', 'KORRIGIERT', 'VERTAGT',
      'AUSGLEICH_AUSSTEHEND', 'EXTERN_BESTAETIGT'
    ));

ALTER TABLE controlling.payment_review_events
  DROP CONSTRAINT IF EXISTS payment_review_events_event_type_check;

ALTER TABLE controlling.payment_review_events
  ADD CONSTRAINT payment_review_events_event_type_check
    CHECK (event_type IN (
      'QLIK_SYNCHRONISIERT', 'IN_PRUEFUNG', 'BESTAETIGT', 'KORRIGIERT',
      'VERTAGT', 'AUSGLEICH_AUSSTEHEND', 'EXTERN_BESTAETIGT', 'RUECKGESETZT'
    ));

ALTER TABLE controlling.payment_transfer_events
  DROP CONSTRAINT IF EXISTS payment_transfer_events_event_type_check;

ALTER TABLE controlling.payment_transfer_events
  ADD CONSTRAINT payment_transfer_events_event_type_check
    CHECK (event_type IN (
      'ERFASST', 'VORGESCHLAGEN', 'ANGEPASST', 'BESTAETIGT',
      'ABGELEHNT', 'VERTAGT', 'FEHLBETRAG_GEMERKT',
      'FEHLBETRAG_AUSGEGLICHEN', 'AUSGLEICH_BESTAETIGT', 'RUECKGESETZT'
    ));

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sandbox_app') THEN
    GRANT SELECT, INSERT, UPDATE ON controlling.payment_transfer_difference_carries TO sandbox_app;
    REVOKE DELETE, TRUNCATE ON controlling.payment_transfer_difference_carries FROM sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
