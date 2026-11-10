BEGIN;

ALTER TABLE controlling.module2_maintenance_operations
  ADD COLUMN IF NOT EXISTS details jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE controlling.payment_transfers
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS deleted_by integer REFERENCES public.users(id),
  ADD COLUMN IF NOT EXISTS deletion_reason text;

DROP INDEX IF EXISTS controlling.ux_payment_transfers_provider_reference;
CREATE UNIQUE INDEX ux_payment_transfers_provider_reference
  ON controlling.payment_transfers (provider, referenz)
  WHERE referenz IS NOT NULL AND deleted_at IS NULL;

DROP INDEX IF EXISTS controlling.ux_payment_transfers_classic_day;
CREATE UNIQUE INDEX ux_payment_transfers_classic_day
  ON controlling.payment_transfers (payment_type_id, buchungsdatum)
  WHERE provider = 'CLASSIC' AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_payment_transfers_active_month
  ON controlling.payment_transfers (buchungsdatum, payment_type_id)
  WHERE deleted_at IS NULL;

ALTER TABLE controlling.payment_review_events
  DROP CONSTRAINT IF EXISTS payment_review_events_event_type_check;

ALTER TABLE controlling.payment_review_events
  ADD CONSTRAINT payment_review_events_event_type_check
    CHECK (event_type IN (
      'QLIK_SYNCHRONISIERT', 'IN_PRUEFUNG', 'BESTAETIGT', 'KORRIGIERT',
      'VERTAGT', 'EXTERN_BESTAETIGT', 'RUECKGESETZT'
    ));

ALTER TABLE controlling.payment_transfer_events
  DROP CONSTRAINT IF EXISTS payment_transfer_events_event_type_check;

ALTER TABLE controlling.payment_transfer_events
  ADD CONSTRAINT payment_transfer_events_event_type_check
    CHECK (event_type IN (
      'ERFASST', 'VORGESCHLAGEN', 'ANGEPASST', 'BESTAETIGT',
      'ABGELEHNT', 'VERTAGT', 'RUECKGESETZT'
    ));

COMMIT;
