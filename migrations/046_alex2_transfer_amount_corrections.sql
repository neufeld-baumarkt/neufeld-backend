BEGIN;

ALTER TABLE controlling.payment_transfer_difference_carries
  DROP CONSTRAINT IF EXISTS payment_transfer_difference_carries_status_check;
ALTER TABLE controlling.payment_transfer_difference_carries
  ADD CONSTRAINT payment_transfer_difference_carries_status_check
    CHECK (status IN ('OFFEN', 'AUSGEGLICHEN', 'STORNIERT')) NOT VALID;

ALTER TABLE controlling.payment_transfer_difference_carries
  DROP CONSTRAINT IF EXISTS payment_transfer_difference_carries_settlement_check;
ALTER TABLE controlling.payment_transfer_difference_carries
  ADD CONSTRAINT payment_transfer_difference_carries_settlement_check CHECK (
    (status = 'OFFEN' AND settlement_match_id IS NULL AND settled_by IS NULL AND settled_at IS NULL)
    OR (status = 'AUSGEGLICHEN' AND settlement_match_id IS NOT NULL AND settled_by IS NOT NULL AND settled_at IS NOT NULL)
    OR status = 'STORNIERT'
  ) NOT VALID;

ALTER TABLE controlling.payment_review_events
  DROP CONSTRAINT IF EXISTS payment_review_events_event_type_check;
ALTER TABLE controlling.payment_review_events
  ADD CONSTRAINT payment_review_events_event_type_check CHECK (event_type IN (
    'QLIK_SYNCHRONISIERT', 'IN_PRUEFUNG', 'BESTAETIGT', 'KORRIGIERT',
    'VERTAGT', 'AUSGLEICH_AUSSTEHEND', 'EXTERN_BESTAETIGT', 'RUECKGESETZT',
    'TRANSFER_KORRIGIERT'
  )) NOT VALID;

ALTER TABLE controlling.payment_transfer_events
  DROP CONSTRAINT IF EXISTS payment_transfer_events_event_type_check;
ALTER TABLE controlling.payment_transfer_events
  ADD CONSTRAINT payment_transfer_events_event_type_check CHECK (event_type IN (
    'ERFASST', 'VORGESCHLAGEN', 'ANGEPASST', 'BESTAETIGT',
    'ABGELEHNT', 'VERTAGT', 'FEHLBETRAG_GEMERKT',
    'FEHLBETRAG_AUSGEGLICHEN', 'AUSGLEICH_BESTAETIGT', 'RUECKGESETZT',
    'ANBIETER_NORMALISIERT', 'DUBLETTE_BEREINIGT', 'BETRAG_KORRIGIERT'
  )) NOT VALID;

COMMIT;
