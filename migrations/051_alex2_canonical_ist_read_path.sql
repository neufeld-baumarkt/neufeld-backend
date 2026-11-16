BEGIN;

-- Klassik und V2 beziehen IST ausschliesslich aus bestaetigten Match-Belegen.
-- Diese Indizes halten den gemeinsamen, belegbasierten Monats-Lesepfad schlank.
CREATE INDEX IF NOT EXISTS idx_payment_transfer_match_items_day_match
  ON controlling.payment_transfer_match_items (payment_day_id, match_id)
  INCLUDE (betrag);

CREATE INDEX IF NOT EXISTS idx_payment_review_events_external_confirmation
  ON controlling.payment_review_events (payment_day_id, created_at DESC)
  WHERE event_type = 'EXTERN_BESTAETIGT';

COMMIT;
