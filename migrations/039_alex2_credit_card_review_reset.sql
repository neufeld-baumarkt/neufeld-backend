BEGIN;

CREATE TABLE IF NOT EXISTS controlling.module2_maintenance_operations (
  operation_key text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);

DO $reset_credit_card_reviews$
DECLARE
  operation_inserted integer;
BEGIN
  INSERT INTO controlling.module2_maintenance_operations (operation_key)
  VALUES ('2026-11-09_MATCH_ENGINE_SUBSET_CENTS_V1_CORRECTION')
  ON CONFLICT (operation_key) DO NOTHING;

  GET DIAGNOSTICS operation_inserted = ROW_COUNT;
  IF operation_inserted = 0 THEN
    RETURN;
  END IF;

  WITH reset_matches AS (
    UPDATE controlling.payment_transfer_matches match
    SET status = 'abgelehnt',
        reviewed_by = NULL,
        reviewed_at = NULL,
        review_note = 'Einmalige Rücksetzung nach Korrektur der Match-Engine subset-cents-v1.',
        deferred_until = NULL,
        updated_at = now()
    FROM controlling.payment_transfers transfer
    JOIN controlling.payment_types payment_type ON payment_type.id = transfer.payment_type_id
    WHERE match.transfer_id = transfer.id
      AND payment_type.code = 'KREDITKARTE'
      AND match.status IN ('vorgeschlagen', 'bestaetigt', 'vertagt')
    RETURNING match.id, match.transfer_id
  )
  INSERT INTO controlling.payment_transfer_events (transfer_id, match_id, event_type, details)
  SELECT transfer_id, id, 'ABGELEHNT', jsonb_build_object(
    'reset', true,
    'reason', 'MATCH_ENGINE_SUBSET_CENTS_V1_CORRECTION'
  )
  FROM reset_matches;

  UPDATE controlling.payment_transfers transfer
  SET status = 'offen',
      decided_by = NULL,
      decided_at = NULL,
      decision_note = NULL,
      deferred_until = NULL,
      updated_at = now()
  FROM controlling.payment_types payment_type
  WHERE transfer.payment_type_id = payment_type.id
    AND payment_type.code = 'KREDITKARTE';

  UPDATE controlling.payment_days payment_day
  SET review_status = 'OFFEN',
      reviewed_by = NULL,
      reviewed_at = NULL,
      review_note = 'Einmalige Rücksetzung nach Korrektur der Match-Engine subset-cents-v1.',
      deferred_until = NULL,
      updated_at = now()
  FROM controlling.payment_types payment_type
  WHERE payment_day.payment_type_id = payment_type.id
    AND payment_type.code = 'KREDITKARTE';
END
$reset_credit_card_reviews$;

COMMIT;
