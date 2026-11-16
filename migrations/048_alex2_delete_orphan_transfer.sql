BEGIN;

ALTER TABLE controlling.payment_transfer_events
  DROP CONSTRAINT IF EXISTS payment_transfer_events_event_type_check;

ALTER TABLE controlling.payment_transfer_events
  ADD CONSTRAINT payment_transfer_events_event_type_check
    CHECK (event_type IN (
      'ERFASST', 'VORGESCHLAGEN', 'ANGEPASST', 'BESTAETIGT',
      'ABGELEHNT', 'VERTAGT', 'FEHLBETRAG_GEMERKT',
      'FEHLBETRAG_AUSGEGLICHEN', 'AUSGLEICH_BESTAETIGT', 'RUECKGESETZT',
      'ANBIETER_NORMALISIERT', 'DUBLETTE_BEREINIGT', 'BETRAG_KORRIGIERT',
      'GELOESCHT'
    )) NOT VALID;

DO $orphan_guard$
DECLARE
  target_count integer;
  target_transfer_id uuid;
BEGIN
  SELECT count(*), min(transfer.id::text)::uuid
  INTO target_count, target_transfer_id
  FROM controlling.payment_transfers transfer
  WHERE transfer.buchungsdatum = DATE '2026-11-05'
    AND round(transfer.betrag * 100)::bigint = 1714786
    AND transfer.deleted_at IS NULL;

  IF target_count = 0 AND EXISTS (
    SELECT 1
    FROM controlling.payment_transfer_events event
    WHERE event.event_type = 'GELOESCHT'
      AND event.details ->> 'source' = 'MIGRATION_048'
      AND event.details ->> 'bookingDate' = '2026-11-05'
      AND (event.details ->> 'amountCents')::bigint = 1714786
  ) THEN
    RETURN;
  END IF;

  IF target_count <> 1 THEN
    RAISE EXCEPTION
      'Verwaiste Sammelueberweisung nicht eindeutig: erwartet 1, gefunden %.', target_count;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM controlling.payment_transfer_matches match
    WHERE match.transfer_id = target_transfer_id
      AND match.status = 'bestaetigt'
  ) THEN
    RAISE EXCEPTION
      'Loeschen abgebrochen: Die Sammelueberweisung besitzt einen bestaetigten Match.';
  END IF;

  UPDATE controlling.payment_transfer_matches
  SET status = 'abgelehnt',
      review_note = 'Fehleingabe vor produktiver Bereinigung verworfen',
      reviewed_at = now(),
      updated_at = now()
  WHERE transfer_id = target_transfer_id
    AND status IN ('vorgeschlagen', 'vertagt');

  INSERT INTO controlling.payment_transfer_events (
    transfer_id,
    match_id,
    event_type,
    actor_user_id,
    details
  )
  VALUES (
    target_transfer_id,
    NULL,
    'GELOESCHT',
    NULL,
    jsonb_build_object(
      'source', 'MIGRATION_048',
      'reason', 'Fehleingabe: Tageswerte bereits mit Sammelueberweisung vom 07.11.2026 gematched',
      'bookingDate', '2026-11-05',
      'amountCents', 1714786
    )
  );

  UPDATE controlling.payment_transfers
  SET deleted_at = now(),
      deleted_by = NULL,
      deletion_reason = 'Fehleingabe: Tageswerte bereits mit Sammelueberweisung vom 07.11.2026 gematched',
      status = 'abgelehnt',
      updated_at = now()
  WHERE id = target_transfer_id;
END
$orphan_guard$;

COMMIT;
