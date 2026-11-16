BEGIN;

DO $target_guard$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM controlling.payment_days day
    JOIN controlling.payment_types payment_type
      ON payment_type.id = day.payment_type_id
    JOIN controlling.payment_transfer_match_items item
      ON item.payment_day_id = day.id
    JOIN controlling.payment_transfer_matches match
      ON match.id = item.match_id
    WHERE payment_type.code = 'KREDITKARTE'
      AND day.datum BETWEEN DATE '2026-11-09' AND DATE '2026-11-14'
      AND match.status = 'bestaetigt'
  ) THEN
    RAISE EXCEPTION
      'IST-Bereinigung abgebrochen: Im Zielzeitraum existiert bereits ein bestaetigter Kreditkarten-Match.';
  END IF;
END
$target_guard$;

INSERT INTO controlling.payment_review_events (
  payment_day_id,
  event_type,
  actor_user_id,
  details
)
SELECT
  day.id,
  'RUECKGESETZT',
  NULL,
  jsonb_build_object(
    'source', 'MIGRATION_047',
    'reason', 'Manueller Altbestand ohne bestaetigten Match entfernt',
    'oldIstBetrag', day.ist_betrag
  )
FROM controlling.payment_days day
JOIN controlling.payment_types payment_type
  ON payment_type.id = day.payment_type_id
WHERE payment_type.code = 'KREDITKARTE'
  AND day.datum BETWEEN DATE '2026-11-09' AND DATE '2026-11-14'
  AND day.ist_betrag IS NOT NULL;

UPDATE controlling.payment_days day
SET ist_betrag = NULL,
    updated_at = now()
FROM controlling.payment_types payment_type
WHERE payment_type.id = day.payment_type_id
  AND payment_type.code = 'KREDITKARTE'
  AND day.datum BETWEEN DATE '2026-11-09' AND DATE '2026-11-14'
  AND day.ist_betrag IS NOT NULL;

CREATE OR REPLACE FUNCTION controlling.enforce_match_engine_ist_value()
RETURNS trigger
LANGUAGE plpgsql
AS $match_engine_ist_guard$
BEGIN
  IF NEW.ist_betrag IS NOT NULL AND NOT EXISTS (
    SELECT 1
    FROM controlling.payment_transfer_match_items item
    JOIN controlling.payment_transfer_matches match
      ON match.id = item.match_id
    WHERE item.payment_day_id = NEW.id
      AND item.betrag = NEW.ist_betrag
      AND match.status = 'bestaetigt'
  ) THEN
    RAISE EXCEPTION
      'IST Match-Engine darf nur aus einem bestaetigten, exakt zugeordneten Match stammen.'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END
$match_engine_ist_guard$;

DROP TRIGGER IF EXISTS payment_days_match_engine_ist_guard
  ON controlling.payment_days;

CREATE TRIGGER payment_days_match_engine_ist_guard
BEFORE INSERT OR UPDATE OF ist_betrag ON controlling.payment_days
FOR EACH ROW
EXECUTE FUNCTION controlling.enforce_match_engine_ist_value();

COMMIT;
