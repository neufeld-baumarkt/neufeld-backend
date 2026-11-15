BEGIN;

ALTER TABLE controlling.payment_transfer_events
  DROP CONSTRAINT IF EXISTS payment_transfer_events_event_type_check;

ALTER TABLE controlling.payment_transfer_events
  ADD CONSTRAINT payment_transfer_events_event_type_check
    CHECK (event_type IN (
      'ERFASST', 'VORGESCHLAGEN', 'ANGEPASST', 'BESTAETIGT',
      'ABGELEHNT', 'VERTAGT', 'FEHLBETRAG_GEMERKT',
      'FEHLBETRAG_AUSGEGLICHEN', 'AUSGLEICH_BESTAETIGT', 'RUECKGESETZT',
      'ANBIETER_NORMALISIERT', 'DUBLETTE_BEREINIGT'
    )) NOT VALID;

WITH exact_duplicates AS (
  SELECT classic.id AS classic_id, duplicate.id AS duplicate_id
  FROM controlling.payment_transfers classic
  JOIN controlling.payment_types payment_type ON payment_type.id=classic.payment_type_id
  JOIN LATERAL (
    SELECT candidate.id
    FROM controlling.payment_transfers candidate
    WHERE candidate.id<>classic.id
      AND candidate.payment_type_id=classic.payment_type_id
      AND candidate.buchungsdatum=classic.buchungsdatum
      AND candidate.deleted_at IS NULL
      AND candidate.provider=CASE
        WHEN payment_type.code='EC_CASH' THEN 'TELECASH'
        WHEN payment_type.code='KREDITKARTE' THEN 'FIRSTDATA'
      END
      AND round(candidate.betrag*100)::bigint=round(classic.betrag*100)::bigint
    ORDER BY candidate.created_at, candidate.id
    LIMIT 1
  ) duplicate ON true
  WHERE classic.provider='CLASSIC' AND classic.deleted_at IS NULL
)
INSERT INTO controlling.payment_transfer_events
  (transfer_id,event_type,details)
SELECT classic_id,'DUBLETTE_BEREINIGT',jsonb_build_object(
  'replacementTransferId',duplicate_id,'previousProvider','CLASSIC'
)
FROM exact_duplicates;

WITH exact_duplicates AS (
  SELECT classic.id AS classic_id
  FROM controlling.payment_transfers classic
  JOIN controlling.payment_types payment_type ON payment_type.id=classic.payment_type_id
  WHERE classic.provider='CLASSIC' AND classic.deleted_at IS NULL
    AND EXISTS (
      SELECT 1 FROM controlling.payment_transfers candidate
      WHERE candidate.id<>classic.id
        AND candidate.payment_type_id=classic.payment_type_id
        AND candidate.buchungsdatum=classic.buchungsdatum
        AND candidate.deleted_at IS NULL
        AND candidate.provider=CASE
          WHEN payment_type.code='EC_CASH' THEN 'TELECASH'
          WHEN payment_type.code='KREDITKARTE' THEN 'FIRSTDATA'
        END
        AND round(candidate.betrag*100)::bigint=round(classic.betrag*100)::bigint
    )
)
UPDATE controlling.payment_transfers transfer
SET deleted_at=now(),
    deletion_reason='Exakte Dublette bei Zusammenführung von Klassik und V2',
    updated_at=now()
FROM exact_duplicates duplicate
WHERE transfer.id=duplicate.classic_id;

WITH normalized AS (
  UPDATE controlling.payment_transfers transfer
  SET provider=CASE
        WHEN payment_type.code='EC_CASH' THEN 'TELECASH'
        WHEN payment_type.code='KREDITKARTE' THEN 'FIRSTDATA'
      END,
      updated_at=now()
  FROM controlling.payment_types payment_type
  WHERE transfer.payment_type_id=payment_type.id
    AND transfer.provider='CLASSIC'
    AND transfer.deleted_at IS NULL
    AND payment_type.code IN ('EC_CASH','KREDITKARTE')
  RETURNING transfer.id, payment_type.code,
    CASE WHEN payment_type.code='EC_CASH' THEN 'TELECASH' ELSE 'FIRSTDATA' END AS provider
)
INSERT INTO controlling.payment_transfer_events
  (transfer_id,event_type,details)
SELECT id,'ANBIETER_NORMALISIERT',jsonb_build_object(
  'previousProvider','CLASSIC','provider',provider,'paymentType',code
)
FROM normalized;

DROP INDEX IF EXISTS controlling.ux_payment_transfers_classic_day;

COMMIT;
