BEGIN;

CREATE TABLE IF NOT EXISTS controlling.payment_day_component_reviews (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  payment_day_id uuid NOT NULL REFERENCES controlling.payment_days(id),
  component_code varchar(40) NOT NULL
    CHECK (component_code IN ('EC_CASH', 'AMEX', 'MAESTRO', 'MASTERCARD', 'VISA')),
  component_label varchar(80) NOT NULL CHECK (length(trim(component_label)) > 0),
  amount_cents bigint NOT NULL CHECK (amount_cents >= 0),
  source_fingerprint char(64) NOT NULL,
  review_status varchar(30) NOT NULL DEFAULT 'OFFEN'
    CHECK (review_status IN ('OFFEN', 'BESTAETIGT')),
  reviewed_by integer REFERENCES public.users(id),
  reviewed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT payment_day_component_reviews_unique UNIQUE (payment_day_id, component_code),
  CONSTRAINT payment_day_component_reviews_metadata_check CHECK (
    (reviewed_by IS NULL AND reviewed_at IS NULL)
    OR (reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_payment_day_component_reviews_day
  ON controlling.payment_day_component_reviews (payment_day_id, review_status, component_code);

WITH component_source AS (
  SELECT pd.id AS payment_day_id, component.code, component.label,
         COALESCE(abs(sum(raw.amount_cents)), 0)::bigint AS amount_cents,
         encode(digest(
           pd.qlik_derived_value_id::text || '|' || component.code || '|' ||
           COALESCE(string_agg(raw.id::text, ',' ORDER BY raw.id::text), 'NO_SOURCE') || '|' ||
           COALESCE(abs(sum(raw.amount_cents)), 0)::bigint::text,
           'sha256'
         ), 'hex') AS source_fingerprint
  FROM controlling.payment_days pd
  JOIN controlling.payment_types payment_type ON payment_type.id=pd.payment_type_id
  JOIN LATERAL (
    SELECT * FROM (VALUES
      ('EC_CASH', 'EC-Kartenzahlung', 'Kartenzahlung IST', 'EC_CASH'),
      ('AMEX', 'Amex', 'American Express IST', 'KREDITKARTE'),
      ('MAESTRO', 'Maestro', 'Maestro IST', 'KREDITKARTE'),
      ('MASTERCARD', 'Mastercard', 'Mastercard IST', 'KREDITKARTE'),
      ('VISA', 'Visa', 'Visa IST', 'KREDITKARTE')
    ) AS definitions(code, label, original_type, target_payment_type)
    WHERE definitions.target_payment_type=payment_type.code
  ) component ON true
  LEFT JOIN controlling.qlik_derived_value_sources source
    ON source.derived_value_id=pd.qlik_derived_value_id
  LEFT JOIN controlling.qlik_import_rows raw
    ON raw.id=source.qlik_import_row_id
   AND raw.payment_type_original=component.original_type
  WHERE pd.qlik_derived_value_id IS NOT NULL
    AND payment_type.code IN ('EC_CASH', 'KREDITKARTE')
  GROUP BY pd.id, pd.qlik_derived_value_id, component.code, component.label
)
INSERT INTO controlling.payment_day_component_reviews (
  payment_day_id, component_code, component_label, amount_cents, source_fingerprint
)
SELECT payment_day_id, code, label, amount_cents, source_fingerprint
FROM component_source
ON CONFLICT (payment_day_id, component_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS controlling.payment_day_component_review_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  component_review_id uuid NOT NULL REFERENCES controlling.payment_day_component_reviews(id),
  event_type varchar(40) NOT NULL
    CHECK (event_type IN ('QLIK_SYNCHRONISIERT', 'BESTAETIGT')),
  actor_user_id integer REFERENCES public.users(id),
  details jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payment_day_component_review_events_component
  ON controlling.payment_day_component_review_events (component_review_id, created_at DESC);

DROP TRIGGER IF EXISTS payment_day_component_review_events_append_only
  ON controlling.payment_day_component_review_events;
CREATE TRIGGER payment_day_component_review_events_append_only
BEFORE UPDATE OR DELETE ON controlling.payment_day_component_review_events
FOR EACH ROW EXECUTE FUNCTION controlling.reject_module2_event_mutation();

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sandbox_app') THEN
    GRANT SELECT, INSERT, UPDATE ON controlling.payment_day_component_reviews TO sandbox_app;
    REVOKE DELETE, TRUNCATE ON controlling.payment_day_component_reviews FROM sandbox_app;

    GRANT SELECT, INSERT ON controlling.payment_day_component_review_events TO sandbox_app;
    REVOKE UPDATE, DELETE, TRUNCATE ON controlling.payment_day_component_review_events FROM sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
