BEGIN;

CREATE TABLE IF NOT EXISTS controlling.qlik_payment_type_mappings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  payment_type_original text NOT NULL CHECK (length(trim(payment_type_original)) > 0),
  target_payment_type_code varchar(50) NOT NULL
    CHECK (target_payment_type_code IN ('EC_CASH', 'KREDITKARTE')),
  amount_multiplier smallint NOT NULL CHECK (amount_multiplier IN (-1, 1)),
  version integer NOT NULL CHECK (version > 0),
  active boolean NOT NULL DEFAULT true,
  evidence_note text NOT NULL CHECK (length(trim(evidence_note)) >= 8),
  approved_by integer NOT NULL REFERENCES public.users(id),
  approved_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (payment_type_original, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_qlik_payment_type_mappings_active
  ON controlling.qlik_payment_type_mappings (lower(payment_type_original))
  WHERE active;

ALTER TABLE controlling.payment_days
  ADD COLUMN IF NOT EXISTS qlik_amount_cents bigint,
  ADD COLUMN IF NOT EXISTS qlik_import_run_id uuid REFERENCES controlling.qlik_import_runs(id),
  ADD COLUMN IF NOT EXISTS availability_status varchar(30) NOT NULL DEFAULT 'AUSSTEHEND',
  ADD COLUMN IF NOT EXISTS comparison_status varchar(30) NOT NULL DEFAULT 'NICHT_VERGLICHEN',
  ADD COLUMN IF NOT EXISTS review_status varchar(30) NOT NULL DEFAULT 'OFFEN',
  ADD COLUMN IF NOT EXISTS reviewed_by integer REFERENCES public.users(id),
  ADD COLUMN IF NOT EXISTS reviewed_at timestamptz,
  ADD COLUMN IF NOT EXISTS review_note text,
  ADD COLUMN IF NOT EXISTS deferred_until date;

ALTER TABLE controlling.payment_days
  DROP CONSTRAINT IF EXISTS payment_days_availability_status_check,
  DROP CONSTRAINT IF EXISTS payment_days_comparison_status_check,
  DROP CONSTRAINT IF EXISTS payment_days_review_status_check,
  DROP CONSTRAINT IF EXISTS payment_days_qlik_provenance_check,
  DROP CONSTRAINT IF EXISTS payment_days_review_metadata_check,
  DROP CONSTRAINT IF EXISTS payment_days_deferred_until_check;

ALTER TABLE controlling.payment_days
  ADD CONSTRAINT payment_days_availability_status_check
    CHECK (availability_status IN ('AUSSTEHEND', 'VORHANDEN', 'UNVOLLSTAENDIG', 'UNGEKLAERTE_ZAHLART')),
  ADD CONSTRAINT payment_days_comparison_status_check
    CHECK (comparison_status IN ('NICHT_VERGLICHEN', 'UEBEREINSTIMMEND', 'ABWEICHEND', 'ZEITVERSATZ_MOEGLICH')),
  ADD CONSTRAINT payment_days_review_status_check
    CHECK (review_status IN ('OFFEN', 'IN_PRUEFUNG', 'BESTAETIGT', 'KORRIGIERT', 'VERTAGT', 'EXTERN_BESTAETIGT')),
  ADD CONSTRAINT payment_days_qlik_provenance_check
    CHECK (
      (qlik_amount_cents IS NULL AND qlik_import_run_id IS NULL)
      OR (qlik_amount_cents IS NOT NULL AND qlik_import_run_id IS NOT NULL)
    ),
  ADD CONSTRAINT payment_days_review_metadata_check
    CHECK (
      (reviewed_by IS NULL AND reviewed_at IS NULL)
      OR (reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL)
    ),
  ADD CONSTRAINT payment_days_deferred_until_check
    CHECK (review_status = 'VERTAGT' OR deferred_until IS NULL);

CREATE INDEX IF NOT EXISTS idx_payment_days_v2_review_queue
  ON controlling.payment_days (review_status, datum DESC, payment_type_id)
  WHERE review_status IN ('OFFEN', 'IN_PRUEFUNG', 'VERTAGT');

CREATE TABLE IF NOT EXISTS controlling.payment_day_qlik_sources (
  payment_day_id uuid NOT NULL REFERENCES controlling.payment_days(id),
  qlik_import_row_id uuid NOT NULL REFERENCES controlling.qlik_import_rows(id),
  mapping_id uuid NOT NULL REFERENCES controlling.qlik_payment_type_mappings(id),
  amount_cents bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (payment_day_id, qlik_import_row_id)
);

CREATE INDEX IF NOT EXISTS idx_payment_day_qlik_sources_mapping
  ON controlling.payment_day_qlik_sources (mapping_id, payment_day_id);

ALTER TABLE controlling.payment_transfers
  ADD COLUMN IF NOT EXISTS provider varchar(20) NOT NULL DEFAULT 'UNBEKANNT',
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS decided_by integer REFERENCES public.users(id),
  ADD COLUMN IF NOT EXISTS decided_at timestamptz,
  ADD COLUMN IF NOT EXISTS decision_note text,
  ADD COLUMN IF NOT EXISTS deferred_until date;

ALTER TABLE controlling.payment_transfers
  DROP CONSTRAINT IF EXISTS payment_transfers_provider_check,
  DROP CONSTRAINT IF EXISTS payment_transfers_status_check,
  DROP CONSTRAINT IF EXISTS payment_transfers_decision_metadata_check,
  DROP CONSTRAINT IF EXISTS payment_transfers_deferred_until_check;

ALTER TABLE controlling.payment_transfers
  ADD CONSTRAINT payment_transfers_provider_check
    CHECK (provider IN ('FIRSTDATA', 'TELECASH', 'UNBEKANNT')),
  ADD CONSTRAINT payment_transfers_status_check
    CHECK (status IN ('offen', 'vorgeschlagen', 'bestaetigt', 'abgelehnt', 'vertagt')),
  ADD CONSTRAINT payment_transfers_decision_metadata_check
    CHECK (
      (decided_by IS NULL AND decided_at IS NULL)
      OR (decided_by IS NOT NULL AND decided_at IS NOT NULL)
    ),
  ADD CONSTRAINT payment_transfers_deferred_until_check
    CHECK (status = 'vertagt' OR deferred_until IS NULL);

CREATE UNIQUE INDEX IF NOT EXISTS ux_payment_transfers_provider_reference
  ON controlling.payment_transfers (provider, referenz)
  WHERE referenz IS NOT NULL;

ALTER TABLE controlling.payment_transfer_matches
  ADD COLUMN IF NOT EXISTS algorithm_version varchar(40) NOT NULL DEFAULT 'subset-cents-v1',
  ADD COLUMN IF NOT EXISTS reviewed_by integer REFERENCES public.users(id),
  ADD COLUMN IF NOT EXISTS reviewed_at timestamptz,
  ADD COLUMN IF NOT EXISTS review_note text,
  ADD COLUMN IF NOT EXISTS deferred_until date,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE controlling.payment_transfer_matches
  DROP CONSTRAINT IF EXISTS payment_transfer_matches_status_check,
  DROP CONSTRAINT IF EXISTS payment_transfer_matches_review_metadata_check,
  DROP CONSTRAINT IF EXISTS payment_transfer_matches_deferred_until_check;

ALTER TABLE controlling.payment_transfer_matches
  ADD CONSTRAINT payment_transfer_matches_status_check
    CHECK (status IN ('vorgeschlagen', 'bestaetigt', 'abgelehnt', 'vertagt')),
  ADD CONSTRAINT payment_transfer_matches_review_metadata_check
    CHECK (
      (reviewed_by IS NULL AND reviewed_at IS NULL)
      OR (reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL)
    ),
  ADD CONSTRAINT payment_transfer_matches_deferred_until_check
    CHECK (status = 'vertagt' OR deferred_until IS NULL);

CREATE UNIQUE INDEX IF NOT EXISTS ux_payment_transfer_matches_active
  ON controlling.payment_transfer_matches (transfer_id)
  WHERE status IN ('vorgeschlagen', 'bestaetigt', 'vertagt');

CREATE TABLE IF NOT EXISTS controlling.payment_review_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  payment_day_id uuid NOT NULL REFERENCES controlling.payment_days(id),
  event_type varchar(40) NOT NULL
    CHECK (event_type IN ('QLIK_SYNCHRONISIERT', 'IN_PRUEFUNG', 'BESTAETIGT', 'KORRIGIERT', 'VERTAGT', 'EXTERN_BESTAETIGT')),
  actor_user_id integer REFERENCES public.users(id),
  details jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payment_review_events_day_time
  ON controlling.payment_review_events (payment_day_id, created_at DESC);

CREATE TABLE IF NOT EXISTS controlling.payment_transfer_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  transfer_id uuid NOT NULL REFERENCES controlling.payment_transfers(id),
  match_id uuid REFERENCES controlling.payment_transfer_matches(id),
  event_type varchar(40) NOT NULL
    CHECK (event_type IN ('ERFASST', 'VORGESCHLAGEN', 'ANGEPASST', 'BESTAETIGT', 'ABGELEHNT', 'VERTAGT')),
  actor_user_id integer REFERENCES public.users(id),
  details jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payment_transfer_events_transfer_time
  ON controlling.payment_transfer_events (transfer_id, created_at DESC);

CREATE OR REPLACE FUNCTION controlling.reject_module2_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $module2_event_guard$
BEGIN
  RAISE EXCEPTION 'Modul-2-Pruefereignisse sind append-only.';
END
$module2_event_guard$;

DROP TRIGGER IF EXISTS payment_review_events_append_only ON controlling.payment_review_events;
CREATE TRIGGER payment_review_events_append_only
BEFORE UPDATE OR DELETE ON controlling.payment_review_events
FOR EACH ROW EXECUTE FUNCTION controlling.reject_module2_event_mutation();

DROP TRIGGER IF EXISTS payment_transfer_events_append_only ON controlling.payment_transfer_events;
CREATE TRIGGER payment_transfer_events_append_only
BEFORE UPDATE OR DELETE ON controlling.payment_transfer_events
FOR EACH ROW EXECUTE FUNCTION controlling.reject_module2_event_mutation();

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sandbox_app') THEN
    GRANT SELECT, INSERT, UPDATE ON
      controlling.qlik_payment_type_mappings,
      controlling.payment_days,
      controlling.payment_day_qlik_sources,
      controlling.payment_transfers,
      controlling.payment_transfer_matches,
      controlling.payment_transfer_match_items
    TO sandbox_app;

    REVOKE DELETE, TRUNCATE ON
      controlling.qlik_payment_type_mappings,
      controlling.payment_days,
      controlling.payment_transfers,
      controlling.payment_transfer_matches,
      controlling.payment_transfer_match_items
    FROM sandbox_app;

    GRANT DELETE ON
      controlling.payment_day_qlik_sources,
      controlling.payment_transfer_match_items
    TO sandbox_app;

    GRANT SELECT, INSERT ON
      controlling.payment_review_events,
      controlling.payment_transfer_events
    TO sandbox_app;

    REVOKE UPDATE, DELETE, TRUNCATE ON
      controlling.payment_review_events,
      controlling.payment_transfer_events
    FROM sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
