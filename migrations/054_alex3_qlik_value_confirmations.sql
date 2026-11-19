-- Unveraenderliche One-click-Bestaetigung des jeweils konkreten Qlik-Werts.
-- Eine neue abgeleitete Qlik-Version erhaelt eine neue ID und ist dadurch
-- automatisch wieder offen, ohne historische Bestaetigungen umzuschreiben.

BEGIN;

CREATE TABLE IF NOT EXISTS controlling.alex3_qlik_value_confirmations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  qlik_derived_value_id uuid NOT NULL UNIQUE
    REFERENCES controlling.qlik_derived_daily_values(id),
  confirmed_by_user_id integer NOT NULL REFERENCES public.users(id),
  confirmed_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alex3_qlik_confirmations_time
  ON controlling.alex3_qlik_value_confirmations (confirmed_at DESC);

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    REVOKE UPDATE, DELETE, TRUNCATE
      ON controlling.alex3_qlik_value_confirmations FROM sandbox_app;
    GRANT SELECT, INSERT
      ON controlling.alex3_qlik_value_confirmations TO sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
