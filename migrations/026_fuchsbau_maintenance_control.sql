-- Fuchsbau – Fox Edition: zentraler Betriebszustand fuer kontrollierte Wartungen.
-- Der globale Session-Zaehler macht bereits ausgegebene Tokens nach dem Wechsel
-- in den Wartungsmodus ungueltig, ohne einzelne Benutzerkonten zu veraendern.

CREATE TABLE IF NOT EXISTS core.app_runtime_control (
  control_key varchar(30) PRIMARY KEY
    CHECK (control_key = 'global'),
  mode varchar(20) NOT NULL DEFAULT 'open'
    CHECK (mode IN ('open', 'draining', 'maintenance')),
  message varchar(500),
  session_epoch integer NOT NULL DEFAULT 0
    CHECK (session_epoch >= 0),
  revision integer NOT NULL DEFAULT 1
    CHECK (revision > 0),
  requested_by_user_id integer REFERENCES public.users(id),
  drain_started_at timestamptz,
  maintenance_started_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (mode = 'open' AND drain_started_at IS NULL AND maintenance_started_at IS NULL)
    OR (mode = 'draining' AND drain_started_at IS NOT NULL AND maintenance_started_at IS NULL)
    OR (mode = 'maintenance' AND drain_started_at IS NOT NULL AND maintenance_started_at IS NOT NULL)
  )
);

INSERT INTO core.app_runtime_control (control_key)
VALUES ('global')
ON CONFLICT (control_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS core.app_runtime_control_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  control_key varchar(30) NOT NULL
    REFERENCES core.app_runtime_control(control_key),
  previous_mode varchar(20) NOT NULL
    CHECK (previous_mode IN ('open', 'draining', 'maintenance')),
  next_mode varchar(20) NOT NULL
    CHECK (next_mode IN ('open', 'draining', 'maintenance')),
  actor_user_id integer NOT NULL REFERENCES public.users(id),
  actor_name varchar(255) NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_app_runtime_control_events_created
  ON core.app_runtime_control_events (created_at DESC);

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    REVOKE INSERT, DELETE, TRUNCATE ON core.app_runtime_control FROM sandbox_app;
    GRANT SELECT, UPDATE ON core.app_runtime_control TO sandbox_app;

    REVOKE UPDATE, DELETE, TRUNCATE ON core.app_runtime_control_events FROM sandbox_app;
    GRANT SELECT, INSERT ON core.app_runtime_control_events TO sandbox_app;
  END IF;
END
$runtime_grants$;
