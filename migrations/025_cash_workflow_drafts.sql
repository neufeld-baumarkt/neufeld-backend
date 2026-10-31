-- Unterbrechungssichere, serverseitige Entwuerfe fuer alle Tageskassenprozesse.
-- PINs, Proof-Tokens und andere Geheimnisse werden vom Backend abgewiesen und
-- duerfen nicht im JSON-Zustand gespeichert werden.

CREATE TABLE IF NOT EXISTS controlling.cash_workflow_drafts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  filiale_id integer NOT NULL REFERENCES public.filialen(id),
  business_date date NOT NULL,
  workflow_type varchar(30) NOT NULL
    CHECK (workflow_type IN ('drawer_opening', 'drawer_closing', 'safe_count', 'day_close')),
  draft_key varchar(80) NOT NULL
    CHECK (draft_key ~ '^[A-Za-z0-9:_-]{1,80}$'),
  stage varchar(80) NOT NULL
    CHECK (stage ~ '^[A-Za-z0-9:_-]{1,80}$'),
  state jsonb NOT NULL DEFAULT '{}'::jsonb,
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  status varchar(20) NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'discarded', 'completed')),
  created_by_user_id integer NOT NULL REFERENCES public.users(id),
  updated_by_user_id integer NOT NULL REFERENCES public.users(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  closed_at timestamptz,
  CHECK (jsonb_typeof(state) = 'object'),
  CHECK (
    (status = 'active' AND closed_at IS NULL)
    OR (status IN ('discarded', 'completed') AND closed_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_cash_workflow_drafts_active
  ON controlling.cash_workflow_drafts
    (filiale_id, business_date, workflow_type, draft_key)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_cash_workflow_drafts_lookup
  ON controlling.cash_workflow_drafts
    (filiale_id, business_date, workflow_type, updated_at DESC)
  WHERE status = 'active';

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    REVOKE DELETE, TRUNCATE ON controlling.cash_workflow_drafts FROM sandbox_app;
    GRANT SELECT, INSERT, UPDATE ON controlling.cash_workflow_drafts TO sandbox_app;
  END IF;
END
$runtime_grants$;
