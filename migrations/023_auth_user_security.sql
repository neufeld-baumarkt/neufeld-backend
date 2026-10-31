-- Additive Saveroom auth migration. Existing users are preserved.
BEGIN;

ALTER TABLE public.users ADD COLUMN IF NOT EXISTS active boolean DEFAULT true NOT NULL;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS token_version integer DEFAULT 0 NOT NULL;

-- Canonicalize only the five already supported roles before enforcing them.
UPDATE public.users SET role = CASE lower(role)
  WHEN 'admin' THEN 'Admin'
  WHEN 'supervisor' THEN 'Supervisor'
  WHEN 'geschäftsführer' THEN 'Geschäftsführer'
  WHEN 'manager-1' THEN 'Manager-1'
  WHEN 'filiale' THEN 'Filiale'
  ELSE role
END;

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1 FROM public.users
    WHERE role NOT IN ('Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1', 'Filiale')
       OR btrim(name) = '' OR btrim(email) = ''
       OR active IS NULL OR token_version IS NULL OR token_version < 0
  ) THEN
    RAISE EXCEPTION 'Existing users violate the auth constraints; migration stopped without deleting data.';
  END IF;

  ALTER TABLE public.users ALTER COLUMN active SET DEFAULT true;
  ALTER TABLE public.users ALTER COLUMN active SET NOT NULL;
  ALTER TABLE public.users ALTER COLUMN token_version SET DEFAULT 0;
  ALTER TABLE public.users ALTER COLUMN token_version SET NOT NULL;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='users_role_check' AND conrelid='public.users'::regclass) THEN
    ALTER TABLE public.users ADD CONSTRAINT users_role_check
      CHECK (role IN ('Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1', 'Filiale'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='users_token_version_check' AND conrelid='public.users'::regclass) THEN
    ALTER TABLE public.users ADD CONSTRAINT users_token_version_check CHECK (token_version >= 0);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='users_name_not_blank' AND conrelid='public.users'::regclass) THEN
    ALTER TABLE public.users ADD CONSTRAINT users_name_not_blank CHECK (btrim(name) <> '');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='users_email_not_blank' AND conrelid='public.users'::regclass) THEN
    ALTER TABLE public.users ADD CONSTRAINT users_email_not_blank CHECK (btrim(email) <> '');
  END IF;
END
$migration$;

CREATE TABLE IF NOT EXISTS public.user_security_audit (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  occurred_at timestamptz DEFAULT now() NOT NULL,
  actor_user_id integer REFERENCES public.users(id) ON DELETE SET NULL,
  actor_name text,
  target_user_id integer REFERENCES public.users(id) ON DELETE SET NULL,
  target_name text,
  action text NOT NULL CHECK (btrim(action) <> ''),
  details jsonb DEFAULT '{}'::jsonb NOT NULL,
  CONSTRAINT user_security_audit_details_object CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX IF NOT EXISTS user_security_audit_occurred_at_idx
  ON public.user_security_audit (occurred_at DESC);
CREATE INDEX IF NOT EXISTS user_security_audit_target_idx
  ON public.user_security_audit (target_user_id, occurred_at DESC);

-- The runtime may append and read security events, but cannot rewrite history.
DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    GRANT SELECT, INSERT ON public.user_security_audit TO sandbox_app;
    GRANT USAGE, SELECT ON SEQUENCE public.user_security_audit_id_seq TO sandbox_app;
    REVOKE UPDATE, DELETE ON public.user_security_audit FROM sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
