-- Manuelle Klassik-Werte fuer Alex Modul 3 waehrend des gestaffelten Rollouts.
-- Vreden-EZ wird im Backend aus der bestehenden automatischen Kette gelesen
-- und deshalb nicht in dieser Tabelle gespeichert.

BEGIN;

CREATE TABLE IF NOT EXISTS controlling.alex3_manual_daily_values (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  business_date date NOT NULL,
  filiale varchar(50) NOT NULL,
  movement_type varchar(2) NOT NULL CHECK (movement_type IN ('EZ', 'AZ')),
  soll_cents integer CHECK (soll_cents IS NULL OR soll_cents >= 0),
  ist_cents integer CHECK (ist_cents IS NULL OR ist_cents >= 0),
  note text,
  updated_by_user_id integer REFERENCES public.users(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (business_date, filiale, movement_type),
  CHECK (NOT (lower(filiale) = 'vreden' AND movement_type = 'EZ'))
);

CREATE INDEX IF NOT EXISTS idx_alex3_manual_values_month
  ON controlling.alex3_manual_daily_values (business_date, filiale, movement_type);

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    REVOKE DELETE, TRUNCATE ON controlling.alex3_manual_daily_values FROM sandbox_app;
    GRANT SELECT, INSERT, UPDATE ON controlling.alex3_manual_daily_values TO sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
