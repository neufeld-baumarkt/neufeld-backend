BEGIN;

-- The original Modul-2 tables predate the integer primary key used by
-- public.users and therefore carried an unusable UUID creator column.
-- Keep the legacy column for traceability and store all new actors in the
-- correctly typed, constrained column.
ALTER TABLE controlling.payment_transfers
  ADD COLUMN IF NOT EXISTS created_by_user_id integer REFERENCES public.users(id);

CREATE INDEX IF NOT EXISTS idx_payment_transfers_created_by_user
  ON controlling.payment_transfers (created_by_user_id)
  WHERE created_by_user_id IS NOT NULL;

COMMIT;
