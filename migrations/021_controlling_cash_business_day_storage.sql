-- Persistente Grundlage fuer den Tageskassenablauf.
-- Diese Migration speichert Eingaben und Backend-Ergebnisse, fuehrt aber bewusst
-- keine fachlichen Berechnungen aus. EC- und Kreditkartendaten gehoeren nicht
-- zu diesem Ablauf und sind deshalb nicht Bestandteil dieses Modells.

CREATE TABLE IF NOT EXISTS controlling.cash_business_days (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  filiale_id integer NOT NULL REFERENCES public.filialen(id),
  business_date date NOT NULL,
  status varchar(20) NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'closing', 'final')),
  workflow_version integer NOT NULL DEFAULT 2 CHECK (workflow_version > 0),
  total_bank_payout_cents integer CHECK (total_bank_payout_cents >= 0),
  loose_change_payout_confirmed boolean NOT NULL DEFAULT false,
  additional_safe_skim_confirmed boolean NOT NULL DEFAULT false,
  bank_payout_confirmed boolean NOT NULL DEFAULT false,
  safe_bag_confirmed boolean NOT NULL DEFAULT false,
  created_by_user_id integer REFERENCES public.users(id),
  finalized_by_user_id integer REFERENCES public.users(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  finalized_at timestamptz,
  UNIQUE (filiale_id, business_date),
  CHECK (
    (status <> 'final' AND finalized_at IS NULL AND finalized_by_user_id IS NULL)
    OR
    (status = 'final' AND finalized_at IS NOT NULL AND finalized_by_user_id IS NOT NULL)
  )
);

-- Die produktive Bestandsdatenbank verwendet bigint, die lokale isolierte
-- Testgrundlage uuid. Die FK-Spalte uebernimmt deshalb exakt den vorhandenen
-- Primaerschluesseltyp von core.cash_safe_counts.
DO $safe_count_reference$
DECLARE
  safe_count_id_type text;
BEGIN
  SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
  INTO safe_count_id_type
  FROM pg_attribute a
  WHERE a.attrelid = 'core.cash_safe_counts'::regclass
    AND a.attname = 'id'
    AND a.attnum > 0
    AND NOT a.attisdropped;

  IF safe_count_id_type IS NULL THEN
    RAISE EXCEPTION 'core.cash_safe_counts.id is missing';
  END IF;

  EXECUTE format(
    'ALTER TABLE controlling.cash_business_days ADD COLUMN IF NOT EXISTS final_safe_count_id %s REFERENCES core.cash_safe_counts(id)',
    safe_count_id_type
  );
END
$safe_count_reference$;

CREATE TABLE IF NOT EXISTS controlling.branch_daily_metrics (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  business_day_id uuid NOT NULL UNIQUE
    REFERENCES controlling.cash_business_days(id),
  daily_revenue_cents integer NOT NULL CHECK (daily_revenue_cents >= 0),
  customer_count integer NOT NULL CHECK (customer_count >= 0),
  reported_average_receipt_cents integer NOT NULL
    CHECK (reported_average_receipt_cents >= 0),
  calculated_average_receipt_cents integer
    CHECK (calculated_average_receipt_cents >= 0),
  average_receipt_difference_cents integer,
  eurocontrol_bank_cents integer NOT NULL CHECK (eurocontrol_bank_cents >= 0),
  source_system varchar(30) NOT NULL DEFAULT 'eurocontrol'
    CHECK (source_system = 'eurocontrol'),
  status varchar(20) NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'final')),
  captured_by_user_id integer REFERENCES public.users(id),
  updated_by_user_id integer REFERENCES public.users(id),
  finalized_by_user_id integer REFERENCES public.users(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  finalized_at timestamptz,
  CHECK (
    (status = 'draft' AND finalized_at IS NULL AND finalized_by_user_id IS NULL)
    OR
    (status = 'final'
      AND calculated_average_receipt_cents IS NOT NULL
      AND average_receipt_difference_cents IS NOT NULL
      AND finalized_at IS NOT NULL
      AND finalized_by_user_id IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS controlling.cash_drawer_openings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  business_day_id uuid NOT NULL REFERENCES controlling.cash_business_days(id),
  drawer_number smallint NOT NULL CHECK (drawer_number IN (1, 2)),
  employee_pin_id bigint NOT NULL REFERENCES core.filiale_pins(id),
  second_counter_pin_id bigint REFERENCES core.filiale_pins(id),
  counted_total_cents integer NOT NULL CHECK (counted_total_cents >= 0),
  target_float_cents integer NOT NULL DEFAULT 25000 CHECK (target_float_cents >= 0),
  difference_to_target_cents integer NOT NULL,
  previous_closing_id uuid,
  carryover_decision varchar(30) NOT NULL DEFAULT 'none'
    CHECK (carryover_decision IN ('none', 'keep_in_drawer', 'adjust_to_target')),
  attempt_count integer NOT NULL DEFAULT 1 CHECK (attempt_count > 0),
  status varchar(20) NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'confirmed')),
  confirmed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (business_day_id, drawer_number),
  CHECK (second_counter_pin_id IS NULL OR second_counter_pin_id <> employee_pin_id),
  CHECK (
    (status = 'draft' AND confirmed_at IS NULL)
    OR (status = 'confirmed' AND confirmed_at IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS controlling.cash_drawer_opening_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  opening_id uuid NOT NULL REFERENCES controlling.cash_drawer_openings(id),
  attempt_number integer NOT NULL CHECK (attempt_number > 0),
  counted_by_pin_id bigint NOT NULL REFERENCES core.filiale_pins(id),
  denomination_counts jsonb NOT NULL,
  counted_total_cents integer NOT NULL CHECK (counted_total_cents >= 0),
  target_float_cents integer NOT NULL CHECK (target_float_cents >= 0),
  difference_to_target_cents integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (opening_id, attempt_number),
  CHECK (jsonb_typeof(denomination_counts) = 'object')
);

CREATE TABLE IF NOT EXISTS controlling.cash_drawer_closings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  business_day_id uuid NOT NULL REFERENCES controlling.cash_business_days(id),
  opening_id uuid REFERENCES controlling.cash_drawer_openings(id),
  drawer_number smallint NOT NULL CHECK (drawer_number IN (1, 2)),
  executing_employee_pin_id bigint NOT NULL REFERENCES core.filiale_pins(id),
  responsible_employee_pin_id bigint NOT NULL REFERENCES core.filiale_pins(id),
  is_last_cash boolean NOT NULL DEFAULT false,
  proceeded_without_opening boolean NOT NULL DEFAULT false,
  denomination_counts jsonb NOT NULL,
  counted_total_cents integer NOT NULL CHECK (counted_total_cents >= 0),
  coin_total_cents integer NOT NULL CHECK (coin_total_cents >= 0),
  retained_coin_cents integer NOT NULL CHECK (retained_coin_cents >= 0),
  loose_change_cents integer NOT NULL CHECK (loose_change_cents >= 0),
  banknotes_needed_cents integer NOT NULL CHECK (banknotes_needed_cents >= 0),
  banknote_counts jsonb NOT NULL,
  bank_amount_cents integer NOT NULL CHECK (bank_amount_cents >= 0),
  eurocontrol_difference_cents integer NOT NULL,
  difference_reviewed boolean NOT NULL DEFAULT false,
  notification_confirmed boolean NOT NULL DEFAULT false,
  attempt_count integer NOT NULL DEFAULT 1 CHECK (attempt_count > 0),
  status varchar(20) NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'confirmed')),
  confirmed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (business_day_id, drawer_number),
  CHECK (executing_employee_pin_id <> responsible_employee_pin_id),
  CHECK (opening_id IS NOT NULL OR proceeded_without_opening),
  CHECK (jsonb_typeof(denomination_counts) = 'object'),
  CHECK (jsonb_typeof(banknote_counts) = 'object'),
  CHECK (
    (status = 'draft' AND confirmed_at IS NULL)
    OR (status = 'confirmed' AND confirmed_at IS NOT NULL)
  )
);

DO $constraint$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'cash_drawer_openings_previous_closing_fkey'
      AND conrelid = 'controlling.cash_drawer_openings'::regclass
  ) THEN
    ALTER TABLE controlling.cash_drawer_openings
      ADD CONSTRAINT cash_drawer_openings_previous_closing_fkey
      FOREIGN KEY (previous_closing_id)
      REFERENCES controlling.cash_drawer_closings(id);
  END IF;
END
$constraint$;

CREATE TABLE IF NOT EXISTS controlling.cash_drawer_closing_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  closing_id uuid NOT NULL REFERENCES controlling.cash_drawer_closings(id),
  attempt_number integer NOT NULL CHECK (attempt_number > 0),
  counted_by_pin_id bigint NOT NULL REFERENCES core.filiale_pins(id),
  denomination_counts jsonb NOT NULL,
  counted_total_cents integer NOT NULL CHECK (counted_total_cents >= 0),
  coin_total_cents integer NOT NULL CHECK (coin_total_cents >= 0),
  retained_coin_cents integer NOT NULL CHECK (retained_coin_cents >= 0),
  loose_change_cents integer NOT NULL CHECK (loose_change_cents >= 0),
  banknotes_needed_cents integer NOT NULL CHECK (banknotes_needed_cents >= 0),
  banknote_counts jsonb NOT NULL,
  bank_amount_cents integer NOT NULL CHECK (bank_amount_cents >= 0),
  eurocontrol_difference_cents integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (closing_id, attempt_number),
  CHECK (jsonb_typeof(denomination_counts) = 'object'),
  CHECK (jsonb_typeof(banknote_counts) = 'object')
);

CREATE TABLE IF NOT EXISTS controlling.cash_business_day_approvals (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  business_day_id uuid NOT NULL REFERENCES controlling.cash_business_days(id),
  approval_sequence smallint NOT NULL CHECK (approval_sequence IN (1, 2)),
  employee_pin_id bigint NOT NULL REFERENCES core.filiale_pins(id),
  employee_name_snapshot varchar(200) NOT NULL
    CHECK (length(trim(employee_name_snapshot)) > 0),
  employee_role_snapshot varchar(80) NOT NULL
    CHECK (length(trim(employee_role_snapshot)) > 0),
  approved_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (business_day_id, approval_sequence),
  UNIQUE (business_day_id, employee_pin_id)
);

CREATE TABLE IF NOT EXISTS controlling.cash_business_day_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  business_day_id uuid NOT NULL REFERENCES controlling.cash_business_days(id),
  event_type varchar(80) NOT NULL CHECK (length(trim(event_type)) > 0),
  actor_user_id integer REFERENCES public.users(id),
  actor_employee_pin_id bigint REFERENCES core.filiale_pins(id),
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(details) = 'object')
);

CREATE TABLE IF NOT EXISTS controlling.cash_deposit_receipt_closings (
  receipt_id uuid NOT NULL REFERENCES controlling.cash_deposit_receipts(id),
  closing_id uuid NOT NULL REFERENCES controlling.cash_drawer_closings(id),
  PRIMARY KEY (receipt_id, closing_id),
  UNIQUE (closing_id)
);

CREATE INDEX IF NOT EXISTS idx_cash_business_days_date_branch
  ON controlling.cash_business_days (business_date DESC, filiale_id);
CREATE INDEX IF NOT EXISTS idx_cash_drawer_openings_business_day
  ON controlling.cash_drawer_openings (business_day_id, drawer_number);
CREATE INDEX IF NOT EXISTS idx_cash_drawer_closings_business_day
  ON controlling.cash_drawer_closings (business_day_id, drawer_number);
CREATE INDEX IF NOT EXISTS idx_cash_business_day_events_day_time
  ON controlling.cash_business_day_events (business_day_id, created_at);

-- Entwuerfe duerfen durch das Backend fortgeschrieben werden. Historische
-- Versuche, Freigaben, Ereignisse und Belegzuordnungen bleiben append-only.
DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    REVOKE DELETE, TRUNCATE ON
      controlling.cash_business_days,
      controlling.branch_daily_metrics,
      controlling.cash_drawer_openings,
      controlling.cash_drawer_closings
    FROM sandbox_app;
    GRANT SELECT, INSERT, UPDATE ON
      controlling.cash_business_days,
      controlling.branch_daily_metrics,
      controlling.cash_drawer_openings,
      controlling.cash_drawer_closings
    TO sandbox_app;

    REVOKE UPDATE, DELETE, TRUNCATE ON
      controlling.cash_drawer_opening_attempts,
      controlling.cash_drawer_closing_attempts,
      controlling.cash_business_day_approvals,
      controlling.cash_business_day_events,
      controlling.cash_deposit_receipt_closings
    FROM sandbox_app;
    GRANT SELECT, INSERT ON
      controlling.cash_drawer_opening_attempts,
      controlling.cash_drawer_closing_attempts,
      controlling.cash_business_day_approvals,
      controlling.cash_business_day_events,
      controlling.cash_deposit_receipt_closings
    TO sandbox_app;
  END IF;
END
$runtime_grants$;
