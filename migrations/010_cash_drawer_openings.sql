CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.cash_drawer_closings (
  id BIGSERIAL PRIMARY KEY,
  branch_id INTEGER NOT NULL REFERENCES public.filialen(id),
  drawer_number SMALLINT NOT NULL CHECK (drawer_number IN (1, 2)),
  employee_pin_id BIGINT REFERENCES core.filiale_pins(id),
  opening_count_id BIGINT,
  opening_balance_cents INTEGER NOT NULL,
  counted_total_cents INTEGER NOT NULL,
  difference_cents INTEGER NOT NULL,
  resolved BOOLEAN NOT NULL DEFAULT FALSE,
  status TEXT NOT NULL DEFAULT 'closed' CHECK (status = 'closed'),
  closed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_cash_drawer_closings_latest
  ON core.cash_drawer_closings (branch_id, drawer_number, closed_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS core.cash_drawer_opening_counts (
  id BIGSERIAL PRIMARY KEY,
  branch_id INTEGER NOT NULL REFERENCES public.filialen(id),
  business_date DATE NOT NULL DEFAULT ((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date),
  drawer_number SMALLINT NOT NULL CHECK (drawer_number IN (1, 2)),
  employee_pin_id BIGINT NOT NULL REFERENCES core.filiale_pins(id),
  final_counter_pin_id BIGINT NOT NULL REFERENCES core.filiale_pins(id),
  target_cents INTEGER NOT NULL DEFAULT 25000 CHECK (target_cents = 25000),
  initial_counted_cents INTEGER NOT NULL,
  initial_difference_cents INTEGER NOT NULL,
  opening_balance_cents INTEGER NOT NULL,
  opening_difference_cents INTEGER NOT NULL,
  previous_closing_id BIGINT REFERENCES core.cash_drawer_closings(id),
  carryover_decision TEXT NOT NULL CHECK (carryover_decision IN ('none', 'keep-in-drawer', 'normalize-to-target')),
  attempts JSONB NOT NULL CHECK (jsonb_typeof(attempts) = 'array' AND jsonb_array_length(attempts) > 0),
  status TEXT NOT NULL DEFAULT 'completed' CHECK (status = 'completed'),
  completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by_user_id INTEGER REFERENCES public.users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_cash_drawer_openings_day
  ON core.cash_drawer_opening_counts (branch_id, business_date DESC, drawer_number, completed_at DESC);

ALTER TABLE core.cash_drawer_closings
  DROP CONSTRAINT IF EXISTS cash_drawer_closings_opening_count_id_fkey;
ALTER TABLE core.cash_drawer_closings
  ADD CONSTRAINT cash_drawer_closings_opening_count_id_fkey
  FOREIGN KEY (opening_count_id) REFERENCES core.cash_drawer_opening_counts(id);
