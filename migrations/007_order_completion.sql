-- Additive production migration for the completed, immutable order workflow.

ALTER TABLE "order".order_suppliers
  ADD COLUMN IF NOT EXISTS order_email text,
  ADD COLUMN IF NOT EXISTS minimum_order_ve integer NOT NULL DEFAULT 2;

ALTER TABLE "order".order_suppliers
  DROP CONSTRAINT IF EXISTS order_suppliers_minimum_order_ve_check;
ALTER TABLE "order".order_suppliers
  ADD CONSTRAINT order_suppliers_minimum_order_ve_check CHECK (minimum_order_ve > 0);

ALTER TABLE "order".order_orders
  ADD COLUMN IF NOT EXISTS gesamt_ve integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS dispatch_status text NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS dispatch_mode text NOT NULL DEFAULT 'light',
  ADD COLUMN IF NOT EXISTS dispatch_attempted_at timestamp without time zone,
  ADD COLUMN IF NOT EXISTS dispatch_error text,
  ADD COLUMN IF NOT EXISTS dispatch_to_snapshot text[] NOT NULL DEFAULT ARRAY[]::text[],
  ADD COLUMN IF NOT EXISTS dispatch_cc_snapshot text[] NOT NULL DEFAULT ARRAY[]::text[];

ALTER TABLE "order".order_orders
  DROP CONSTRAINT IF EXISTS order_orders_gesamt_ve_check,
  DROP CONSTRAINT IF EXISTS order_orders_dispatch_status_check,
  DROP CONSTRAINT IF EXISTS order_orders_dispatch_mode_check;
ALTER TABLE "order".order_orders
  ADD CONSTRAINT order_orders_gesamt_ve_check CHECK (gesamt_ve >= 0),
  ADD CONSTRAINT order_orders_dispatch_status_check
    CHECK (dispatch_status IN ('pending', 'sent', 'failed', 'blocked')),
  ADD CONSTRAINT order_orders_dispatch_mode_check
    CHECK (dispatch_mode IN ('light', 'final'));

ALTER TABLE "order".order_order_positions
  ADD COLUMN IF NOT EXISTS kunden_art_nr_snapshot text,
  ADD COLUMN IF NOT EXISTS ek_einzel_snapshot numeric(12,4);

CREATE INDEX IF NOT EXISTS idx_order_orders_dispatch_status
  ON "order".order_orders (dispatch_status);
