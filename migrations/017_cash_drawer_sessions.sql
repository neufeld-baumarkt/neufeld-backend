ALTER TABLE core.cash_drawer_opening_counts
  ADD COLUMN IF NOT EXISTS lifecycle_status TEXT,
  ADD COLUMN IF NOT EXISTS replaced_by_opening_id BIGINT REFERENCES core.cash_drawer_opening_counts(id),
  ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ;

UPDATE core.cash_drawer_opening_counts o
SET lifecycle_status='closed',ended_at=COALESCE(o.ended_at,c.closed_at)
FROM core.cash_drawer_closings c
WHERE c.opening_count_id=o.id AND c.record_kind='original' AND o.lifecycle_status IS NULL;

WITH ranked AS (
  SELECT id,business_date,
    ROW_NUMBER() OVER (PARTITION BY branch_id,business_date,drawer_number ORDER BY completed_at DESC,id DESC) AS row_number
  FROM core.cash_drawer_opening_counts
  WHERE lifecycle_status IS NULL
)
UPDATE core.cash_drawer_opening_counts o
SET lifecycle_status=CASE
    WHEN ranked.business_date<((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date) THEN 'expired'
    WHEN ranked.row_number=1 THEN 'active'
    ELSE 'replaced'
  END,
  ended_at=CASE WHEN ranked.business_date<((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date) OR ranked.row_number>1 THEN COALESCE(o.ended_at,NOW()) ELSE o.ended_at END
FROM ranked WHERE ranked.id=o.id;

ALTER TABLE core.cash_drawer_opening_counts
  ALTER COLUMN lifecycle_status SET DEFAULT 'active',
  ALTER COLUMN lifecycle_status SET NOT NULL;

ALTER TABLE core.cash_drawer_opening_counts
  DROP CONSTRAINT IF EXISTS cash_drawer_opening_counts_lifecycle_status_check;
ALTER TABLE core.cash_drawer_opening_counts
  ADD CONSTRAINT cash_drawer_opening_counts_lifecycle_status_check
  CHECK (lifecycle_status IN ('active','replaced','closed','expired'));

CREATE UNIQUE INDEX IF NOT EXISTS ux_cash_drawer_opening_active_slot
  ON core.cash_drawer_opening_counts (branch_id,business_date,drawer_number)
  WHERE lifecycle_status='active';

CREATE INDEX IF NOT EXISTS ix_cash_drawer_opening_lifecycle
  ON core.cash_drawer_opening_counts (branch_id,business_date DESC,lifecycle_status,completed_at DESC);
