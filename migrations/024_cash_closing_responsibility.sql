-- Eine Person darf ihre eigene Kasse abrechnen. Die getrennten Spalten bleiben
-- erhalten, damit Fremdabrechnungen weiterhin eindeutig protokolliert werden.
-- Es werden keine Daten verändert oder gelöscht.
DO $constraint$
DECLARE
  constraint_name text;
BEGIN
  SELECT c.conname INTO constraint_name
  FROM pg_constraint c
  WHERE c.conrelid = 'controlling.cash_drawer_closings'::regclass
    AND c.contype = 'c'
    AND pg_get_constraintdef(c.oid) LIKE '%executing_employee_pin_id%'
    AND pg_get_constraintdef(c.oid) LIKE '%responsible_employee_pin_id%'
  LIMIT 1;

  IF constraint_name IS NOT NULL THEN
    EXECUTE format(
      'ALTER TABLE controlling.cash_drawer_closings DROP CONSTRAINT %I',
      constraint_name
    );
  END IF;
END
$constraint$;
