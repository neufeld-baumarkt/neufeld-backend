BEGIN;

CREATE OR REPLACE FUNCTION controlling.notify_alex_module_2_changed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM pg_notify(
    'alex_module_2_changed',
    json_build_object(
      'table', TG_TABLE_NAME,
      'operation', TG_OP,
      'changedAt', clock_timestamp()
    )::text
  );
  RETURN NULL;
END;
$$;

DO $$
DECLARE
  table_name text;
  trigger_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'payment_days',
    'payment_transfers',
    'payment_transfer_matches',
    'payment_transfer_match_items',
    'payment_transfer_difference_carries',
    'payment_day_component_reviews'
  ]
  LOOP
    trigger_name := 'alex2_live_sync_' || table_name;
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON controlling.%I', trigger_name, table_name);
    EXECUTE format(
      'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON controlling.%I FOR EACH STATEMENT EXECUTE FUNCTION controlling.notify_alex_module_2_changed()',
      trigger_name,
      table_name
    );
  END LOOP;
END;
$$;

COMMIT;
