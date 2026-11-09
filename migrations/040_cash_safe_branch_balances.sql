-- Verbindliche filialabhaengige Tresor-Zielbestaende.
-- Der Gesamtbestand umfasst Kassenladen, Rollengeld und – nur in Vreden –
-- Little Bertha. Die Migration stoppt vor jeder Aenderung, wenn der
-- produktive Filialstamm nicht genau den vier erwarteten Treffern entspricht.

DO $cash_safe_branch_preflight$
DECLARE
  matching_branches integer;
BEGIN
  SELECT COUNT(*)
  INTO matching_branches
  FROM public.filialen
  WHERE LOWER(TRIM(name)) IN ('ahaus', 'münster', 'telgte', 'vreden');

  IF matching_branches <> 4 THEN
    RAISE EXCEPTION
      'Tresor-Zielbestaende nicht migriert: erwartet wurden genau Ahaus, Münster, Telgte und Vreden.';
  END IF;
END
$cash_safe_branch_preflight$;

INSERT INTO core.cash_safe_settings (
  branch_id,
  standard_balance_cents,
  fixed_drawer_cents,
  little_bertha_cents,
  updated_at
)
SELECT
  id,
  CASE LOWER(TRIM(name))
    WHEN 'ahaus' THEN 200000
    WHEN 'münster' THEN 100000
    WHEN 'telgte' THEN 150000
    WHEN 'vreden' THEN 250000
  END,
  50000,
  CASE WHEN LOWER(TRIM(name)) = 'vreden' THEN 100000 ELSE 0 END,
  NOW()
FROM public.filialen
WHERE LOWER(TRIM(name)) IN ('ahaus', 'münster', 'telgte', 'vreden')
ON CONFLICT (branch_id) DO UPDATE SET
  standard_balance_cents = EXCLUDED.standard_balance_cents,
  fixed_drawer_cents = EXCLUDED.fixed_drawer_cents,
  little_bertha_cents = EXCLUDED.little_bertha_cents,
  updated_at = NOW();

DO $cash_safe_branch_verification$
BEGIN
  IF (
    SELECT COUNT(*)
    FROM core.cash_safe_settings settings
    JOIN public.filialen branch ON branch.id = settings.branch_id
    WHERE (LOWER(TRIM(branch.name)), settings.standard_balance_cents,
           settings.fixed_drawer_cents, settings.little_bertha_cents) IN (
      ('ahaus',   200000, 50000,      0),
      ('münster', 100000, 50000,      0),
      ('telgte',  150000, 50000,      0),
      ('vreden',  250000, 50000, 100000)
    )
  ) <> 4 THEN
    RAISE EXCEPTION 'Tresor-Zielbestaende konnten nicht vollstaendig verifiziert werden.';
  END IF;
END
$cash_safe_branch_verification$;
