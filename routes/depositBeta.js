const { depositBetaConfig, depositCycle, depositWeek, depositScope, depositStatuses } = require('../lib/depositBeta');
const { berlinDate, sellingWeek } = require('../lib/sellingWeek');

const controllingStatusQuery = `
  SELECT f.name AS branch, to_char(d.business_date, 'YYYY-MM-DD') AS date,
    d.status = 'final' AS final, c.bank_amount_cents,
    r.confirmed_at IS NOT NULL AS verified, r.receipt_total_cents, r.banknote_counts,
    receipt_links.linked_total_cents, receipt_links.valid_links
  FROM public.filialen f
  LEFT JOIN controlling.cash_business_days d
    ON d.filiale_id = f.id AND d.business_date = ANY($2::date[])
  LEFT JOIN controlling.cash_drawer_closings c
    ON c.business_day_id = d.id AND c.status = 'confirmed'
  LEFT JOIN controlling.cash_deposit_receipt_closings link ON link.closing_id = c.id
  LEFT JOIN controlling.cash_deposit_receipts r
    ON r.id = link.receipt_id AND LOWER(r.filiale) = LOWER(f.name)
  LEFT JOIN LATERAL (
    SELECT SUM(other.bank_amount_cents) AS linked_total_cents,
      BOOL_AND(other_day.filiale_id = f.id AND other.status = 'confirmed') AS valid_links
    FROM controlling.cash_deposit_receipt_closings other_link
    JOIN controlling.cash_drawer_closings other ON other.id = other_link.closing_id
    JOIN controlling.cash_business_days other_day ON other_day.id = other.business_day_id
    WHERE other_link.receipt_id = r.id
  ) receipt_links ON TRUE
  WHERE f.aktiv = true AND ($1::text IS NULL OR LOWER(f.name) = LOWER($1))
  ORDER BY f.name, d.business_date, c.id
`;

// 3.0's cash archive verifies a finalized business day against its linked receipt.
const coreStatusQuery = `
  SELECT f.name AS branch, TO_CHAR(d.business_date, 'YYYY-MM-DD') AS date,
    d.status = 'finalized' AS final,
    (d.final_report->'totals'->>'bankPayoutCents')::bigint AS bank_amount_cents,
    r.confirmed_at IS NOT NULL AS verified, r.receipt_total_cents, r.banknote_counts,
    receipt_links.linked_total_cents, receipt_links.valid_links
  FROM public.filialen f
  LEFT JOIN core.cash_business_days d
    ON d.branch_id = f.id AND d.business_date = ANY($2::date[])
  LEFT JOIN core.cash_deposit_receipt_business_days link ON link.business_day_id = d.id
  LEFT JOIN core.cash_deposit_receipts r ON r.id = link.receipt_id AND r.branch_id = f.id
  LEFT JOIN LATERAL (
    SELECT SUM((other.final_report->'totals'->>'bankPayoutCents')::bigint) AS linked_total_cents,
      BOOL_AND(other.branch_id = f.id AND other.status = 'finalized'
        AND other.final_report->'totals'->>'bankPayoutCents' IS NOT NULL) AS valid_links
    FROM core.cash_deposit_receipt_business_days other_link
    JOIN core.cash_business_days other ON other.id = other_link.business_day_id
    WHERE other_link.receipt_id = r.id
  ) receipt_links ON TRUE
  WHERE f.aktiv = true AND ($1::text IS NULL OR LOWER(f.name) = LOWER($1))
  ORDER BY f.name, d.business_date, d.id
`;

function createDepositBetaHandler({ pool, config, now = () => new Date() }) {
  const statusQuery = config.source === 'core' ? coreStatusQuery : controllingStatusQuery;
  return async (req, res) => {
    res.set('Cache-Control', 'private, no-store');
    const scope = depositScope(config, req.user);
    if (!scope) return res.sendStatus(404);
    try {
      const instant = now();
      const today = berlinDate.format(instant);
      const week = sellingWeek(today, scope.branch ? undefined : req.query?.week);
      if (!week) return res.status(400).json({ message: 'Bitte den Montag der Kalenderwoche als ISO-Datum angeben.' });
      const liveWeek = week.start === sellingWeek(today).start;
      const current = depositCycle(instant, config.holidays);
      const cycles = scope.branch ? [current] : depositWeek(instant, config.holidays, week.start);
      const dates = [...new Set(cycles.flatMap(cycle => cycle.dates))];
      const { rows } = await pool.query(statusQuery, [scope.branch, dates]);
      return res.json({
        ...(scope.branch ? {} : { week }),
        currentDueDate: liveWeek ? current.dueDate : null,
        cycles: cycles.map(cycle => ({ dueDate: cycle.dueDate, branches: depositStatuses(rows, cycle) })),
      });
    } catch {
      return res.status(503).json({ message: 'Einzahlungsstatus vorübergehend nicht verfügbar.' });
    }
  };
}

function mountDepositBeta(app, { pool, env, verifyToken, now, reportError = console.error }) {
  let config;
  try {
    config = depositBetaConfig(env);
  } catch (error) {
    reportError(error.message);
    return;
  }
  if (config) {
    app.get('/api/deposit-beta', verifyToken(), createDepositBetaHandler({ pool, config, now }));
  }
}

module.exports = { createDepositBetaHandler, mountDepositBeta };
