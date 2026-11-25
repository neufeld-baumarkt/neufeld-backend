const { berlinDate, sellingWeek } = require('../lib/sellingWeek');

const branches = ['Ahaus', 'Telgte', 'Vreden', 'Münster'];
const types = ['EC_CASH', 'KREDITKARTE'];

const statusQuery = `
  SELECT f.name AS branch, to_char(days.day, 'YYYY-MM-DD') AS day, kind.code,
         q.id IS NOT NULL AS imported,
         COALESCE(review.event_type='BESTAETIGT' AND review.actor_user_id=$3, false) AS reviewed,
         EXISTS (
           SELECT 1 FROM controlling.payment_transfer_match_items item
           JOIN controlling.payment_transfer_matches m ON m.id=item.match_id AND m.status='bestaetigt'
           JOIN controlling.payment_transfers transfer ON transfer.id=m.transfer_id
             AND transfer.deleted_at IS NULL AND transfer.status='bestaetigt'
           WHERE item.payment_day_id=pd.id
             AND m.reviewed_at >= COALESCE(sync.at, '-infinity'::timestamptz)
             AND (m.differenz=0 OR EXISTS (
               SELECT 1 FROM controlling.payment_transfer_difference_carries carry
               JOIN controlling.payment_transfer_matches settlement ON settlement.id=carry.settlement_match_id
                 AND settlement.status='bestaetigt'
               JOIN controlling.payment_transfers settlement_transfer ON settlement_transfer.id=settlement.transfer_id
                 AND settlement_transfer.deleted_at IS NULL AND settlement_transfer.status='bestaetigt'
               WHERE carry.source_match_id=m.id AND carry.status='AUSGEGLICHEN'
             ))
         ) AS matched
  FROM public.filialen f
  CROSS JOIN unnest($1::date[]) AS days(day)
  CROSS JOIN (VALUES ('EC_CASH','EC_ZAHLUNGEN'), ('KREDITKARTE','KREDITKARTEN_ZAHLUNGEN')) AS kind(code,metric)
  LEFT JOIN controlling.canonical_daily_metric_values q ON q.branch_id=f.id
    AND q.business_date=days.day AND q.metric_code=kind.metric AND q.selected_source='QLIK'
  LEFT JOIN controlling.payment_types pt ON pt.code=kind.code
  LEFT JOIN controlling.payment_days pd ON pd.payment_type_id=pt.id AND pd.filiale=f.name AND pd.datum=days.day
    AND pd.canonical_source_system='QLIK' AND pd.canonical_source_value_id=q.id
    AND pd.qlik_derived_value_id=q.id AND pd.qlik_amount_cents=q.numeric_value
  LEFT JOIN LATERAL (
    SELECT max(event.created_at) AS at FROM controlling.payment_review_events event
    WHERE event.payment_day_id=pd.id AND event.event_type IN ('QLIK_SYNCHRONISIERT','EUROCONTROL_SYNCHRONISIERT')
  ) sync ON true
  LEFT JOIN LATERAL (
    SELECT event.event_type,event.actor_user_id FROM controlling.payment_review_events event
    WHERE event.payment_day_id=pd.id AND event.created_at >= COALESCE(sync.at, '-infinity'::timestamptz)
      AND event.event_type IN ('BESTAETIGT','IN_PRUEFUNG','KORRIGIERT','VERTAGT')
    ORDER BY event.created_at DESC,event.id DESC LIMIT 1
  ) review ON true
  WHERE f.name=ANY($2::text[])
`;

function cardStatus(row, day, today) {
  if (day > today) return 'future';
  if (row?.imported && row.reviewed) return row.matched ? 'matched' : 'verified';
  if (day === today) return row?.imported ? 'review_pending' : 'import_pending';
  return row?.imported ? 'past_review_pending' : 'past_import_pending';
}

function mountCardPaymentsBeta(app, { pool, env, verifyToken, now = () => new Date() }) {
  if (env.CARD_PAYMENTS_BETA_ENABLED !== '1') return;
  const ids = [env.CARD_PAYMENTS_BETA_PETER_ID, env.CARD_PAYMENTS_BETA_MIRKO_ID, env.CARD_PAYMENTS_BETA_ALEX_ID];
  if (ids.some(id => !/^[1-9]\d*$/.test(id || '') || !Number.isSafeInteger(Number(id))) || new Set(ids).size !== 3) {
    console.error('Kartenzahlungsbeta benötigt drei unterschiedliche, gültige Benutzer-IDs.');
    return;
  }
  app.get('/api/card-payments-beta', verifyToken(), async (req, res) => {
    res.set('Cache-Control', 'private, no-store');
    if (!ids.includes(String(req.user?.id))) return res.sendStatus(404);
    const today = berlinDate.format(now());
    const week = sellingWeek(today, req.query.week);
    if (!week) return res.status(400).json({ message: 'Bitte den Montag der Kalenderwoche als ISO-Datum angeben.' });
    try {
      const { rows } = await pool.query(statusQuery, [week.days, branches, ids[1]]);
      const values = new Map(rows.map(row => [`${row.branch}|${row.code}|${row.day}`, row]));
      return res.json({ ...week, branches: branches.map(branch => ({
        branch, rows: types.map(code => ({
          code, statuses: week.days.map(day => cardStatus(values.get(`${branch}|${code}|${day}`), day, today)),
        })),
      })) });
    } catch (error) {
      console.error('Kartenzahlungsbeta-Abfrage fehlgeschlagen:', error.code || error.name);
      return res.status(503).json({ message: 'Kartenstatus vorübergehend nicht verfügbar.' });
    }
  });
}

module.exports = { cardStatus, mountCardPaymentsBeta };
