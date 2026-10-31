const express = require('express');
const bcrypt = require('bcrypt');

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const { findActivePinMatches } = require('../lib/employeePinIdentity');
const { participantLabels, validateSafeCountPayload } = require('../lib/safeCountPolicy');
const { resolveBranch } = require('../lib/tageskassePolicy');
const { resolveAllowedBusinessDate } = require('../lib/businessDatePolicy');

const router = express.Router();
const ALLOWED_ROLES = new Set(['Filiale', 'Admin', 'Supervisor', 'Manager-1', 'Geschäftsführer']);
const pinAttempts = new Map();

router.use(verifyToken());
router.use((_req, res, next) => { res.setHeader('Cache-Control', 'no-store'); next(); });

function consumePinAttempt(req) {
  const key = `${req.user?.id || 'unknown'}:${req.ip || 'unknown'}`;
  const now = Date.now();
  const current = pinAttempts.get(key);
  const entry = !current || current.resetAt <= now ? { count: 0, resetAt: now + 15 * 60_000 } : current;
  entry.count += 1;
  pinAttempts.set(key, entry);
  return { allowed: entry.count <= 10, key };
}

function publicCount(row) {
  if (!row) return null;
  return {
    id: row.id,
    branch: row.filiale,
    businessDate: String(row.business_date).slice(0, 10),
    countType: row.count_type,
    mode: row.count_mode,
    previousEveningCountId: row.previous_evening_count_id,
    fixedDrawerCents: Number(row.fixed_drawer_cents),
    littleBerthaCents: Number(row.little_bertha_cents),
    looseMoneyCents: Number(row.loose_money_cents),
    coinRollCounts: row.coin_roll_counts || {},
    rollTotalCents: Number(row.coin_roll_total_cents),
    banknoteCounts: row.banknote_counts || {},
    banknoteTotalCents: Number(row.banknote_total_cents),
    countedTotalCents: Number(row.counted_total_cents),
    eurocontrolTotalCents: row.eurocontrol_total_cents == null ? null : Number(row.eurocontrol_total_cents),
    differenceCents: row.difference_cents == null ? null : Number(row.difference_cents),
    participants: [
      { id: Number(row.first_employee_id), name: row.first_employee_name, role: row.first_employee_role },
      { id: Number(row.second_employee_id), name: row.second_employee_name, role: row.second_employee_role },
    ],
    submittedAt: row.submitted_at,
  };
}

function assertAllowed(req) {
  if (!ALLOWED_ROLES.has(req.user?.role)) throw Object.assign(new Error('Zugriff verweigert.'), { statusCode: 403 });
}

async function settingsFor(queryable, branch) {
  const result = await queryable.query(`SELECT filiale,fixed_drawer_cents,little_bertha_cents
    FROM core.cash_safe_settings WHERE filiale=$1 LIMIT 1`, [branch]);
  if (!result.rows[0]) throw Object.assign(new Error('Für diese Filiale ist noch kein Tresor-Grundbestand eingerichtet.'), { statusCode: 409 });
  return {
    branch: result.rows[0].filiale,
    fixedDrawerCents: Number(result.rows[0].fixed_drawer_cents),
    littleBerthaCents: Number(result.rows[0].little_bertha_cents),
  };
}

async function identify(queryable, pin) {
  const candidates = await queryable.query(`SELECT id,display_name,pin_hash,must_change_pin,locked_until
    FROM core.filiale_pins WHERE is_active=true ORDER BY id`);
  const matches = await findActivePinMatches(candidates.rows, String(pin), bcrypt.compare);
  if (matches.length !== 1) {
    throw Object.assign(new Error(matches.length ? 'Dieser PIN ist nicht eindeutig zugeordnet.' : 'Mindestens einer der beiden PINs ist ungültig.'), { statusCode: matches.length ? 409 : 401 });
  }
  return { id: Number(matches[0].id), name: matches[0].display_name };
}

function sendError(res, error) {
  if (error?.code === '23505') return res.status(409).json({ message: 'Diese Tresorzählung wurde bereits verbindlich abgeschlossen.' });
  const status = Number(error?.statusCode) || 500;
  if (status >= 500) console.error('[SAFE_COUNT_ERROR]', error?.code || error?.message);
  return res.status(status).json({ message: status >= 500 ? 'Tresorzählung konnte nicht verarbeitet werden.' : error.message });
}

router.get('/context', async (req, res) => {
  try {
    assertAllowed(req);
    const branch = resolveBranch(req.user, req.query.branch);
    if (!branch) throw Object.assign(new Error('Bitte zuerst eine Filiale auswählen.'), { statusCode: 400 });
    const businessDate = resolveAllowedBusinessDate({ requestedDate: req.query.businessDate, role: req.user?.role });
    const [settings, existing, previous] = await Promise.all([
      settingsFor(pool, branch),
      pool.query(`SELECT * FROM core.cash_safe_counts WHERE filiale=$1 AND business_date=$2 ORDER BY submitted_at`, [branch, businessDate]),
      pool.query(`SELECT * FROM core.cash_safe_counts
        WHERE filiale=$1 AND count_type='evening' AND business_date<$2
        ORDER BY business_date DESC,submitted_at DESC LIMIT 1`, [branch, businessDate]),
    ]);
    return res.json({ businessDate, settings, existing: existing.rows.map(publicCount), previousEvening: publicCount(previous.rows[0]) });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post('/', async (req, res) => {
  const attempt = consumePinAttempt(req);
  if (!attempt.allowed) return res.status(429).json({ message: 'Zu viele PIN-Versuche. Bitte 15 Minuten warten.' });
  const client = await pool.connect();
  try {
    assertAllowed(req);
    const branch = resolveBranch(req.user, req.body?.branch);
    if (!branch) throw Object.assign(new Error('Bitte zuerst eine Filiale auswählen.'), { statusCode: 400 });
    const businessDate = resolveAllowedBusinessDate({ requestedDate: req.body?.businessDate, role: req.user?.role });
    await client.query('BEGIN');
    const settings = await settingsFor(client, branch);
    const validated = validateSafeCountPayload(req.body, settings);
    const first = await identify(client, req.body.firstPin);
    const second = await identify(client, req.body.secondPin);
    if (first.id === second.id) throw Object.assign(new Error('Für das Vier-Augen-Prinzip müssen zwei unterschiedliche Personen bestätigen.'), { statusCode: 400 });
    const labels = participantLabels(validated.countType);
    let figures = validated.result;
    let previousEveningCountId = null;
    if (validated.mode === 'confirmation') {
      const previous = await client.query(`SELECT * FROM core.cash_safe_counts
        WHERE id=$1 AND filiale=$2 AND count_type='evening' AND business_date<$3 LIMIT 1`,
      [req.body.previousEveningCountId, branch, businessDate]);
      if (!previous.rows[0]) throw Object.assign(new Error('Die bestätigte Abendzählung des Vortags wurde nicht gefunden.'), { statusCode: 409 });
      const row = previous.rows[0];
      previousEveningCountId = row.id;
      figures = {
        fixedDrawerCents: Number(row.fixed_drawer_cents), littleBerthaCents: Number(row.little_bertha_cents),
        looseMoneyCents: Number(row.loose_money_cents), coinRollCounts: row.coin_roll_counts,
        banknoteCounts: row.banknote_counts, rollTotalCents: Number(row.coin_roll_total_cents),
        banknoteTotalCents: Number(row.banknote_total_cents), countedTotalCents: Number(row.counted_total_cents),
      };
    }
    const result = await client.query(`INSERT INTO core.cash_safe_counts
      (filiale,business_date,count_type,count_mode,previous_evening_count_id,
       fixed_drawer_cents,little_bertha_cents,loose_money_cents,coin_roll_counts,
       coin_roll_total_cents,banknote_counts,banknote_total_cents,counted_total_cents,
       eurocontrol_total_cents,difference_cents,first_employee_id,first_employee_name,
       first_employee_role,second_employee_id,second_employee_name,second_employee_role)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11::jsonb,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
      RETURNING *`, [
      branch, businessDate, validated.countType, validated.mode, previousEveningCountId,
      figures.fixedDrawerCents, figures.littleBerthaCents, figures.looseMoneyCents,
      JSON.stringify(figures.coinRollCounts), figures.rollTotalCents, JSON.stringify(figures.banknoteCounts),
      figures.banknoteTotalCents, figures.countedTotalCents, validated.eurocontrolTotalCents,
      validated.differenceCents, first.id, first.name, labels[0], second.id, second.name, labels[1],
    ]);
    await client.query('UPDATE core.filiale_pins SET last_used_at=NOW(),updated_at=NOW() WHERE id=ANY($1::bigint[])', [[first.id, second.id]]);
    await client.query('COMMIT');
    pinAttempts.delete(attempt.key);
    return res.status(201).json({ count: publicCount(result.rows[0]) });
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch { /* original error wins */ }
    return sendError(res, error);
  } finally {
    client.release();
  }
});

module.exports = router;
