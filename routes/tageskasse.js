const express = require('express');
const bcrypt = require('bcrypt');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const { resolveBranch, validatePayload } = require('../lib/tageskassePolicy');
const { resolveAllowedBusinessDate } = require('../lib/businessDatePolicy');
const { findActivePinMatches } = require('../lib/employeePinIdentity');
const { DEFAULT_EMPLOYEE_PIN, fourDigitPin } = require('../lib/employeePinPolicy');
const { validateDepositReceipt } = require('../lib/depositReceiptPolicy');
const { calculateSafeTotal } = require('../lib/safeCountPolicy');
const { verifyEmployeePinProof } = require('../lib/employeePinProof');

const router = express.Router();
const ROLES = new Set(['Filiale', 'Admin', 'Supervisor', 'Manager-1', 'Geschäftsführer']);
const CASH = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000];
const NOTES = [500, 1000, 2000, 5000, 10000, 20000];
const receiptPinAttempts = new Map();

router.use((_req, res, next) => { res.setHeader('Cache-Control', 'no-store'); next(); });

const problem = (message, statusCode = 400) => Object.assign(new Error(message), { statusCode });
function allowed(req, res) {
  if (ROLES.has(req.user?.role)) return true;
  res.status(403).json({ message: 'Zugriff verweigert.' });
  return false;
}
function sendError(res, error, fallback = 'Tageskasse konnte nicht verarbeitet werden.') {
  if (error?.code === '23505') return res.status(409).json({ message: 'Dieser Vorgang wurde bereits verbindlich gespeichert.' });
  const status = Number(error?.statusCode) || 500;
  if (status >= 500) console.error('[TAGESKASSE_ERROR]', error?.code || error?.message);
  return res.status(status).json({ message: status >= 500 ? fallback : error.message });
}
function int(value, label, min = 0) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < min) throw problem(`${label} ist ungültig.`);
  return parsed;
}
function counts(value, denominations = CASH) {
  const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  return Object.fromEntries(denominations.map((cents) => [String(cents), int(source[cents] ?? 0, 'Eine Stückzahl')]));
}
const sumCounts = (value, denominations = CASH) => denominations.reduce((sum, cents) => sum + cents * value[String(cents)], 0);

async function contextFor(req, queryable, input = {}) {
  if (!ROLES.has(req.user?.role)) throw problem('Zugriff verweigert.', 403);
  const branchName = resolveBranch(req.user, input.branch || input.filiale);
  if (!branchName) throw problem('Filiale ist erforderlich.');
  const businessDate = resolveAllowedBusinessDate({ requestedDate: input.businessDate || input.datum, role: req.user?.role });
  const result = await queryable.query('SELECT id,name FROM public.filialen WHERE aktiv=true AND LOWER(name)=LOWER($1) LIMIT 1', [branchName]);
  if (!result.rows[0]) throw problem('Filiale wurde nicht gefunden.', 404);
  return { branchId: Number(result.rows[0].id), branch: result.rows[0].name, businessDate };
}
async function ensureDay(queryable, ctx, userId) {
  const result = await queryable.query(`INSERT INTO controlling.cash_business_days
    (filiale_id,business_date,created_by_user_id) VALUES ($1,$2,$3)
    ON CONFLICT (filiale_id,business_date) DO UPDATE SET updated_at=controlling.cash_business_days.updated_at
    RETURNING *`, [ctx.branchId, ctx.businessDate, userId]);
  if (result.rows[0].status === 'final') throw problem('Dieser Geschäftstag ist bereits verbindlich abgeschlossen.', 409);
  return result.rows[0];
}
async function proofEmployee(queryable, token) {
  const proof = verifyEmployeePinProof(token);
  const result = await queryable.query('SELECT id,display_name FROM core.filiale_pins WHERE id=$1 AND is_active=true LIMIT 1', [proof.id]);
  if (!result.rows[0]) throw problem('Die bestätigte Person ist nicht mehr aktiv.', 409);
  return { id: Number(result.rows[0].id), name: result.rows[0].display_name };
}
const openingJson = (row) => row ? { id: row.id, drawerNumber: Number(row.drawer_number), openingBalanceCents: Number(row.counted_total_cents), status: row.status } : null;
const closingJson = (row, branch) => ({ id: row.id, branch, businessDate: String(row.business_date).slice(0, 10), drawerNumber: Number(row.drawer_number), bankNoteCounts: row.banknote_counts || {}, expectedDepositCents: Number(row.bank_amount_cents), differenceCents: Number(row.eurocontrol_difference_cents), status: row.status });

// Rückwärtskompatibilität für das bisherige Tageskasse-Frontend während des
// gestuften Rollouts. Neue Tageskassenabrechnungen verwenden ausschließlich
// die V2-Endpunkte und die normalisierten Tabellen weiter unten.
function legacyPayload(payload, branch, userId, validation, status) {
  const amount = (value) => Number(String(value ?? 0).replace(',', '.')) || 0;
  return [branch, payload.businessDate, status, JSON.stringify(payload.counts || {}), validation.countedCash,
    amount(payload.expectedCash), amount(payload.floatAmount), Math.max(0, validation.countedCash - amount(payload.floatAmount)),
    amount(payload.ecRegister), amount(payload.ecTerminal), amount(payload.cardRegister), amount(payload.cardTerminal),
    validation.totalDifference, String(payload.differenceReason || '').trim() || null, userId];
}
router.get('/', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  try {
    const branch = resolveBranch(req.user, req.query.filiale);
    const businessDate = String(req.query.datum || '');
    if (!branch || !/^\d{4}-\d{2}-\d{2}$/.test(businessDate)) throw problem('Datum und Filiale sind erforderlich.');
    const result = await pool.query('SELECT * FROM controlling.cash_closings WHERE filiale=$1 AND business_date=$2 LIMIT 1', [branch, businessDate]);
    return res.json({ ok: true, closing: result.rows[0] || null });
  } catch (error) { return sendError(res, error, 'Tageskasse konnte nicht geladen werden.'); }
});
async function legacyUpsert(req, res, submitting) {
  if (!allowed(req, res)) return;
  const branch = resolveBranch(req.user, req.body?.branch);
  const payload = { ...(req.body || {}), branch };
  const validation = validatePayload(payload, { submitting });
  if (validation.errors.length) return res.status(400).json({ message: validation.errors.join(' ') });
  const status = submitting ? 'submitted' : 'draft';
  try {
    const result = await pool.query(`INSERT INTO controlling.cash_closings
      (filiale,business_date,status,denomination_counts,counted_cash,expected_cash,float_amount,deposit_amount,
       ec_register,ec_terminal,card_register,card_terminal,total_difference,difference_reason,created_by,submitted_at)
      VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,CASE WHEN $3='submitted' THEN NOW() ELSE NULL END)
      ON CONFLICT (filiale,business_date) DO UPDATE SET status=EXCLUDED.status,denomination_counts=EXCLUDED.denomination_counts,
       counted_cash=EXCLUDED.counted_cash,expected_cash=EXCLUDED.expected_cash,float_amount=EXCLUDED.float_amount,
       deposit_amount=EXCLUDED.deposit_amount,ec_register=EXCLUDED.ec_register,ec_terminal=EXCLUDED.ec_terminal,
       card_register=EXCLUDED.card_register,card_terminal=EXCLUDED.card_terminal,total_difference=EXCLUDED.total_difference,
       difference_reason=EXCLUDED.difference_reason,updated_at=NOW(),submitted_at=CASE WHEN EXCLUDED.status='submitted' THEN NOW() ELSE NULL END
      WHERE controlling.cash_closings.status<>'submitted' RETURNING *`, legacyPayload(payload, branch, req.user.id, validation, status));
    if (!result.rowCount) throw problem('Diese Tageskasse wurde bereits verbindlich abgeschlossen.', 409);
    return res.status(submitting ? 201 : 200).json({ ok: true, closing: result.rows[0] });
  } catch (error) { return sendError(res, error, 'Tageskasse konnte nicht gespeichert werden.'); }
}
router.put('/', verifyToken(), (req, res) => legacyUpsert(req, res, false));
router.post('/submit', verifyToken(), (req, res) => legacyUpsert(req, res, true));

router.get('/openings/previous', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  try {
    const ctx = await contextFor(req, pool, req.query);
    const drawer = int(req.query.drawerNumber, 'Lade', 1);
    if (![1, 2].includes(drawer)) throw problem('Lade ist ungültig.');
    const result = await pool.query(`SELECT c.*,d.business_date FROM controlling.cash_drawer_closings c
      JOIN controlling.cash_business_days d ON d.id=c.business_day_id
      WHERE d.filiale_id=$1 AND d.business_date<$2 AND c.drawer_number=$3 AND c.status='confirmed'
      ORDER BY d.business_date DESC,c.confirmed_at DESC LIMIT 1`, [ctx.branchId, ctx.businessDate, drawer]);
    return res.json({ closing: result.rows[0] ? { ...closingJson(result.rows[0], ctx.branch), status: 'closed', resolved: false } : null });
  } catch (error) { return sendError(res, error); }
});

router.post('/openings', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const ctx = await contextFor(req, client, req.body || {});
    const day = await ensureDay(client, ctx, req.user.id);
    const drawer = int(req.body?.drawerNumber, 'Lade', 1);
    if (![1, 2].includes(drawer)) throw problem('Lade ist ungültig.');
    const employee = await proofEmployee(client, req.body?.employeeProofToken);
    const sourceAttempts = Array.isArray(req.body?.attempts) ? req.body.attempts : [];
    if (!sourceAttempts.length) throw problem('Mindestens eine vollständige Zählung ist erforderlich.');
    const attempts = [];
    for (const attempt of sourceAttempts) {
      const counter = await proofEmployee(client, attempt.proofToken || req.body?.employeeProofToken);
      const denominationCounts = counts(attempt.denominationCounts);
      attempts.push({ counter, denominationCounts, total: sumCounts(denominationCounts) });
    }
    const finalAttempt = attempts.at(-1);
    const carryover = String(req.body?.carryoverDecision || 'none').replaceAll('-', '_');
    if (!['none', 'keep_in_drawer', 'adjust_to_target'].includes(carryover)) throw problem('Die Entscheidung zum Anfangsbestand ist ungültig.');
    const inserted = await client.query(`INSERT INTO controlling.cash_drawer_openings
      (business_day_id,drawer_number,employee_pin_id,second_counter_pin_id,counted_total_cents,target_float_cents,
       difference_to_target_cents,previous_closing_id,carryover_decision,attempt_count,status,confirmed_at)
      VALUES ($1,$2,$3,$4,$5,25000,$6,$7,$8,$9,'confirmed',NOW()) RETURNING *`, [day.id, drawer,
      employee.id, finalAttempt.counter.id === employee.id ? null : finalAttempt.counter.id, finalAttempt.total,
      finalAttempt.total - 25000, req.body?.previousClosingId || null, carryover, attempts.length]);
    for (let index = 0; index < attempts.length; index += 1) {
      const attempt = attempts[index];
      await client.query(`INSERT INTO controlling.cash_drawer_opening_attempts
        (opening_id,attempt_number,counted_by_pin_id,denomination_counts,counted_total_cents,target_float_cents,difference_to_target_cents)
        VALUES ($1,$2,$3,$4::jsonb,$5,25000,$6)`, [inserted.rows[0].id, index + 1, attempt.counter.id,
        JSON.stringify(attempt.denominationCounts), attempt.total, attempt.total - 25000]);
    }
    await client.query(`INSERT INTO controlling.cash_business_day_events
      (business_day_id,event_type,actor_user_id,actor_employee_pin_id,details)
      VALUES ($1,'drawer_opening_confirmed',$2,$3,$4::jsonb)`, [day.id, req.user.id, employee.id, JSON.stringify({ drawerNumber: drawer, attemptCount: attempts.length })]);
    await client.query('COMMIT');
    return res.status(201).json({ opening: openingJson(inserted.rows[0]) });
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); return sendError(res, error, 'Kasseneinzahlung konnte nicht gespeichert werden.'); }
  finally { client.release(); }
});

router.get('/drawer-closings/context', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  try {
    const ctx = await contextFor(req, pool, req.query);
    const drawer = int(req.query.drawerNumber, 'Lade', 1);
    const result = await pool.query(`SELECT o.* FROM controlling.cash_drawer_openings o
      JOIN controlling.cash_business_days d ON d.id=o.business_day_id
      WHERE d.filiale_id=$1 AND d.business_date=$2 AND o.drawer_number=$3 AND o.status='confirmed' LIMIT 1`, [ctx.branchId, ctx.businessDate, drawer]);
    return res.json({ opening: openingJson(result.rows[0]) });
  } catch (error) { return sendError(res, error); }
});

router.post('/drawer-closings', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const ctx = await contextFor(req, client, req.body || {});
    const day = await ensureDay(client, ctx, req.user.id);
    const executing = await proofEmployee(client, req.body?.proofToken || req.body?.workflowToken);
    const responsibleId = int(req.body?.responsibleEmployeePinId, 'Verantwortliche Person', 1);
    const responsible = await client.query('SELECT id FROM core.filiale_pins WHERE id=$1 AND is_active=true', [responsibleId]);
    if (!responsible.rows[0]) throw problem('Die verantwortliche Person wurde nicht gefunden.', 409);
    const drawer = int(req.body?.drawerNumber, 'Lade', 1);
    if (![1, 2].includes(drawer)) throw problem('Lade ist ungültig.');
    const fullCounts = counts(req.body?.fullCounts);
    const bankCounts = counts(req.body?.bankNoteCounts, NOTES);
    const counted = sumCounts(fullCounts);
    const coinTotal = CASH.filter((cents) => cents <= 200).reduce((sum, cents) => sum + cents * fullCounts[String(cents)], 0);
    const retained = Math.min(Math.floor(coinTotal / 500) * 500, 25000);
    const loose = coinTotal - retained;
    const bankAmount = sumCounts(bankCounts, NOTES);
    if (counted - loose - bankAmount !== 25000) throw problem('Die Kontrollrechnung der Kassenabrechnung stimmt nicht.');
    const difference = Number(req.body?.differenceCents);
    if (!Number.isInteger(difference)) throw problem('EuroControl-Differenz ist ungültig.');
    const openingId = req.body?.openingCountId || null;
    if (!openingId && req.body?.proceededWithoutOpening !== true) throw problem('Die zugehörige Einzählung fehlt.');
    const attempts = [...(Array.isArray(req.body?.recountAttempts) ? req.body.recountAttempts : []), { fullCounts, bankNoteCounts: bankCounts, reportedDifferenceCents: difference }];
    const inserted = await client.query(`INSERT INTO controlling.cash_drawer_closings
      (business_day_id,opening_id,drawer_number,executing_employee_pin_id,responsible_employee_pin_id,is_last_cash,
       proceeded_without_opening,denomination_counts,counted_total_cents,coin_total_cents,retained_coin_cents,
       loose_change_cents,banknotes_needed_cents,banknote_counts,bank_amount_cents,eurocontrol_difference_cents,
       difference_reviewed,notification_confirmed,attempt_count,status,confirmed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13,$14::jsonb,$15,$16,true,$17,$18,'confirmed',NOW()) RETURNING *`,
    [day.id, openingId, drawer, executing.id, responsibleId, req.body?.isLastCash === true, !openingId, JSON.stringify(fullCounts), counted,
      coinTotal, retained, loose, 25000 - retained, JSON.stringify(bankCounts), bankAmount, difference,
      req.body?.notificationConfirmed === true, attempts.length]);
    for (let index = 0; index < attempts.length; index += 1) {
      const attemptCounts = counts(attempts[index].fullCounts || fullCounts);
      const attemptNotes = counts(attempts[index].bankNoteCounts || bankCounts, NOTES);
      const attemptTotal = sumCounts(attemptCounts);
      const attemptCoins = CASH.filter((cents) => cents <= 200).reduce((sum, cents) => sum + cents * attemptCounts[String(cents)], 0);
      const attemptRetained = Math.min(Math.floor(attemptCoins / 500) * 500, 25000);
      await client.query(`INSERT INTO controlling.cash_drawer_closing_attempts
        (closing_id,attempt_number,counted_by_pin_id,denomination_counts,counted_total_cents,coin_total_cents,
         retained_coin_cents,loose_change_cents,banknotes_needed_cents,banknote_counts,bank_amount_cents,eurocontrol_difference_cents)
        VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10::jsonb,$11,$12)`, [inserted.rows[0].id, index + 1, executing.id,
        JSON.stringify(attemptCounts), attemptTotal, attemptCoins, attemptRetained, attemptCoins - attemptRetained, 25000 - attemptRetained,
        JSON.stringify(attemptNotes), sumCounts(attemptNotes, NOTES), Number(attempts[index].reportedDifferenceCents ?? difference)]);
    }
    await client.query(`INSERT INTO controlling.cash_business_day_events
      (business_day_id,event_type,actor_user_id,actor_employee_pin_id,details)
      VALUES ($1,'drawer_closing_confirmed',$2,$3,$4::jsonb)`, [day.id, req.user.id, executing.id, JSON.stringify({ drawerNumber: drawer, attemptCount: attempts.length })]);
    await client.query('COMMIT');
    return res.status(201).json({ closing: closingJson({ ...inserted.rows[0], business_date: ctx.businessDate }, ctx.branch) });
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); return sendError(res, error, 'Kassenabrechnung konnte nicht gespeichert werden.'); }
  finally { client.release(); }
});

async function loadDay(req, queryable, input) {
  const ctx = await contextFor(req, queryable, input);
  const [dayResult, settingsResult] = await Promise.all([
    queryable.query('SELECT * FROM controlling.cash_business_days WHERE filiale_id=$1 AND business_date=$2 LIMIT 1', [ctx.branchId, ctx.businessDate]),
    queryable.query('SELECT * FROM core.cash_safe_settings WHERE filiale=$1 LIMIT 1', [ctx.branch]),
  ]);
  const day = dayResult.rows[0];
  if (!day) return { ctx, day: null, settings: settingsResult.rows[0], closings: [], openings: [], metrics: null, safeCount: null };
  const [closings, openings, metrics, safeCount] = await Promise.all([
    queryable.query("SELECT * FROM controlling.cash_drawer_closings WHERE business_day_id=$1 AND status='confirmed' ORDER BY drawer_number", [day.id]),
    queryable.query(`SELECT o.* FROM controlling.cash_drawer_openings o WHERE o.business_day_id=$1 AND o.status='confirmed'
      AND NOT EXISTS (SELECT 1 FROM controlling.cash_drawer_closings c WHERE c.opening_id=o.id AND c.status='confirmed') ORDER BY o.drawer_number`, [day.id]),
    queryable.query('SELECT * FROM controlling.branch_daily_metrics WHERE business_day_id=$1 LIMIT 1', [day.id]),
    queryable.query(`SELECT * FROM core.cash_safe_counts WHERE filiale=$1 AND business_date<=$2
      ORDER BY CASE count_type WHEN 'evening' THEN 1 WHEN 'handover' THEN 2 ELSE 3 END,submitted_at DESC LIMIT 1`, [ctx.branch, ctx.businessDate]),
  ]);
  return { ctx, day, settings: settingsResult.rows[0], closings: closings.rows, openings: openings.rows, metrics: metrics.rows[0], safeCount: safeCount.rows[0] };
}
const safeJson = (row) => row ? { id: row.id, businessDate: String(row.business_date).slice(0, 10), countedTotalCents: Number(row.counted_total_cents), countType: row.count_type, completedAt: row.submitted_at, fixedDrawerCents: Number(row.fixed_drawer_cents), littleBerthaCents: Number(row.little_bertha_cents) } : null;

router.get('/day-close/context', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  try {
    const loaded = await loadDay(req, pool, req.query);
    const bankTotal = loaded.closings.reduce((sum, row) => sum + Number(row.bank_amount_cents), 0);
    const looseTotal = loaded.closings.reduce((sum, row) => sum + Number(row.loose_change_cents), 0);
    return res.json({ businessDayId: loaded.day?.id || null, status: loaded.day?.status || 'open',
      activeOpenings: loaded.openings.map(openingJson), closings: loaded.closings.map((row) => closingJson({ ...row, business_date: loaded.ctx.businessDate }, loaded.ctx.branch)),
      baseSafeCount: safeJson(loaded.safeCount),
      finalSafeCount: loaded.safeCount?.count_type === 'evening' && String(loaded.safeCount.business_date).slice(0, 10) === loaded.ctx.businessDate ? safeJson(loaded.safeCount) : null,
      safeSettings: loaded.settings ? { fixedDrawerCents: Number(loaded.settings.fixed_drawer_cents), littleBerthaCents: Number(loaded.settings.little_bertha_cents) } : null,
      dailyMetrics: loaded.metrics ? { dailyRevenueCents: Number(loaded.metrics.daily_revenue_cents), customerCount: Number(loaded.metrics.customer_count), averageReceiptCents: Number(loaded.metrics.reported_average_receipt_cents), eurocontrolBankCents: Number(loaded.metrics.eurocontrol_bank_cents), status: loaded.metrics.status } : null,
      totals: { cashRevenueCents: bankTotal, looseChangeTotalCents: looseTotal, standardBalanceCents: loaded.settings ? Number(loaded.settings.fixed_drawer_cents) + Number(loaded.settings.little_bertha_cents) : null },
      processStates: { ...(loaded.metrics ? { 'daily-report': { status: 'completed' } } : {}), ...(loaded.safeCount?.count_type === 'evening' && String(loaded.safeCount.business_date).slice(0, 10) === loaded.ctx.businessDate ? { 'final-safe-count': { status: 'completed', completedAt: loaded.safeCount.submitted_at } } : {}), ...(loaded.day?.status === 'final' ? { finalize: { status: 'completed', completedAt: loaded.day.finalized_at } } : {}) },
    });
  } catch (error) { return sendError(res, error); }
});

router.put('/day-close/metrics', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const ctx = await contextFor(req, client, req.body || {});
    const day = await ensureDay(client, ctx, req.user.id);
    const values = [int(req.body?.dailyRevenueCents, 'Tagesumsatz'), int(req.body?.customerCount, 'Kundenanzahl'), int(req.body?.averageReceiptCents, 'Durchschnittsbon'), int(req.body?.eurocontrolBankCents, 'Bar an Bank')];
    const result = await client.query(`INSERT INTO controlling.branch_daily_metrics
      (business_day_id,daily_revenue_cents,customer_count,reported_average_receipt_cents,eurocontrol_bank_cents,captured_by_user_id,updated_by_user_id)
      VALUES ($1,$2,$3,$4,$5,$6,$6) ON CONFLICT (business_day_id) DO UPDATE SET
      daily_revenue_cents=EXCLUDED.daily_revenue_cents,customer_count=EXCLUDED.customer_count,
      reported_average_receipt_cents=EXCLUDED.reported_average_receipt_cents,eurocontrol_bank_cents=EXCLUDED.eurocontrol_bank_cents,
      updated_by_user_id=EXCLUDED.updated_by_user_id,updated_at=NOW() WHERE controlling.branch_daily_metrics.status='draft' RETURNING *`, [day.id, ...values, req.user.id]);
    if (!result.rows[0]) throw problem('Die Tageswerte wurden bereits verbindlich abgeschlossen.', 409);
    await client.query('COMMIT');
    return res.json({ metrics: result.rows[0] });
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); return sendError(res, error, 'Tageswerte konnten nicht gespeichert werden.'); }
  finally { client.release(); }
});

router.post('/day-close/final-safe-count', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const ctx = await contextFor(req, client, req.body || {});
    await ensureDay(client, ctx, req.user.id);
    const first = await proofEmployee(client, req.body?.counterProofToken);
    const second = await proofEmployee(client, req.body?.recorderProofToken);
    if (first.id === second.id) throw problem('Für das Vier-Augen-Prinzip sind zwei Personen erforderlich.');
    const settings = await client.query('SELECT * FROM core.cash_safe_settings WHERE filiale=$1 LIMIT 1', [ctx.branch]);
    if (!settings.rows[0]) throw problem('Der Tresor-Grundbestand ist nicht eingerichtet.', 409);
    const total = calculateSafeTotal({ fixedDrawerCents: Number(settings.rows[0].fixed_drawer_cents), littleBerthaCents: Number(settings.rows[0].little_bertha_cents), looseMoneyCents: req.body?.looseMoneyCents, coinRollCounts: req.body?.coinRollCounts, banknoteCounts: req.body?.banknoteCounts });
    const eurocontrol = int(req.body?.eurocontrolTotalCents, 'EuroControl-Tresorbestand');
    const result = await client.query(`INSERT INTO core.cash_safe_counts
      (filiale,business_date,count_type,count_mode,fixed_drawer_cents,little_bertha_cents,loose_money_cents,coin_roll_counts,
       coin_roll_total_cents,banknote_counts,banknote_total_cents,counted_total_cents,eurocontrol_total_cents,difference_cents,
       first_employee_id,first_employee_name,first_employee_role,second_employee_id,second_employee_name,second_employee_role)
      VALUES ($1,$2,'evening','full_count',$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9,$10,$11,$12,$13,$14,'Spätschicht 1',$15,$16,'Spätschicht 2') RETURNING *`,
    [ctx.branch, ctx.businessDate, total.fixedDrawerCents, total.littleBerthaCents, total.looseMoneyCents, JSON.stringify(total.coinRollCounts), total.rollTotalCents,
      JSON.stringify(total.banknoteCounts), total.banknoteTotalCents, total.countedTotalCents, eurocontrol, total.countedTotalCents - eurocontrol, first.id, first.name, second.id, second.name]);
    await client.query('COMMIT');
    return res.status(201).json({ count: safeJson(result.rows[0]) });
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); return sendError(res, error, 'Abendzählung konnte nicht gespeichert werden.'); }
  finally { client.release(); }
});

router.post('/day-close/finalize', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const loaded = await loadDay(req, client, req.body || {});
    if (!loaded.day) throw problem('Der Geschäftstag wurde nicht gefunden.', 404);
    const lock = await client.query('SELECT * FROM controlling.cash_business_days WHERE id=$1 FOR UPDATE', [loaded.day.id]);
    if (lock.rows[0].status === 'final') throw problem('Dieser Geschäftstag ist bereits verbindlich abgeschlossen.', 409);
    if (!loaded.metrics) throw problem('Die Tageswerte fehlen.', 409);
    if (!loaded.closings.length) throw problem('Es wurde noch keine Kasse abgerechnet.', 409);
    if (loaded.openings.length) throw problem('Es sind noch aktive Kassenschichten vorhanden.', 409);
    const safeCount = await client.query('SELECT * FROM core.cash_safe_counts WHERE id=$1 AND filiale=$2 AND business_date=$3 LIMIT 1', [req.body?.safeCountId, loaded.ctx.branch, loaded.ctx.businessDate]);
    if (!safeCount.rows[0]) throw problem('Die abschließende Tresorzählung wurde nicht gefunden.', 409);
    const tokens = Array.isArray(req.body?.approvalProofTokens) ? req.body.approvalProofTokens : [];
    if (tokens.length !== 2) throw problem('Zwei persönliche Freigaben sind erforderlich.');
    const first = await proofEmployee(client, tokens[0]);
    const second = await proofEmployee(client, tokens[1]);
    if (first.id === second.id) throw problem('Für den Abschluss sind zwei unterschiedliche Personen erforderlich.');
    const calculatedAverage = loaded.metrics.customer_count > 0 ? Math.round(Number(loaded.metrics.daily_revenue_cents) / Number(loaded.metrics.customer_count)) : 0;
    await client.query(`UPDATE controlling.branch_daily_metrics SET status='final',calculated_average_receipt_cents=$2,
      average_receipt_difference_cents=reported_average_receipt_cents-$2,finalized_by_user_id=$3,updated_by_user_id=$3,finalized_at=NOW(),updated_at=NOW()
      WHERE business_day_id=$1 AND status='draft'`, [loaded.day.id, calculatedAverage, req.user.id]);
    await client.query(`INSERT INTO controlling.cash_business_day_approvals
      (business_day_id,approval_sequence,employee_pin_id,employee_name_snapshot,employee_role_snapshot)
      VALUES ($1,1,$2,$3,'Abschlusskontrolle 1'),($1,2,$4,$5,'Abschlusskontrolle 2')`, [loaded.day.id, first.id, first.name, second.id, second.name]);
    const totalBank = loaded.closings.reduce((sum, row) => sum + Number(row.bank_amount_cents), 0);
    const finalized = await client.query(`UPDATE controlling.cash_business_days SET status='final',workflow_version=2,total_bank_payout_cents=$2,
      loose_change_payout_confirmed=$3,additional_safe_skim_confirmed=$4,bank_payout_confirmed=$5,safe_bag_confirmed=$6,
      final_safe_count_id=$7,finalized_by_user_id=$8,finalized_at=NOW(),updated_at=NOW() WHERE id=$1 RETURNING *`, [loaded.day.id, totalBank,
      req.body?.looseChangePayoutConfirmed === true, req.body?.additionalSafeSkimConfirmed === true, req.body?.bankPayoutConfirmed === true,
      req.body?.safeBagConfirmed === true, safeCount.rows[0].id, req.user.id]);
    await client.query(`INSERT INTO controlling.cash_business_day_events (business_day_id,event_type,actor_user_id,details)
      VALUES ($1,'business_day_finalized',$2,$3::jsonb)`, [loaded.day.id, req.user.id, JSON.stringify({ workflowVersion: 2, totalBankPayoutCents: totalBank })]);
    await client.query('COMMIT');
    return res.json({ businessDay: finalized.rows[0], approvals: [first, second] });
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); return sendError(res, error, 'Tagesabschluss konnte nicht gespeichert werden.'); }
  finally { client.release(); }
});

function receiptAttempt(req) {
  const key = `${req.user?.id || 'unknown'}:${req.ip || 'unknown'}`, now = Date.now(), current = receiptPinAttempts.get(key);
  const entry = !current || current.resetAt <= now ? { count: 0, resetAt: now + 15 * 60_000 } : current;
  entry.count += 1; receiptPinAttempts.set(key, entry); return { allowed: entry.count <= 10, key };
}
function closingIds(value) {
  if (!Array.isArray(value) || value.length < 1 || value.length > 20) return null;
  const ids = [...new Set(value.map((item) => String(item || '').toLowerCase()))];
  return ids.length === value.length && ids.every((id) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(id)) ? ids : null;
}
async function receiptEmployee(client, pinValue) {
  const pin = fourDigitPin(pinValue);
  if (pin === DEFAULT_EMPLOYEE_PIN) throw problem('Bitte zuerst einen persönlichen PIN festlegen.', 409);
  const candidates = await client.query('SELECT id,display_name,pin_hash,must_change_pin,locked_until FROM core.filiale_pins WHERE is_active=true ORDER BY id');
  const matches = await findActivePinMatches(candidates.rows, pin, bcrypt.compare);
  if (matches.length > 1) throw problem('Dieser PIN ist nicht eindeutig. Bitte die PIN-Verwaltung öffnen.', 409);
  if (!matches[0]) throw problem('PIN ungültig.', 401);
  return { id: Number(matches[0].id), name: matches[0].display_name };
}

router.get('/deposit-receipts/open', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  try {
    const branch = resolveBranch(req.user, req.query.filiale);
    if (!branch) throw problem('Filiale ist erforderlich.');
    const result = await pool.query(`SELECT c.id,d.business_date,c.bank_amount_cents AS expected_deposit_cents
      FROM controlling.cash_drawer_closings c JOIN controlling.cash_business_days d ON d.id=c.business_day_id
      JOIN public.filialen f ON f.id=d.filiale_id WHERE LOWER(f.name)=LOWER($1) AND c.status='confirmed'
      AND NOT EXISTS (SELECT 1 FROM controlling.cash_deposit_receipt_closings link WHERE link.closing_id=c.id)
      ORDER BY d.business_date,c.drawer_number`, [branch]);
    return res.json({ ok: true, branch, closings: result.rows.map((row) => ({ id: row.id, businessDate: String(row.business_date).slice(0, 10), expectedDepositCents: Number(row.expected_deposit_cents) })) });
  } catch (error) { return sendError(res, error, 'Offene Einzahlungsbelege konnten nicht geladen werden.'); }
});

router.post('/deposit-receipts/confirm', verifyToken(), async (req, res) => {
  if (!allowed(req, res)) return;
  const branch = resolveBranch(req.user, req.body?.branch), ids = closingIds(req.body?.closingIds), validation = validateDepositReceipt(req.body || {});
  if (!branch) validation.errors.unshift('Filiale ist erforderlich.');
  if (!ids) validation.errors.unshift('Mindestens ein gültiger Tagesabschluss muss ausgewählt werden.');
  if (validation.errors.length) return res.status(400).json({ message: validation.errors.join(' ') });
  const attempt = receiptAttempt(req);
  if (!attempt.allowed) return res.status(429).json({ message: 'Zu viele PIN-Versuche. Bitte 15 Minuten warten.' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const employee = await receiptEmployee(client, req.body?.pin);
    const closings = await client.query(`SELECT c.id,d.business_date,c.bank_amount_cents AS expected_deposit_cents
      FROM controlling.cash_drawer_closings c JOIN controlling.cash_business_days d ON d.id=c.business_day_id
      JOIN public.filialen f ON f.id=d.filiale_id WHERE c.id=ANY($1::uuid[]) AND LOWER(f.name)=LOWER($2) AND c.status='confirmed'
      ORDER BY d.business_date,c.id FOR UPDATE OF c`, [ids, branch]);
    if (closings.rows.length !== ids.length) throw problem('Mindestens ein ausgewählter Tagesabschluss ist nicht verfügbar.', 409);
    const linked = await client.query('SELECT closing_id FROM controlling.cash_deposit_receipt_closings WHERE closing_id=ANY($1::uuid[])', [ids]);
    if (linked.rowCount) throw problem('Mindestens ein Tagesabschluss besitzt bereits einen Einzahlungsbeleg.', 409);
    const total = closings.rows.reduce((sum, row) => sum + Number(row.expected_deposit_cents), 0);
    if (total !== validation.normalized.receiptTotalCents) throw problem('Belegsumme und ausgewählte Tagesabschlüsse stimmen nicht überein.', 409);
    const receipt = await client.query(`INSERT INTO controlling.cash_deposit_receipts
      (filiale,deposit_date,deposit_time,transaction_number,terminal_number,purpose,banknote_counts,receipt_total_cents,
       ocr_confidence,archive_mode,confirmed_by_employee_id,confirmed_by_employee_name,image_retained)
      VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,'manual_onedrive',$10,$11,false) RETURNING *`, [branch,
      validation.normalized.depositDate, validation.normalized.depositTime, validation.normalized.transactionNumber, validation.normalized.terminalNumber,
      validation.normalized.purpose, JSON.stringify(validation.normalized.banknoteCounts), validation.normalized.receiptTotalCents,
      validation.normalized.ocrConfidence, employee.id, employee.name]);
    for (const id of ids) await client.query('INSERT INTO controlling.cash_deposit_receipt_closings (receipt_id,closing_id) VALUES ($1,$2)', [receipt.rows[0].id, id]);
    await client.query('UPDATE core.filiale_pins SET last_used_at=NOW(),updated_at=NOW() WHERE id=$1', [employee.id]);
    await client.query('COMMIT'); receiptPinAttempts.delete(attempt.key);
    return res.status(201).json({ ok: true, receipt: receipt.rows[0], closings: closings.rows });
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); return sendError(res, error, 'Einzahlungsbeleg konnte nicht gespeichert werden.'); }
  finally { client.release(); }
});

module.exports = router;
