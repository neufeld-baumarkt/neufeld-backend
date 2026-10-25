const express = require('express');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const { findActivePinMatches } = require('../lib/employeePinIdentity');
const { validateOpeningCount } = require('../lib/cashOpeningPolicy');
const { validateCashClosing } = require('../lib/cashClosingPolicy');
const { validateCashCorrection } = require('../lib/cashCorrectionPolicy');
const { requireOpeningReplacement } = require('../lib/cashDrawerSessionPolicy');
const { calculateBusinessDayTotals, validateBusinessDayFinalization } = require('../lib/cashBusinessDayPolicy');
const { participantLabels, validateSafeCountPayload, validateSafeCountType } = require('../lib/cashSafePolicy');
const { canCorrectCashClosing, canUseCashModule, isCentralRole, resolveCashBranch } = require('../lib/cashModuleAccess');
const { resolveCashBusinessDate } = require('../lib/cashBusinessDatePolicy');

const router = express.Router();
const PIN_PROOF_TTL_SECONDS = 10 * 60;
const attemptsByActor = new Map();

router.use(verifyToken());

function pinRateKey(req) {
  return `${req.user?.id || 'unknown'}:${req.ip || 'unknown'}`;
}

function consumePinAttempt(req) {
  const key = pinRateKey(req);
  const now = Date.now();
  const current = attemptsByActor.get(key);
  const entry = !current || current.resetAt <= now ? { count: 0, resetAt: now + 15 * 60_000 } : current;
  entry.count += 1;
  attemptsByActor.set(key, entry);
  return entry.count <= 10;
}

function clearPinAttempts(req) {
  attemptsByActor.delete(pinRateKey(req));
}

async function resolveBranch(client, req) {
  return resolveCashBranch(client, req);
}

async function ensureBusinessDayOpen(client, branchId, businessDate = null) {
  await client.query(`INSERT INTO core.cash_business_days (branch_id,business_date)
    VALUES ($1,COALESCE($2::date,(CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date))
    ON CONFLICT (branch_id,business_date) DO NOTHING`, [branchId,businessDate]);
  const result = await client.query(`SELECT id,business_date,status,finalized_at,final_report
    FROM core.cash_business_days
    WHERE branch_id=$1 AND business_date=COALESCE($2::date,(CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date)`, [branchId,businessDate]);
  if (result.rows[0].status !== 'open') {
    throw Object.assign(new Error('Dieser Geschäftstag wurde bereits verbindlich abgeschlossen und ist vollständig Read-only.'), { statusCode:409 });
  }
  return result.rows[0];
}

async function loadImmediatePreviousClosing(client, branchId, drawerNumber, businessDate) {
  const result = await client.query(`SELECT id,branch_id,drawer_number,difference_cents,resolved,status,closed_at
    FROM core.cash_drawer_closings
    WHERE branch_id=$1 AND drawer_number=$2 AND status='closed' AND business_date<=$3
    ORDER BY business_date DESC,closed_at DESC,id DESC LIMIT 1`, [branchId, drawerNumber, businessDate]);
  return result.rows[0] || null;
}

function signPinProof(pinRow, branchId) {
  return jwt.sign({ type: 'cash-pin-proof', pinId: Number(pinRow.id), branchId: Number(branchId) }, process.env.JWT_SECRET, { expiresIn: PIN_PROOF_TTL_SECONDS });
}

function verifyPinProof(token, branchId) {
  try {
    const proof = jwt.verify(String(token || ''), process.env.JWT_SECRET);
    if (proof.type !== 'cash-pin-proof' || Number(proof.branchId) !== Number(branchId) || !Number.isInteger(Number(proof.pinId))) throw new Error('invalid proof');
    return Number(proof.pinId);
  } catch {
    throw Object.assign(new Error('Eine Mitarbeiter-PIN-Bestätigung ist abgelaufen. Bitte den Vorgang neu beginnen.'), { statusCode: 401 });
  }
}

function signClosingContext(closedByPinId, responsibleEmployeePinId, branchId, drawerNumber) {
  return jwt.sign({
    type: 'cash-closing-context',
    closedByPinId: Number(closedByPinId),
    responsibleEmployeePinId: Number(responsibleEmployeePinId),
    branchId: Number(branchId),
    drawerNumber: Number(drawerNumber),
  }, process.env.JWT_SECRET, { expiresIn: PIN_PROOF_TTL_SECONDS });
}

function verifyClosingContext(token, { branchId, drawerNumber, closedByPinId, responsibleEmployeePinId }) {
  try {
    const proof = jwt.verify(String(token || ''), process.env.JWT_SECRET);
    if (proof.type !== 'cash-closing-context'
      || Number(proof.branchId) !== Number(branchId)
      || Number(proof.drawerNumber) !== Number(drawerNumber)
      || Number(proof.closedByPinId) !== Number(closedByPinId)
      || Number(proof.responsibleEmployeePinId) !== Number(responsibleEmployeePinId)) throw new Error('invalid closing context');
  } catch {
    throw Object.assign(new Error('Der Abrechnungskontext ist abgelaufen. Bitte die Abrechnung neu beginnen.'), { statusCode: 401 });
  }
}

function closingJson(row) {
  return {
    id: Number(row.id), businessDate: row.business_date, drawerNumber: Number(row.drawer_number),
    responsibleEmployee: { id: Number(row.employee_pin_id), name: row.responsible_employee_name },
    closedBy: { id: Number(row.closed_by_pin_id || row.employee_pin_id), name: row.closed_by_name || row.responsible_employee_name },
    openingCountId: row.opening_count_id == null ? null : Number(row.opening_count_id),
    openingPresent: row.opening_present === true, proceededWithoutOpening: row.proceeded_without_opening === true,
    openingBalanceCents: Number(row.opening_balance_cents), countedTotalCents: Number(row.counted_total_cents),
    fullCounts: row.full_counts || {}, retainedCoinCents: Number(row.retained_coin_cents),
    looseChangeCents: Number(row.loose_change_cents), bankNoteCounts: row.bank_note_counts || {},
    bankAmountCents: Number(row.bank_amount_cents), differenceCents: Number(row.difference_cents),
    notificationRequired: row.notification_required === true, notificationConfirmed: row.notification_confirmed === true,
    recountAttempts: row.recount_attempts || [], resolved: row.resolved === true, status: row.status,
    recordKind: row.record_kind || 'original', revisionNumber: Number(row.revision_number || 1),
    correctsClosingId: row.corrects_closing_id == null ? null : Number(row.corrects_closing_id),
    correctionReason: row.correction_reason || null, superseded: row.superseded === true,
    businessDayStatus: row.business_day_status || 'open',
    closedAt: row.closed_at,
  };
}

function openingJson(row) {
  return {
    id:Number(row.id), businessDate:row.business_date, drawerNumber:Number(row.drawer_number),
    employee:{ id:Number(row.employee_pin_id), name:row.employee_name },
    finalCounter:{ id:Number(row.final_counter_pin_id), name:row.final_counter_name },
    openingBalanceCents:Number(row.opening_balance_cents), openingDifferenceCents:Number(row.opening_difference_cents),
    lifecycleStatus:row.lifecycle_status || 'active', replacedByOpeningId:row.replaced_by_opening_id == null ? null : Number(row.replaced_by_opening_id),
    completedAt:row.completed_at, endedAt:row.ended_at || null,
  };
}

function safeCountJson(row) {
  return {
    id:Number(row.id), businessDate:row.business_date, countType:row.count_type,
    counter:{ id:Number(row.counter_pin_id), name:row.counter_name },
    recorder:{ id:Number(row.recorder_pin_id), name:row.recorder_name },
    participants:[
      { id:Number(row.counter_pin_id), name:row.counter_name, role:row.first_employee_role },
      { id:Number(row.recorder_pin_id), name:row.recorder_name, role:row.second_employee_role },
    ],
    mode:row.count_mode || 'full_count',
    previousCountId:row.previous_count_id == null ? null : Number(row.previous_count_id),
    previousEveningCountId:row.previous_evening_count_id == null ? null : Number(row.previous_evening_count_id),
    fixedDrawerCents:Number(row.fixed_drawer_cents || 0), littleBerthaCents:Number(row.little_bertha_cents || 0),
    looseMoneyCents:Number(row.loose_money_cents || 0), coinRollCounts:row.coin_roll_counts || {},
    rollTotalCents:Number(row.coin_roll_total_cents || 0), banknoteCounts:row.banknote_counts || {},
    banknoteTotalCents:Number(row.banknote_total_cents || 0),
    countedTotalCents:Number(row.counted_total_cents), differenceCents:row.eurocontrol_total_cents == null ? null : Number(row.difference_cents),
    eurocontrolTotalCents:row.eurocontrol_total_cents == null ? null : Number(row.eurocontrol_total_cents),
    status:row.status, completedAt:row.completed_at,
  };
}

async function loadSafeContext(client, branchId, countType, businessDate) {
  const settings = await client.query(`SELECT fixed_drawer_cents,little_bertha_cents FROM core.cash_safe_settings WHERE branch_id=$1`, [branchId]);
  if (!settings.rows[0]) throw Object.assign(new Error('Für diese Filiale ist noch kein Tresor-Standardbestand hinterlegt.'), { statusCode:409 });
  const previous = await client.query(`SELECT c.*,counter.display_name AS counter_name,recorder.display_name AS recorder_name
    FROM core.cash_safe_counts c
    JOIN core.filiale_pins counter ON counter.id=c.counter_pin_id
    JOIN core.filiale_pins recorder ON recorder.id=c.recorder_pin_id
    WHERE c.branch_id=$1 AND c.count_type='closing'
      AND c.business_date<$2
      AND c.first_employee_role IS NOT NULL
    ORDER BY c.business_date DESC,c.completed_at DESC,c.id DESC LIMIT 1`, [branchId,businessDate]);
  const existing = await client.query(`SELECT c.*,counter.display_name AS counter_name,recorder.display_name AS recorder_name
    FROM core.cash_safe_counts c
    JOIN core.filiale_pins counter ON counter.id=c.counter_pin_id
    JOIN core.filiale_pins recorder ON recorder.id=c.recorder_pin_id
    WHERE c.branch_id=$1 AND c.business_date=$2 AND c.count_type=$3
    ORDER BY c.completed_at DESC,c.id DESC LIMIT 1`, [branchId,businessDate,countType]);
  return {
    settings:{ fixedDrawerCents:Number(settings.rows[0].fixed_drawer_cents), littleBerthaCents:Number(settings.rows[0].little_bertha_cents) },
    previousEvening:previous.rows[0] ? safeCountJson(previous.rows[0]) : null,
    existingCount:existing.rows[0] ? safeCountJson(existing.rows[0]) : null,
  };
}

async function loadBusinessDayCloseContext(client, branch, businessDay) {
  const date = businessDay.business_date;
  const [settingsResult, baseSafeResult, finalSafeResult] = await Promise.all([
    client.query('SELECT standard_balance_cents FROM core.cash_safe_settings WHERE branch_id=$1', [branch.id]),
    client.query(`SELECT c.*,counter.display_name AS counter_name,recorder.display_name AS recorder_name
      FROM core.cash_safe_counts c
      JOIN core.filiale_pins counter ON counter.id=c.counter_pin_id
      JOIN core.filiale_pins recorder ON recorder.id=c.recorder_pin_id
      WHERE c.branch_id=$1 AND ((c.business_date<$2 AND c.count_type='closing') OR (c.business_date=$2 AND c.count_type='morning'))
      ORDER BY c.business_date DESC,c.completed_at DESC,c.id DESC LIMIT 1`, [branch.id,date]),
    client.query(`SELECT c.*,counter.display_name AS counter_name,recorder.display_name AS recorder_name
      FROM core.cash_safe_counts c
      JOIN core.filiale_pins counter ON counter.id=c.counter_pin_id
      JOIN core.filiale_pins recorder ON recorder.id=c.recorder_pin_id
      WHERE c.branch_id=$1 AND c.business_date=$2 AND c.count_type='closing'
      ORDER BY c.completed_at DESC,c.id DESC LIMIT 1`, [branch.id,date]),
  ]);
  const baseSafe = baseSafeResult.rows[0] || null;
  const [closingResult, openingResult, looseResult, lastCashResult] = await Promise.all([
    client.query(`SELECT c.*,responsible.display_name AS responsible_employee_name,
        closer.display_name AS closed_by_name,day.status AS business_day_status,
        false AS superseded
      FROM core.cash_drawer_closings c
      JOIN core.filiale_pins responsible ON responsible.id=c.employee_pin_id
      LEFT JOIN core.filiale_pins closer ON closer.id=COALESCE(c.closed_by_pin_id,c.employee_pin_id)
      LEFT JOIN core.cash_business_days day ON day.branch_id=c.branch_id AND day.business_date=c.business_date
      WHERE c.branch_id=$1 AND c.business_date=$2
        AND NOT EXISTS (SELECT 1 FROM core.cash_drawer_closings newer WHERE newer.corrects_closing_id=c.id)
      ORDER BY c.closed_at,c.id`, [branch.id,date]),
    client.query(`SELECT o.*,employee.display_name AS employee_name,final_counter.display_name AS final_counter_name
      FROM core.cash_drawer_opening_counts o
      JOIN core.filiale_pins employee ON employee.id=o.employee_pin_id
      JOIN core.filiale_pins final_counter ON final_counter.id=o.final_counter_pin_id
      WHERE o.branch_id=$1 AND o.business_date=$2
      ORDER BY o.completed_at,o.id`, [branch.id,date]),
    client.query(`SELECT COALESCE(SUM(c.loose_change_cents),0)::bigint AS total
      FROM core.cash_drawer_closings c
      WHERE c.branch_id=$1 AND c.business_date=$2 AND c.record_kind='original'
        AND ($3::timestamptz IS NULL OR c.closed_at>$3::timestamptz)`, [branch.id,date,baseSafe?.completed_at || null]),
    client.query(`SELECT MAX(closed_at) AS latest FROM core.cash_drawer_closings
      WHERE branch_id=$1 AND business_date=$2 AND record_kind='original'`, [branch.id,date]),
  ]);
  const settings = settingsResult.rows[0];
  if (!settings) throw Object.assign(new Error('Für diese Filiale ist kein Tresor-Standardbestand hinterlegt.'), { statusCode:409 });
  const finalSafeRow = finalSafeResult.rows[0] || null;
  const closings = closingResult.rows.map(closingJson);
  const openings = openingResult.rows.map(openingJson);
  const standardBalanceCents = Number(settings.standard_balance_cents);
  const totals = calculateBusinessDayTotals({
    closings,
    standardBalanceCents,
    baseSafeCountCents:baseSafe ? Number(baseSafe.counted_total_cents) : standardBalanceCents,
    looseAfterBaseCents:Number(looseResult.rows[0]?.total || 0),
  });
  const latestCashAt = lastCashResult.rows[0]?.latest || null;
  const finalSafeCount = finalSafeRow ? safeCountJson(finalSafeRow) : null;
  return {
    branch:branch.name, businessDate:date, businessDayStatus:businessDay.status,
    closings, openings,
    activeOpenings:openings.filter((item)=>item.lifecycleStatus==='active'),
    replacedOpenings:openings.filter((item)=>item.lifecycleStatus==='replaced'),
    baseSafeCount:baseSafe ? safeCountJson(baseSafe) : null,
    finalSafeCount,
    finalSafeCountIsCurrent:Boolean(finalSafeCount && (!latestCashAt || new Date(finalSafeCount.completedAt)>=new Date(latestCashAt))),
    latestCashAt,
    totals,
  };
}

router.get('/branches', async (req, res) => {
  try {
    if (!canUseCashModule(req.user)) return res.status(403).json({ message: 'Die Tageskassenabrechnung ist während der DEV-Phase nicht freigegeben.' });
    const assigned = String(req.user?.filiale || '').trim();
    if (isCentralRole(req.user?.role) && assigned.toLowerCase() === 'alle') {
      const result = await pool.query(`SELECT id,name FROM public.filialen
        WHERE aktiv=true AND LOWER(name)<>'alle' ORDER BY name`);
      return res.json({ branches: result.rows.map((row) => ({ id: Number(row.id), name: row.name })) });
    }
    const branch = await resolveBranch(pool, req);
    return res.json({ branches: [{ id: Number(branch.id), name: branch.name }] });
  } catch (error) {
    console.error('GET /api/tageskasse/branches Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Filialen konnten nicht geladen werden.' });
  }
});

router.get('/employees', async (req, res) => {
  try {
    await resolveBranch(pool, req);
    const result = await pool.query(`SELECT id,display_name FROM core.filiale_pins
      WHERE is_active=true ORDER BY LOWER(display_name),id`);
    return res.json({ employees: result.rows.map((row)=>({ id:Number(row.id), name:row.display_name })) });
  } catch (error) {
    console.error('GET /api/tageskasse/employees Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Mitarbeiter konnten nicht geladen werden.' });
  }
});

router.get('/safe-counts/context', async (req, res) => {
  try {
    let countType;
    try { countType = validateSafeCountType(req.query?.type); }
    catch (validationError) { throw Object.assign(validationError, { statusCode:400 }); }
    const branch = await resolveBranch(pool, req);
    const requestedDate = resolveCashBusinessDate(req);
    const businessDay = await ensureBusinessDayOpen(pool, branch.id, requestedDate);
    const context = await loadSafeContext(pool, branch.id, countType, requestedDate);
    return res.json({ branch:branch.name, countType, context:{ ...context, businessDate:businessDay.business_date } });
  } catch (error) {
    console.error('GET /api/tageskasse/safe-counts/context Fehler:', error);
    return res.status(error.statusCode || 500).json({ message:error.statusCode ? error.message : 'Tresorkontext konnte nicht geladen werden.' });
  }
});

router.get('/safe-counts', async (req, res) => {
  try {
    const branch = await resolveBranch(pool, req);
    const businessDate = resolveCashBusinessDate(req);
    const result = await pool.query(`SELECT c.*,counter.display_name AS counter_name,recorder.display_name AS recorder_name
      FROM core.cash_safe_counts c
      JOIN core.filiale_pins counter ON counter.id=c.counter_pin_id
      JOIN core.filiale_pins recorder ON recorder.id=c.recorder_pin_id
      WHERE c.branch_id=$1 AND c.business_date=$2
      ORDER BY c.completed_at,c.id`, [branch.id,businessDate]);
    return res.json({ branch:branch.name, counts:result.rows.map(safeCountJson) });
  } catch (error) {
    console.error('GET /api/tageskasse/safe-counts Fehler:', error);
    return res.status(error.statusCode || 500).json({ message:error.statusCode ? error.message : 'Tresorzählungen konnten nicht geladen werden.' });
  }
});

router.post('/safe-counts', async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const branch = await resolveBranch(client, req);
    const businessDate = resolveCashBusinessDate(req);
    await ensureBusinessDayOpen(client, branch.id, businessDate);
    let countType;
    try {
      countType = validateSafeCountType(req.body?.countType);
    } catch (validationError) {
      throw Object.assign(validationError, { statusCode:400 });
    }
    const counterPinId = verifyPinProof(req.body?.counterProofToken, branch.id);
    const recorderPinId = verifyPinProof(req.body?.recorderProofToken, branch.id);
    if (counterPinId === recorderPinId) throw Object.assign(new Error('Zählende und eingebende Person müssen zwei unterschiedliche Mitarbeiter sein.'), { statusCode:400 });
    const employees = await client.query(`SELECT id,display_name FROM core.filiale_pins WHERE id=ANY($1::bigint[]) AND is_active=true`, [[counterPinId,recorderPinId]]);
    const employeeById = new Map(employees.rows.map((row)=>[Number(row.id),row]));
    if (!employeeById.has(counterPinId) || !employeeById.has(recorderPinId)) throw Object.assign(new Error('Mindestens eine bestätigte Person ist nicht mehr aktiv.'), { statusCode:409 });
    const context = await loadSafeContext(client, branch.id, countType, businessDate);
    const repeatClosing = countType === 'closing' && context.existingCount && req.body?.repeatClosingConfirmed === true;
    if (context.existingCount && !repeatClosing) throw Object.assign(new Error('Diese Tresorzählung wurde für heute bereits verbindlich abgeschlossen.'), { statusCode:409 });
    let validated;
    try {
      validated = validateSafeCountPayload(req.body, context.settings);
    } catch (validationError) {
      throw Object.assign(validationError, { statusCode:400 });
    }
    let figures = validated.result;
    let previousEveningCountId = null;
    let previousCountId = repeatClosing ? context.existingCount.id : null;
    if (validated.mode === 'confirmation') {
      const previous = context.previousEvening;
      if (!previous || Number(previous.id) !== Number(req.body?.previousEveningCountId)) {
        throw Object.assign(new Error('Die bestätigte Schlusszählung des Vortags wurde nicht gefunden.'), { statusCode:409 });
      }
      previousEveningCountId = previous.id;
      previousCountId = previous.id;
      figures = {
        fixedDrawerCents:previous.fixedDrawerCents, littleBerthaCents:previous.littleBerthaCents,
        looseMoneyCents:previous.looseMoneyCents, coinRollCounts:previous.coinRollCounts,
        rollTotalCents:previous.rollTotalCents, banknoteCounts:previous.banknoteCounts,
        banknoteTotalCents:previous.banknoteTotalCents, countedTotalCents:previous.countedTotalCents,
      };
    }
    const labels = participantLabels(countType);
    const legacyDifference = validated.differenceCents ?? 0;
    const result = await client.query(`INSERT INTO core.cash_safe_counts
      (branch_id,count_type,counter_pin_id,recorder_pin_id,previous_count_id,standard_balance_cents,
       base_balance_cents,added_loose_change_cents,balance_before_skim_cents,skimmed_cents,
       expected_balance_cents,counted_total_cents,difference_cents,denomination_counts,created_by_user_id,
       count_mode,previous_evening_count_id,fixed_drawer_cents,little_bertha_cents,loose_money_cents,
       coin_roll_counts,coin_roll_total_cents,banknote_counts,banknote_total_cents,eurocontrol_total_cents,
       first_employee_role,second_employee_role,business_date)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15,$16,$17,$18,$19,$20,$21::jsonb,$22,$23::jsonb,$24,$25,$26,$27,$28)
      RETURNING id,business_date,completed_at`, [
      branch.id,countType,counterPinId,recorderPinId,previousCountId,
      figures.fixedDrawerCents + figures.littleBerthaCents,figures.countedTotalCents,figures.looseMoneyCents,
      figures.countedTotalCents,0,validated.eurocontrolTotalCents ?? figures.countedTotalCents,
      figures.countedTotalCents,legacyDifference,JSON.stringify({ rolls:figures.coinRollCounts, notes:figures.banknoteCounts }),req.user.id,
      validated.mode,previousEveningCountId,figures.fixedDrawerCents,figures.littleBerthaCents,figures.looseMoneyCents,
      JSON.stringify(figures.coinRollCounts),figures.rollTotalCents,JSON.stringify(figures.banknoteCounts),
      figures.banknoteTotalCents,validated.eurocontrolTotalCents,labels[0],labels[1],businessDate,
    ]);
    await client.query('COMMIT');
    return res.status(201).json({ safeCount:{
      id:Number(result.rows[0].id), businessDate:result.rows[0].business_date, completedAt:result.rows[0].completed_at,
      branch:branch.name, countType, counter:{id:counterPinId,name:employeeById.get(counterPinId).display_name},
      recorder:{id:recorderPinId,name:employeeById.get(recorderPinId).display_name},
      participants:[
        {id:counterPinId,name:employeeById.get(counterPinId).display_name,role:labels[0]},
        {id:recorderPinId,name:employeeById.get(recorderPinId).display_name,role:labels[1]},
      ],
      mode:validated.mode,previousEveningCountId,...figures,
      eurocontrolTotalCents:validated.eurocontrolTotalCents,differenceCents:validated.differenceCents,status:'completed',
    } });
  } catch (error) {
    await client.query('ROLLBACK').catch(()=>{});
    console.error('POST /api/tageskasse/safe-counts Fehler:', error);
    const duplicate = error.code === '23505' && String(error.constraint || '').includes('cash_safe_counts_slot');
    return res.status(duplicate ? 409 : (error.statusCode || 500)).json({ message:duplicate ? 'Diese Tresorzählung wurde für heute bereits abgeschlossen.' : (error.statusCode ? error.message : 'Die Tresorzählung konnte nicht gespeichert werden.') });
  } finally { client.release(); }
});

router.get('/day-close/context', async (req, res) => {
  try {
    const branch = await resolveBranch(pool, req);
    const businessDate = resolveCashBusinessDate(req);
    const businessDay = await ensureBusinessDayOpen(pool, branch.id, businessDate);
    const context = await loadBusinessDayCloseContext(pool, branch, businessDay);
    return res.json({ context });
  } catch (error) {
    console.error('GET /api/tageskasse/day-close/context Fehler:', error);
    return res.status(error.statusCode || 500).json({ message:error.statusCode ? error.message : 'Der Tagesabschluss konnte nicht vorbereitet werden.' });
  }
});

router.post('/day-close/finalize', async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const branch = await resolveBranch(client, req);
    const businessDate = resolveCashBusinessDate(req);
    const businessDay = await ensureBusinessDayOpen(client, branch.id, businessDate);
    await client.query('SELECT pg_advisory_xact_lock($1,$2)', [branch.id,424242]);
    const lockedDay = await client.query('SELECT * FROM core.cash_business_days WHERE id=$1 FOR UPDATE', [businessDay.id]);
    if (lockedDay.rows[0].status !== 'open') throw Object.assign(new Error('Dieser Geschäftstag wurde bereits verbindlich abgeschlossen.'), { statusCode:409 });
    const context = await loadBusinessDayCloseContext(client, branch, lockedDay.rows[0]);
    try { validateBusinessDayFinalization(req.body || {}, context); }
    catch (validationError) { throw Object.assign(validationError, { statusCode:400 }); }
    const finalizedAt = new Date().toISOString();
    const report = {
      branch:branch.name, businessDate:context.businessDate, finalizedAt,
      totals:context.totals,
      closings:context.closings,
      replacedOpenings:context.replacedOpenings,
      finalSafeCount:context.finalSafeCount,
      eurocontrolCashTotalCents:Number(req.body.eurocontrolCashTotalCents),
      looseChangePayoutConfirmed:context.totals.looseChangePayoutCents === 0 || req.body.looseChangePayoutConfirmed === true,
      additionalSafeSkimConfirmed:context.totals.additionalSafeSkimCents === 0 || req.body.additionalSafeSkimConfirmed === true,
      bankPayoutConfirmed:true, safeBagConfirmed:true,
    };
    const result = await client.query(`UPDATE core.cash_business_days
      SET status='finalized',finalized_at=$1,finalized_by_user_id=$2,final_report=$3::jsonb
      WHERE id=$4 AND status='open'
      RETURNING id,business_date,status,finalized_at,final_report`, [finalizedAt,req.user.id,JSON.stringify(report),businessDay.id]);
    if (!result.rows[0]) throw Object.assign(new Error('Der Geschäftstag wurde zwischenzeitlich bereits abgeschlossen.'), { statusCode:409 });
    await client.query('COMMIT');
    return res.status(201).json({ report:result.rows[0].final_report });
  } catch (error) {
    await client.query('ROLLBACK').catch(()=>{});
    console.error('POST /api/tageskasse/day-close/finalize Fehler:', error);
    return res.status(error.statusCode || 500).json({ message:error.statusCode ? error.message : 'Der Geschäftstag konnte nicht abgeschlossen werden.' });
  } finally { client.release(); }
});

router.get('/business-days', async (req, res) => {
  try {
    const branch = await resolveBranch(pool, req);
    const result = await pool.query(`SELECT business_date,status,finalized_at,final_report
      FROM core.cash_business_days WHERE branch_id=$1 AND status='finalized'
      ORDER BY business_date DESC LIMIT 120`, [branch.id]);
    return res.json({ days:result.rows.map((row)=>({
      businessDate:row.business_date,status:row.status,finalizedAt:row.finalized_at,
      totals:row.final_report?.totals || {}, closingCount:Array.isArray(row.final_report?.closings) ? row.final_report.closings.length : 0,
    })) });
  } catch (error) {
    console.error('GET /api/tageskasse/business-days Fehler:', error);
    return res.status(error.statusCode || 500).json({ message:error.statusCode ? error.message : 'Abgeschlossene Geschäftstage konnten nicht geladen werden.' });
  }
});

router.get('/business-days/:date', async (req, res) => {
  try {
    const businessDate = String(req.params.date || '');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(businessDate)) return res.status(400).json({ message:'Das Datum ist ungültig.' });
    const branch = await resolveBranch(pool, req);
    const result = await pool.query(`SELECT final_report FROM core.cash_business_days
      WHERE branch_id=$1 AND business_date=$2 AND status='finalized'`, [branch.id,businessDate]);
    if (!result.rows[0]) return res.status(404).json({ message:'Für dieses Datum wurde kein abgeschlossener Tagesbericht gefunden.' });
    return res.json({ report:result.rows[0].final_report });
  } catch (error) {
    console.error('GET /api/tageskasse/business-days/:date Fehler:', error);
    return res.status(error.statusCode || 500).json({ message:error.statusCode ? error.message : 'Der Tagesbericht konnte nicht geladen werden.' });
  }
});

router.get('/closings', async (req, res) => {
  try {
    const branch = await resolveBranch(pool, req);
    const businessDate = String(req.query?.businessDate || '').trim();
    if (businessDate && !/^\d{4}-\d{2}-\d{2}$/.test(businessDate)) return res.status(400).json({ message:'Das Datum ist ungültig.' });
    const values = [branch.id];
    let dateFilter = '';
    if (businessDate) { values.push(businessDate); dateFilter = `AND c.business_date=$${values.length}`; }
    const result = await pool.query(`SELECT c.*,responsible.display_name AS responsible_employee_name,
        closer.display_name AS closed_by_name,day.status AS business_day_status,
        EXISTS (SELECT 1 FROM core.cash_drawer_closings newer WHERE newer.corrects_closing_id=c.id) AS superseded
      FROM core.cash_drawer_closings c
      JOIN core.filiale_pins responsible ON responsible.id=c.employee_pin_id
      LEFT JOIN core.filiale_pins closer ON closer.id=COALESCE(c.closed_by_pin_id,c.employee_pin_id)
      LEFT JOIN core.cash_business_days day ON day.branch_id=c.branch_id AND day.business_date=c.business_date
      WHERE c.branch_id=$1 ${dateFilter}
      ORDER BY c.business_date DESC,c.closed_at DESC,c.id DESC LIMIT 200`, values);
    const dayStatus = businessDate ? await pool.query(`SELECT status FROM core.cash_business_days WHERE branch_id=$1 AND business_date=$2`, [branch.id,businessDate]) : { rows:[] };
    return res.json({ branch:branch.name, businessDayStatus:dayStatus.rows[0]?.status || 'open', closings:result.rows.map(closingJson), canCorrect:canCorrectCashClosing(req.user?.role) });
  } catch (error) {
    console.error('GET /api/tageskasse/closings Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Abrechnungsarchiv konnte nicht geladen werden.' });
  }
});

router.get('/closings/:id', async (req, res) => {
  try {
    const branch = await resolveBranch(pool, req);
    const closingId = Number(req.params.id);
    if (!Number.isInteger(closingId) || closingId <= 0) return res.status(400).json({ message:'Die Abrechnung ist ungültig.' });
    const result = await pool.query(`SELECT c.*,responsible.display_name AS responsible_employee_name,
        closer.display_name AS closed_by_name,day.status AS business_day_status,
        EXISTS (SELECT 1 FROM core.cash_drawer_closings newer WHERE newer.corrects_closing_id=c.id) AS superseded
      FROM core.cash_drawer_closings c
      JOIN core.filiale_pins responsible ON responsible.id=c.employee_pin_id
      LEFT JOIN core.filiale_pins closer ON closer.id=COALESCE(c.closed_by_pin_id,c.employee_pin_id)
      LEFT JOIN core.cash_business_days day ON day.branch_id=c.branch_id AND day.business_date=c.business_date
      WHERE c.id=$1 AND c.branch_id=$2 LIMIT 1`, [closingId,branch.id]);
    if (!result.rows[0]) return res.status(404).json({ message:'Die Abrechnung wurde nicht gefunden.' });
    return res.json({ closing:closingJson(result.rows[0]), canCorrect:canCorrectCashClosing(req.user?.role) });
  } catch (error) {
    console.error('GET /api/tageskasse/closings/:id Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Abrechnung konnte nicht geladen werden.' });
  }
});

router.post('/pin/verify', async (req, res) => {
  try {
    if (!consumePinAttempt(req)) return res.status(429).json({ message: 'Zu viele PIN-Versuche. Bitte 15 Minuten warten.' });
    const pin = String(req.body?.pin || '').trim();
    if (!/^\d{4}$/.test(pin)) return res.status(400).json({ message: 'Der Mitarbeiter-PIN muss genau vier Ziffern haben.' });
    if (pin === '0000') {
      return res.status(409).json({
        code: 'PIN_CHANGE_REQUIRED',
        message: 'Bitte zuerst einen persönlichen PIN festlegen.',
      });
    }

    const branch = await resolveBranch(pool, req);
    const excludedId = req.body?.excludeEmployeeId == null ? null : Number(req.body.excludeEmployeeId);
    const candidates = await pool.query(`SELECT id,display_name,pin_hash,must_change_pin,locked_until
      FROM core.filiale_pins WHERE is_active=true ORDER BY id`);

    const matches = await findActivePinMatches(candidates.rows, pin, bcrypt.compare);
    if (matches.length > 1) {
      return res.status(409).json({
        code: 'PIN_AMBIGUOUS',
        message: 'Dieser PIN ist mehreren Mitarbeitern zugeordnet. Bitte die betroffenen PINs in der Verwaltung zurücksetzen.',
      });
    }
    const matched = matches[0];
    if (!matched) return res.status(401).json({ message: 'PIN ungültig.' });
    if (excludedId && Number(matched.id) === excludedId) return res.status(400).json({ message: 'Für diese Nachzählung muss eine andere Person ihren PIN eingeben.' });

    clearPinAttempts(req);
    await pool.query('UPDATE core.filiale_pins SET failed_attempts=0,locked_until=NULL,last_failed_at=NULL,last_used_at=NOW() WHERE id=$1', [matched.id]);
    return res.json({ employee: { id: Number(matched.id), name: matched.display_name, proofToken: signPinProof(matched, branch.id) } });
  } catch (error) {
    console.error('POST /api/tageskasse/pin/verify Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'PIN konnte nicht geprüft werden.' });
  }
});

router.get('/previous-closing', async (req, res) => {
  try {
    const drawerNumber = Number(req.query?.drawer);
    if (![1, 2].includes(drawerNumber)) return res.status(400).json({ message: 'Bitte Lade 1 oder Lade 2 auswählen.' });
    const branch = await resolveBranch(pool, req);
    const businessDate = resolveCashBusinessDate(req);
    const closing = await loadImmediatePreviousClosing(pool, branch.id, drawerNumber, businessDate);
    return res.json({ closing: closing ? {
      id: Number(closing.id), branch: branch.name, drawerNumber: Number(closing.drawer_number),
      differenceCents: Number(closing.difference_cents), resolved: closing.resolved === true,
      status: closing.status, closedAt: closing.closed_at,
    } : null });
  } catch (error) {
    console.error('GET /api/tageskasse/previous-closing Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Die letzte Abrechnung konnte nicht geladen werden.' });
  }
});

router.get('/day-overview', async (req, res) => {
  try {
    const branch = await resolveBranch(pool, req);
    const businessDate = resolveCashBusinessDate(req);
    await pool.query(`INSERT INTO core.cash_business_days (branch_id,business_date)
      VALUES ($1,$2)
      ON CONFLICT (branch_id,business_date) DO NOTHING`, [branch.id,businessDate]);
    const dayResult = await pool.query(`SELECT id,business_date,status,finalized_at,final_report
      FROM core.cash_business_days WHERE branch_id=$1 AND business_date=$2`, [branch.id,businessDate]);
    const businessDay = dayResult.rows[0];
    const [openings, closings] = await Promise.all([
      pool.query(`SELECT o.*,employee.display_name AS employee_name,final_counter.display_name AS final_counter_name
        FROM core.cash_drawer_opening_counts o
        JOIN core.filiale_pins employee ON employee.id=o.employee_pin_id
        JOIN core.filiale_pins final_counter ON final_counter.id=o.final_counter_pin_id
        WHERE o.branch_id=$1 AND o.business_date=$2
        ORDER BY o.completed_at,o.id`, [branch.id,businessDay.business_date]),
      pool.query(`SELECT c.*,responsible.display_name AS responsible_employee_name,
          closer.display_name AS closed_by_name,day.status AS business_day_status,
          EXISTS (SELECT 1 FROM core.cash_drawer_closings newer WHERE newer.corrects_closing_id=c.id) AS superseded
        FROM core.cash_drawer_closings c
        JOIN core.filiale_pins responsible ON responsible.id=c.employee_pin_id
        LEFT JOIN core.filiale_pins closer ON closer.id=COALESCE(c.closed_by_pin_id,c.employee_pin_id)
        LEFT JOIN core.cash_business_days day ON day.branch_id=c.branch_id AND day.business_date=c.business_date
        WHERE c.branch_id=$1 AND c.business_date=$2
        ORDER BY c.closed_at,c.id`, [branch.id,businessDay.business_date]),
    ]);
    return res.json({
      branch:branch.name, businessDate:businessDay.business_date, businessDayStatus:businessDay.status,
      finalizedAt:businessDay.finalized_at || null, finalReport:businessDay.final_report || null,
      openings:openings.rows.map(openingJson), closings:closings.rows.map(closingJson),
      canCorrect:canCorrectCashClosing(req.user?.role),
    });
  } catch (error) {
    console.error('GET /api/tageskasse/day-overview Fehler:', error);
    return res.status(error.statusCode || 500).json({ message:error.statusCode ? error.message : 'Der laufende Geschäftstag konnte nicht geladen werden.' });
  }
});

router.post('/opening-counts', async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const branch = await resolveBranch(client, req);
    const businessDate = resolveCashBusinessDate(req);
    await ensureBusinessDayOpen(client, branch.id, businessDate);
    const drawerNumber = Number(req.body?.drawerNumber);
    if (![1, 2].includes(drawerNumber)) throw Object.assign(new Error('Bitte Lade 1 oder Lade 2 auswählen.'), { statusCode: 400 });
    await client.query('SELECT pg_advisory_xact_lock($1,$2)', [branch.id,drawerNumber]);
    const activeResult = await client.query(`SELECT o.id,o.drawer_number,o.employee_pin_id,employee.display_name AS employee_name,o.completed_at
      FROM core.cash_drawer_opening_counts o
      JOIN core.filiale_pins employee ON employee.id=o.employee_pin_id
      WHERE o.branch_id=$1 AND o.business_date=$3
        AND o.drawer_number=$2 AND o.lifecycle_status='active'
      ORDER BY o.completed_at DESC,o.id DESC LIMIT 1 FOR UPDATE OF o`, [branch.id,drawerNumber,businessDate]);
    const activeOpening = activeResult.rows[0] || null;
    const replacedOpeningId = requireOpeningReplacement(activeOpening, req.body?.replaceActiveOpeningId);
    const previousClosing = await loadImmediatePreviousClosing(client, branch.id, drawerNumber, businessDate);

    const rawAttempts = Array.isArray(req.body?.attempts) ? req.body.attempts : [];
    const proofIds = rawAttempts.map((attempt) => verifyPinProof(attempt?.proofToken, branch.id));
    const pins = proofIds.length ? await client.query('SELECT id,display_name FROM core.filiale_pins WHERE is_active=true AND id=ANY($1::bigint[])', [proofIds]) : { rows: [] };
    const pinById = new Map(pins.rows.map((row) => [Number(row.id), row]));
    if (proofIds.some((id) => !pinById.has(id))) throw Object.assign(new Error('Mindestens eine bestätigte Mitarbeiter-PIN ist nicht mehr aktiv.'), { statusCode: 409 });

    const preparedInput = { ...req.body, drawerNumber, attempts: rawAttempts.map((attempt, index) => ({
      counts: attempt.counts,
      employeePinId: proofIds[index],
      employeeName: pinById.get(proofIds[index]).display_name,
    })) };
    const checked = validateOpeningCount(preparedInput, previousClosing, branch.id);
    const firstEmployee = checked.attempts[0];
    const finalEmployee = checked.attempts.at(-1);
    if (replacedOpeningId) {
      await client.query(`UPDATE core.cash_drawer_opening_counts
        SET lifecycle_status='replaced',ended_at=NOW()
        WHERE id=$1 AND lifecycle_status='active'`, [replacedOpeningId]);
    }
    const result = await client.query(`INSERT INTO core.cash_drawer_opening_counts
      (branch_id,drawer_number,employee_pin_id,final_counter_pin_id,target_cents,initial_counted_cents,
       initial_difference_cents,opening_balance_cents,opening_difference_cents,previous_closing_id,
       carryover_decision,attempts,created_by_user_id,business_date)
      VALUES ($1,$2,$3,$4,25000,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13)
      RETURNING id,business_date,completed_at`, [
      branch.id, checked.drawerNumber, firstEmployee.employeePinId, finalEmployee.employeePinId,
      checked.initialCountedCents, checked.initialDifferenceCents, checked.openingBalanceCents,
      checked.openingDifferenceCents, checked.previousClosingId, checked.carryoverDecision,
      JSON.stringify(checked.attempts), req.user.id, businessDate,
    ]);
    if (replacedOpeningId) {
      await client.query('UPDATE core.cash_drawer_opening_counts SET replaced_by_opening_id=$1 WHERE id=$2', [result.rows[0].id,replacedOpeningId]);
    }
    await client.query('COMMIT');
    return res.status(201).json({ openingCount: {
      id: Number(result.rows[0].id), businessDate: result.rows[0].business_date,
      completedAt: result.rows[0].completed_at, branch: branch.name, drawerNumber: checked.drawerNumber,
      employee: { id: firstEmployee.employeePinId, name: firstEmployee.employeeName },
      openingBalanceCents: checked.openingBalanceCents, openingDifferenceCents: checked.openingDifferenceCents,
      carryoverDecision: checked.carryoverDecision, previousClosingId: checked.previousClosingId,
      lifecycleStatus:'active', replacedOpeningId,
    } });
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('POST /api/tageskasse/opening-counts Fehler:', error);
    return res.status(error.statusCode || 500).json({
      message:error.statusCode ? error.message : 'Die Einzählung konnte nicht gespeichert werden.',
      code:error.code || undefined,
      activeOpening:error.activeOpening ? {
        id:Number(error.activeOpening.id), drawerNumber:Number(error.activeOpening.drawer_number),
        employee:{ id:Number(error.activeOpening.employee_pin_id), name:error.activeOpening.employee_name },
        completedAt:error.activeOpening.completed_at,
      } : undefined,
    });
  } finally {
    client.release();
  }
});

router.post('/closing-context', async (req, res) => {
  try {
    const branch = await resolveBranch(pool, req);
    const businessDate = resolveCashBusinessDate(req);
    await ensureBusinessDayOpen(pool, branch.id, businessDate);
    const drawerNumber = Number(req.body?.drawerNumber);
    if (![1, 2].includes(drawerNumber)) return res.status(400).json({ message: 'Bitte Lade 1 oder Lade 2 auswählen.' });
    const closedByPinId = verifyPinProof(req.body?.proofToken, branch.id);
    const responsibleEmployeePinId = req.body?.responsibleEmployeeId == null ? closedByPinId : Number(req.body.responsibleEmployeeId);
    if (!Number.isInteger(responsibleEmployeePinId) || responsibleEmployeePinId <= 0) return res.status(400).json({ message:'Bitte wähle den verantwortlichen Kassierer aus.' });
    const employees = await pool.query(`SELECT id,display_name FROM core.filiale_pins
      WHERE id=ANY($1::bigint[]) AND is_active=true`, [[closedByPinId,responsibleEmployeePinId]]);
    const employeeById = new Map(employees.rows.map((row)=>[Number(row.id),row]));
    if (!employeeById.has(closedByPinId) || !employeeById.has(responsibleEmployeePinId)) return res.status(409).json({ message: 'Mindestens eine beteiligte Person ist nicht mehr aktiv.' });
    const opening = await pool.query(`SELECT o.id,o.business_date,o.drawer_number,o.opening_balance_cents,o.completed_at
      FROM core.cash_drawer_opening_counts o
      WHERE o.branch_id=$1 AND o.drawer_number=$2 AND o.employee_pin_id=$3
        AND o.business_date=$4
        AND o.status='completed'
        AND o.lifecycle_status='active'
        AND NOT EXISTS (SELECT 1 FROM core.cash_drawer_closings c WHERE c.opening_count_id=o.id)
      ORDER BY o.completed_at DESC,o.id DESC LIMIT 1`, [branch.id, drawerNumber, responsibleEmployeePinId, businessDate]);
    const row = opening.rows[0];
    return res.json({
      employee: { id: responsibleEmployeePinId, name: employeeById.get(responsibleEmployeePinId).display_name },
      closedBy: { id: closedByPinId, name: employeeById.get(closedByPinId).display_name },
      contextProofToken: signClosingContext(closedByPinId, responsibleEmployeePinId, branch.id, drawerNumber),
      opening: row ? {
        id: Number(row.id),
        businessDate: row.business_date,
        drawerNumber: Number(row.drawer_number),
        openingBalanceCents: Number(row.opening_balance_cents),
        completedAt: row.completed_at,
      } : null,
    });
  } catch (error) {
    console.error('POST /api/tageskasse/closing-context Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Die passende Einzählung konnte nicht geprüft werden.' });
  }
});

router.post('/closings', async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const branch = await resolveBranch(client, req);
    const businessDate = resolveCashBusinessDate(req);
    await ensureBusinessDayOpen(client, branch.id, businessDate);
    const closedByPinId = verifyPinProof(req.body?.proofToken, branch.id);
    const responsibleEmployeePinId = req.body?.responsibleEmployeeId == null ? closedByPinId : Number(req.body.responsibleEmployeeId);
    if (!Number.isInteger(responsibleEmployeePinId) || responsibleEmployeePinId <= 0) throw Object.assign(new Error('Bitte wähle den verantwortlichen Kassierer aus.'), { statusCode:400 });
    const employees = await client.query(`SELECT id,display_name FROM core.filiale_pins
      WHERE id=ANY($1::bigint[]) AND is_active=true`, [[closedByPinId,responsibleEmployeePinId]]);
    const employeeById = new Map(employees.rows.map((row)=>[Number(row.id),row]));
    if (!employeeById.has(closedByPinId) || !employeeById.has(responsibleEmployeePinId)) throw Object.assign(new Error('Mindestens eine beteiligte Person ist nicht mehr aktiv.'), { statusCode:409 });

    let checked;
    try {
      checked = validateCashClosing(req.body);
    } catch (validationError) {
      throw Object.assign(validationError, { statusCode: 400 });
    }
    verifyClosingContext(req.body?.contextProofToken, {
      branchId: branch.id,
      drawerNumber: checked.drawerNumber,
      closedByPinId,
      responsibleEmployeePinId,
    });
    let opening = null;
    if (req.body?.openingCountId != null) {
      const openingId = Number(req.body.openingCountId);
      if (!Number.isInteger(openingId) || openingId <= 0) throw Object.assign(new Error('Die zugeordnete Einzählung ist ungültig.'), { statusCode: 400 });
      const result = await client.query(`SELECT o.id,o.opening_balance_cents
        FROM core.cash_drawer_opening_counts o
        WHERE o.id=$1 AND o.branch_id=$2 AND o.drawer_number=$3 AND o.employee_pin_id=$4
          AND o.business_date=$5
          AND o.status='completed'
          AND o.lifecycle_status='active'
          AND NOT EXISTS (SELECT 1 FROM core.cash_drawer_closings c WHERE c.opening_count_id=o.id)
        FOR UPDATE`, [openingId, branch.id, checked.drawerNumber, responsibleEmployeePinId, businessDate]);
      opening = result.rows[0];
      if (!opening) throw Object.assign(new Error('Die passende Einzählung ist nicht mehr verfügbar. Bitte den Vorgang neu beginnen.'), { statusCode: 409 });
    } else if (req.body?.proceededWithoutOpening !== true) {
      throw Object.assign(new Error('Die fehlende Einzählung muss ausdrücklich bestätigt werden.'), { statusCode: 400 });
    }

    const result = await client.query(`INSERT INTO core.cash_drawer_closings
      (branch_id,business_date,drawer_number,employee_pin_id,closed_by_pin_id,opening_count_id,opening_balance_cents,
       opening_present,proceeded_without_opening,counted_total_cents,full_counts,retained_coin_cents,
       loose_change_cents,retained_note_counts,bank_note_counts,bank_amount_cents,eurocontrol_total_cents,
       difference_cents,notification_required,notification_confirmed,recount_attempts,resolved,status,created_by_user_id)
      VALUES ($1,$23,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,
        $13::jsonb,$14::jsonb,$15,$16,$17,$18,$19,$20::jsonb,$21,'closed',$22)
      RETURNING id,business_date,closed_at`, [
      branch.id, checked.drawerNumber, responsibleEmployeePinId, closedByPinId, opening?.id || null,
      opening ? Number(opening.opening_balance_cents) : 25000, Boolean(opening), !opening,
      checked.countedTotalCents, JSON.stringify(checked.fullCounts), checked.retainedCoinCents,
      checked.looseChangeCents, JSON.stringify(checked.retainedNoteCounts), JSON.stringify(checked.bankNoteCounts),
      checked.bankAmountCents, checked.bankAmountCents, checked.differenceCents,
      checked.notificationRequired, checked.notificationRequired ? req.body.notificationConfirmed === true : false,
      JSON.stringify(checked.recountAttempts), checked.differenceCents === 0, req.user.id, businessDate,
    ]);
    if (opening) {
      await client.query(`UPDATE core.cash_drawer_opening_counts
        SET lifecycle_status='closed',ended_at=$1
        WHERE id=$2 AND lifecycle_status='active'`, [result.rows[0].closed_at,opening.id]);
    }
    await client.query('COMMIT');
    return res.status(201).json({ closing: {
      id: Number(result.rows[0].id), businessDate: result.rows[0].business_date,
      closedAt: result.rows[0].closed_at, branch: branch.name, drawerNumber: checked.drawerNumber,
      employee: { id: responsibleEmployeePinId, name: employeeById.get(responsibleEmployeePinId).display_name },
      closedBy: { id: closedByPinId, name: employeeById.get(closedByPinId).display_name },
      bankAmountCents: checked.bankAmountCents, differenceCents: checked.differenceCents,
      openingPresent: Boolean(opening), status: 'closed',
    } });
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('POST /api/tageskasse/closings Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Die Kassenabrechnung konnte nicht gespeichert werden.' });
  } finally {
    client.release();
  }
});

router.post('/closings/:id/corrections', async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    if (!canCorrectCashClosing(req.user?.role)) throw Object.assign(new Error('Nur Filialleitung und freigegebene Führungsrollen dürfen Korrekturen anlegen.'), { statusCode:403 });
    const branch = await resolveBranch(client, req);
    const sourceId = Number(req.params.id);
    if (!Number.isInteger(sourceId) || sourceId <= 0) throw Object.assign(new Error('Die zu korrigierende Abrechnung ist ungültig.'), { statusCode:400 });
    const closedByPinId = verifyPinProof(req.body?.proofToken, branch.id);
    const closer = await client.query(`SELECT id,display_name FROM core.filiale_pins WHERE id=$1 AND is_active=true LIMIT 1`, [closedByPinId]);
    if (!closer.rows[0]) throw Object.assign(new Error('Der bestätigte Mitarbeiter ist nicht mehr aktiv.'), { statusCode:409 });
    let checked;
    try { checked = validateCashCorrection(req.body); }
    catch (validationError) { throw Object.assign(validationError, { statusCode:400 }); }

    const sourceResult = await client.query(`SELECT c.*,
        EXISTS (SELECT 1 FROM core.cash_drawer_closings newer WHERE newer.corrects_closing_id=c.id) AS superseded
      FROM core.cash_drawer_closings c WHERE c.id=$1 AND c.branch_id=$2 FOR SHARE`, [sourceId,branch.id]);
    const source = sourceResult.rows[0];
    if (!source) throw Object.assign(new Error('Die zu korrigierende Abrechnung wurde nicht gefunden.'), { statusCode:404 });
    if (source.superseded) throw Object.assign(new Error('Diese Abrechnung wurde bereits korrigiert. Bitte öffne die neueste Revision.'), { statusCode:409 });
    const requestedDate = resolveCashBusinessDate(req);
    if (String(source.business_date).slice(0,10) !== requestedDate) {
      throw Object.assign(new Error('Die Korrektur gehört nicht zum ausgewählten Geschäftstag.'), { statusCode:409 });
    }
    await ensureBusinessDayOpen(client, branch.id, source.business_date);

    const result = await client.query(`INSERT INTO core.cash_drawer_closings
      (branch_id,business_date,drawer_number,employee_pin_id,closed_by_pin_id,opening_count_id,opening_balance_cents,
       opening_present,proceeded_without_opening,counted_total_cents,full_counts,retained_coin_cents,loose_change_cents,
       retained_note_counts,bank_note_counts,bank_amount_cents,eurocontrol_total_cents,difference_cents,
       notification_required,notification_confirmed,recount_attempts,resolved,status,created_by_user_id,
       record_kind,corrects_closing_id,correction_reason,revision_number)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14::jsonb,$15::jsonb,$16,$17,$18,$19,$20,
        $21::jsonb,$22,'closed',$23,'correction',$24,$25,$26)
      RETURNING id,business_date,closed_at`, [
      source.branch_id,source.business_date,source.drawer_number,source.employee_pin_id,closedByPinId,
      source.opening_count_id,source.opening_balance_cents,source.opening_present,source.proceeded_without_opening,
      source.counted_total_cents,JSON.stringify(source.full_counts || {}),source.retained_coin_cents,source.loose_change_cents,
      JSON.stringify(source.retained_note_counts || {}),JSON.stringify(source.bank_note_counts || {}),source.bank_amount_cents,
      source.eurocontrol_total_cents,checked.differenceCents,checked.notificationRequired,
      checked.notificationRequired ? req.body.notificationConfirmed === true : false,
      JSON.stringify(source.recount_attempts || []),checked.differenceCents===0,req.user.id,source.id,checked.reason,
      Number(source.revision_number || 1)+1,
    ]);
    await client.query('COMMIT');
    return res.status(201).json({ correction:{
      id:Number(result.rows[0].id), businessDate:result.rows[0].business_date, closedAt:result.rows[0].closed_at,
      correctsClosingId:sourceId, revisionNumber:Number(source.revision_number || 1)+1,
      differenceCents:checked.differenceCents, correctionReason:checked.reason,
      closedBy:{ id:closedByPinId, name:closer.rows[0].display_name }, status:'closed',
    } });
  } catch (error) {
    await client.query('ROLLBACK').catch(()=>{});
    console.error('POST /api/tageskasse/closings/:id/corrections Fehler:', error);
    return res.status(error.statusCode || 500).json({ message:error.statusCode ? error.message : 'Die Korrektur konnte nicht gespeichert werden.' });
  } finally { client.release(); }
});

module.exports = router;
