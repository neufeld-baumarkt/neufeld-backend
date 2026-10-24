const express = require('express');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const { findActivePinMatches } = require('../lib/employeePinIdentity');
const { validateOpeningCount } = require('../lib/cashOpeningPolicy');
const { validateCashClosing } = require('../lib/cashClosingPolicy');
const { canUseCashModule, isCentralRole, resolveCashBranch } = require('../lib/cashModuleAccess');

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

async function loadImmediatePreviousClosing(client, branchId, drawerNumber) {
  const result = await client.query(`SELECT id,branch_id,drawer_number,difference_cents,resolved,status,closed_at
    FROM core.cash_drawer_closings
    WHERE branch_id=$1 AND drawer_number=$2 AND status='closed'
    ORDER BY closed_at DESC,id DESC LIMIT 1`, [branchId, drawerNumber]);
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

function signClosingContext(employeePinId, branchId, drawerNumber) {
  return jwt.sign({
    type: 'cash-closing-context',
    pinId: Number(employeePinId),
    branchId: Number(branchId),
    drawerNumber: Number(drawerNumber),
  }, process.env.JWT_SECRET, { expiresIn: PIN_PROOF_TTL_SECONDS });
}

function verifyClosingContext(token, { branchId, drawerNumber, employeePinId }) {
  try {
    const proof = jwt.verify(String(token || ''), process.env.JWT_SECRET);
    if (proof.type !== 'cash-closing-context'
      || Number(proof.branchId) !== Number(branchId)
      || Number(proof.drawerNumber) !== Number(drawerNumber)
      || Number(proof.pinId) !== Number(employeePinId)) throw new Error('invalid closing context');
  } catch {
    throw Object.assign(new Error('Der Abrechnungskontext ist abgelaufen. Bitte die Abrechnung neu beginnen.'), { statusCode: 401 });
  }
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
    const closing = await loadImmediatePreviousClosing(pool, branch.id, drawerNumber);
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

router.post('/opening-counts', async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const branch = await resolveBranch(client, req);
    const drawerNumber = Number(req.body?.drawerNumber);
    if (![1, 2].includes(drawerNumber)) throw Object.assign(new Error('Bitte Lade 1 oder Lade 2 auswählen.'), { statusCode: 400 });
    const previousClosing = await loadImmediatePreviousClosing(client, branch.id, drawerNumber);

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
    const result = await client.query(`INSERT INTO core.cash_drawer_opening_counts
      (branch_id,drawer_number,employee_pin_id,final_counter_pin_id,target_cents,initial_counted_cents,
       initial_difference_cents,opening_balance_cents,opening_difference_cents,previous_closing_id,
       carryover_decision,attempts,created_by_user_id)
      VALUES ($1,$2,$3,$4,25000,$5,$6,$7,$8,$9,$10,$11::jsonb,$12)
      RETURNING id,business_date,completed_at`, [
      branch.id, checked.drawerNumber, firstEmployee.employeePinId, finalEmployee.employeePinId,
      checked.initialCountedCents, checked.initialDifferenceCents, checked.openingBalanceCents,
      checked.openingDifferenceCents, checked.previousClosingId, checked.carryoverDecision,
      JSON.stringify(checked.attempts), req.user.id,
    ]);
    await client.query('COMMIT');
    return res.status(201).json({ openingCount: {
      id: Number(result.rows[0].id), businessDate: result.rows[0].business_date,
      completedAt: result.rows[0].completed_at, branch: branch.name, drawerNumber: checked.drawerNumber,
      employee: { id: firstEmployee.employeePinId, name: firstEmployee.employeeName },
      openingBalanceCents: checked.openingBalanceCents, openingDifferenceCents: checked.openingDifferenceCents,
      carryoverDecision: checked.carryoverDecision, previousClosingId: checked.previousClosingId,
    } });
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('POST /api/tageskasse/opening-counts Fehler:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Die Einzählung konnte nicht gespeichert werden.' });
  } finally {
    client.release();
  }
});

router.post('/closing-context', async (req, res) => {
  try {
    const branch = await resolveBranch(pool, req);
    const drawerNumber = Number(req.body?.drawerNumber);
    if (![1, 2].includes(drawerNumber)) return res.status(400).json({ message: 'Bitte Lade 1 oder Lade 2 auswählen.' });
    const employeePinId = verifyPinProof(req.body?.proofToken, branch.id);
    const employee = await pool.query(`SELECT id,display_name FROM core.filiale_pins
      WHERE id=$1 AND is_active=true LIMIT 1`, [employeePinId]);
    if (!employee.rows[0]) return res.status(409).json({ message: 'Der bestätigte Mitarbeiter ist nicht mehr aktiv.' });
    const opening = await pool.query(`SELECT o.id,o.business_date,o.drawer_number,o.opening_balance_cents,o.completed_at
      FROM core.cash_drawer_opening_counts o
      WHERE o.branch_id=$1 AND o.drawer_number=$2 AND o.employee_pin_id=$3
        AND o.business_date=((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date)
        AND o.status='completed'
        AND NOT EXISTS (SELECT 1 FROM core.cash_drawer_closings c WHERE c.opening_count_id=o.id)
      ORDER BY o.completed_at DESC,o.id DESC LIMIT 1`, [branch.id, drawerNumber, employeePinId]);
    const row = opening.rows[0];
    return res.json({
      employee: { id: Number(employee.rows[0].id), name: employee.rows[0].display_name },
      contextProofToken: signClosingContext(employeePinId, branch.id, drawerNumber),
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
    const employeePinId = verifyPinProof(req.body?.proofToken, branch.id);
    const employee = await client.query(`SELECT id,display_name FROM core.filiale_pins
      WHERE id=$1 AND is_active=true LIMIT 1`, [employeePinId]);
    if (!employee.rows[0]) throw Object.assign(new Error('Der bestätigte Mitarbeiter ist nicht mehr aktiv.'), { statusCode: 409 });

    let checked;
    try {
      checked = validateCashClosing(req.body);
    } catch (validationError) {
      throw Object.assign(validationError, { statusCode: 400 });
    }
    verifyClosingContext(req.body?.contextProofToken, {
      branchId: branch.id,
      drawerNumber: checked.drawerNumber,
      employeePinId,
    });
    let opening = null;
    if (req.body?.openingCountId != null) {
      const openingId = Number(req.body.openingCountId);
      if (!Number.isInteger(openingId) || openingId <= 0) throw Object.assign(new Error('Die zugeordnete Einzählung ist ungültig.'), { statusCode: 400 });
      const result = await client.query(`SELECT o.id,o.opening_balance_cents
        FROM core.cash_drawer_opening_counts o
        WHERE o.id=$1 AND o.branch_id=$2 AND o.drawer_number=$3 AND o.employee_pin_id=$4
          AND o.business_date=((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date)
          AND o.status='completed'
          AND NOT EXISTS (SELECT 1 FROM core.cash_drawer_closings c WHERE c.opening_count_id=o.id)
        FOR UPDATE`, [openingId, branch.id, checked.drawerNumber, employeePinId]);
      opening = result.rows[0];
      if (!opening) throw Object.assign(new Error('Die passende Einzählung ist nicht mehr verfügbar. Bitte den Vorgang neu beginnen.'), { statusCode: 409 });
    } else if (req.body?.proceededWithoutOpening !== true) {
      throw Object.assign(new Error('Die fehlende Einzählung muss ausdrücklich bestätigt werden.'), { statusCode: 400 });
    }

    const result = await client.query(`INSERT INTO core.cash_drawer_closings
      (branch_id,business_date,drawer_number,employee_pin_id,opening_count_id,opening_balance_cents,
       opening_present,proceeded_without_opening,counted_total_cents,full_counts,retained_coin_cents,
       loose_change_cents,retained_note_counts,bank_note_counts,bank_amount_cents,eurocontrol_total_cents,
       difference_cents,notification_required,notification_confirmed,recount_attempts,resolved,status,created_by_user_id)
      VALUES ($1,((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin')::date),$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,
        $12::jsonb,$13::jsonb,$14,$15,$16,$17,$18,$19::jsonb,$20,'closed',$21)
      RETURNING id,business_date,closed_at`, [
      branch.id, checked.drawerNumber, employeePinId, opening?.id || null,
      opening ? Number(opening.opening_balance_cents) : 25000, Boolean(opening), !opening,
      checked.countedTotalCents, JSON.stringify(checked.fullCounts), checked.retainedCoinCents,
      checked.looseChangeCents, JSON.stringify(checked.retainedNoteCounts), JSON.stringify(checked.bankNoteCounts),
      checked.bankAmountCents, checked.bankAmountCents, checked.differenceCents,
      checked.notificationRequired, checked.notificationRequired ? req.body.notificationConfirmed === true : false,
      JSON.stringify(checked.recountAttempts), checked.differenceCents === 0, req.user.id,
    ]);
    await client.query('COMMIT');
    return res.status(201).json({ closing: {
      id: Number(result.rows[0].id), businessDate: result.rows[0].business_date,
      closedAt: result.rows[0].closed_at, branch: branch.name, drawerNumber: checked.drawerNumber,
      employee: { id: employeePinId, name: employee.rows[0].display_name },
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

module.exports = router;
