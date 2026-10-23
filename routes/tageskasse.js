const express = require('express');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const { validateOpeningCount } = require('../lib/cashOpeningPolicy');

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
  const branchName = String(req.user?.filiale || '').trim();
  if (!branchName || branchName.toLowerCase() === 'alle') {
    throw Object.assign(new Error('Deinem Benutzerkonto ist keine einzelne Filiale zugeordnet.'), { statusCode: 403 });
  }
  const result = await client.query('SELECT id, name FROM public.filialen WHERE name=$1 AND aktiv=true LIMIT 1', [branchName]);
  if (!result.rowCount) throw Object.assign(new Error('Die zugeordnete Filiale ist nicht aktiv oder wurde nicht gefunden.'), { statusCode: 403 });
  return result.rows[0];
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

router.post('/pin/verify', async (req, res) => {
  try {
    if (!consumePinAttempt(req)) return res.status(429).json({ message: 'Zu viele PIN-Versuche. Bitte 15 Minuten warten.' });
    const pin = String(req.body?.pin || '').trim();
    if (!/^\d{4}$/.test(pin)) return res.status(400).json({ message: 'Der Mitarbeiter-PIN muss genau vier Ziffern haben.' });

    const branch = await resolveBranch(pool, req);
    const excludedId = req.body?.excludeEmployeeId == null ? null : Number(req.body.excludeEmployeeId);
    const candidates = await pool.query(`SELECT id,display_name,pin_hash,locked_until
      FROM core.filiale_pins WHERE filiale_id=$1 AND is_active=true ORDER BY id`, [branch.id]);

    let matched = null;
    for (const candidate of candidates.rows) {
      if (candidate.locked_until && new Date(candidate.locked_until) > new Date()) continue;
      if (await bcrypt.compare(pin, candidate.pin_hash)) { matched = candidate; break; }
    }
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
    const pins = proofIds.length ? await client.query('SELECT id,display_name FROM core.filiale_pins WHERE filiale_id=$1 AND is_active=true AND id=ANY($2::bigint[])', [branch.id, proofIds]) : { rows: [] };
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

module.exports = router;
