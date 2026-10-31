const express = require('express');
const bcrypt = require('bcrypt');

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const { findActivePinMatches } = require('../lib/employeePinIdentity');
const { visibleCashEmployees } = require('../lib/cashEmployeeVisibility');
const { createEmployeePinProof } = require('../lib/employeePinProof');
const {
  DEFAULT_EMPLOYEE_PIN,
  assignmentBranchId,
  employeeName,
  fourDigitPin,
  personalPin,
} = require('../lib/employeePinPolicy');

const router = express.Router();
const activationAttempts = new Map();
const verificationAttempts = new Map();

router.use((_req, res, next) => {
  res.setHeader('Cache-Control', 'no-store');
  next();
});

function requireAdmin(req, res) {
  if (req.user?.role !== 'Admin') {
    res.status(403).json({ message: 'Die Mitarbeiter-PIN-Verwaltung ist nur für Admins freigegeben.' });
    return false;
  }
  return true;
}

function positiveEmployeeId(value) {
  const id = Number(value);
  if (!Number.isInteger(id) || id <= 0) {
    throw Object.assign(new Error('Ungültige Mitarbeiter-ID.'), { statusCode: 400 });
  }
  return id;
}

function sendError(res, error) {
  if (error?.code === '23505') {
    return res.status(409).json({ message: 'Dieser Mitarbeiter ist in der gewählten Filiale bereits angelegt.' });
  }
  const status = Number(error?.statusCode) || 500;
  if (status >= 500) console.error('Mitarbeiter-PIN-Verwaltung fehlgeschlagen:', error?.code || error?.message);
  return res.status(status).json({ message: status >= 500 ? 'Mitarbeiter-PIN-Verwaltung fehlgeschlagen.' : error.message });
}

async function assertAssignmentBranch(queryable, id) {
  if (id == null) return { id: null, name: 'Alle' };
  const result = await queryable.query('SELECT id,name FROM public.filialen WHERE id=$1 AND aktiv=true LIMIT 1', [id]);
  if (!result.rows[0]) throw Object.assign(new Error('Die gewählte Filiale ist nicht aktiv.'), { statusCode: 400 });
  return result.rows[0];
}

function publicEmployee(row) {
  return {
    id: Number(row.id),
    filiale_id: row.filiale_id == null ? null : Number(row.filiale_id),
    filiale: row.filiale || 'Alle',
    first_name: row.first_name || '',
    last_name: row.last_name || '',
    display_name: row.display_name,
    is_active: row.is_active === true,
    must_change_pin: row.must_change_pin === true,
    last_used_at: row.last_used_at || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

function activationRateKey(req, employeeId) {
  return `${req.user?.id || 'unknown'}:${req.ip || 'unknown'}:${employeeId}`;
}

function consumeActivationAttempt(req, employeeId) {
  const key = activationRateKey(req, employeeId);
  const now = Date.now();
  const current = activationAttempts.get(key);
  const entry = !current || current.resetAt <= now ? { count: 0, resetAt: now + 15 * 60_000 } : current;
  entry.count += 1;
  activationAttempts.set(key, entry);
  return entry.count <= 10;
}

async function pinAlreadyUsed(queryable, employeeId, pin) {
  const candidates = await queryable.query(
    'SELECT id,pin_hash FROM core.filiale_pins WHERE is_active=true AND id<>$1',
    [employeeId]
  );
  for (const candidate of candidates.rows) {
    if (await bcrypt.compare(pin, candidate.pin_hash)) return true;
  }
  return false;
}

function consumeVerificationAttempt(req) {
  const key = `${req.user?.id || 'unknown'}:${req.ip || 'unknown'}`;
  const now = Date.now();
  const current = verificationAttempts.get(key);
  const entry = !current || current.resetAt <= now ? { count: 0, resetAt: now + 15 * 60_000 } : current;
  entry.count += 1;
  verificationAttempts.set(key, entry);
  return { allowed: entry.count <= 10, key };
}

router.get('/', verifyToken(), async (req, res) => {
  if (!requireAdmin(req, res)) return;
  try {
    const [employees, branches] = await Promise.all([
      pool.query(`SELECT p.id,p.filiale_id,f.name AS filiale,p.first_name,p.last_name,p.display_name,
          p.is_active,p.must_change_pin,p.last_used_at,p.created_at,p.updated_at
        FROM core.filiale_pins p
        LEFT JOIN public.filialen f ON f.id=p.filiale_id
        ORDER BY COALESCE(f.name,'Alle'),p.last_name NULLS LAST,p.first_name NULLS LAST,p.display_name`),
      pool.query('SELECT id,name FROM public.filialen WHERE aktiv=true ORDER BY name'),
    ]);
    return res.json({
      employees: employees.rows.map(publicEmployee),
      filialen: [
        { id: 'all', name: 'Alle' },
        ...branches.rows.map((row) => ({ id: Number(row.id), name: row.name })),
      ],
      default_pin: DEFAULT_EMPLOYEE_PIN,
    });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post('/', verifyToken(), async (req, res) => {
  if (!requireAdmin(req, res)) return;
  try {
    const filialeId = assignmentBranchId(req.body?.filiale_id);
    const name = employeeName(req.body);
    await assertAssignmentBranch(pool, filialeId);
    const pinHash = await bcrypt.hash(DEFAULT_EMPLOYEE_PIN, 10);
    const result = await pool.query(`INSERT INTO core.filiale_pins
      (filiale_id,first_name,last_name,display_name,pin_hash,is_active,must_change_pin,
       failed_attempts,locked_until,last_failed_at,last_used_at,updated_at)
      VALUES ($1,$2,$3,$4,$5,true,true,0,NULL,NULL,NULL,NOW())
      RETURNING id,filiale_id,first_name,last_name,display_name,is_active,must_change_pin,
        last_used_at,created_at,updated_at`,
    [filialeId, name.firstName, name.lastName, name.displayName, pinHash]);
    return res.status(201).json({
      message: 'Mitarbeiter angelegt. Beim ersten Gebrauch muss 0000 durch einen persönlichen PIN ersetzt werden.',
      employee: publicEmployee(result.rows[0]),
    });
  } catch (error) {
    return sendError(res, error);
  }
});

router.patch('/:id', verifyToken(), async (req, res) => {
  if (!requireAdmin(req, res)) return;
  try {
    const id = positiveEmployeeId(req.params.id);
    const filialeId = assignmentBranchId(req.body?.filiale_id);
    const name = employeeName(req.body);
    await assertAssignmentBranch(pool, filialeId);
    const result = await pool.query(`UPDATE core.filiale_pins
      SET filiale_id=$1,first_name=$2,last_name=$3,display_name=$4,updated_at=NOW()
      WHERE id=$5
      RETURNING id,filiale_id,first_name,last_name,display_name,is_active,must_change_pin,
        last_used_at,created_at,updated_at`,
    [filialeId, name.firstName, name.lastName, name.displayName, id]);
    if (!result.rows[0]) throw Object.assign(new Error('Mitarbeiter wurde nicht gefunden.'), { statusCode: 404 });
    return res.json({ message: 'Mitarbeiter gespeichert.', employee: publicEmployee(result.rows[0]) });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post('/:id/reset-pin', verifyToken(), async (req, res) => {
  if (!requireAdmin(req, res)) return;
  try {
    const id = positiveEmployeeId(req.params.id);
    const pinHash = await bcrypt.hash(DEFAULT_EMPLOYEE_PIN, 10);
    const result = await pool.query(`UPDATE core.filiale_pins
      SET pin_hash=$1,must_change_pin=true,failed_attempts=0,locked_until=NULL,
          last_failed_at=NULL,last_used_at=NULL,updated_at=NOW()
      WHERE id=$2 RETURNING id,display_name`, [pinHash, id]);
    if (!result.rows[0]) throw Object.assign(new Error('Mitarbeiter wurde nicht gefunden.'), { statusCode: 404 });
    return res.json({ message: `${result.rows[0].display_name} verwendet beim nächsten Mal wieder den Standard-PIN 0000.` });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post('/verify', verifyToken(), async (req, res) => {
  const attempt = consumeVerificationAttempt(req);
  if (!attempt.allowed) return res.status(429).json({ message: 'Zu viele PIN-Versuche. Bitte 15 Minuten warten.' });
  try {
    const pin = fourDigitPin(req.body?.pin);
    if (pin === DEFAULT_EMPLOYEE_PIN) {
      return res.status(409).json({ code: 'PIN_CHANGE_REQUIRED', message: 'Bitte zuerst einen persönlichen PIN festlegen.' });
    }
    const excludedId = req.body?.excludeEmployeeId == null ? null : positiveEmployeeId(req.body.excludeEmployeeId);
    const candidates = await pool.query(`SELECT id,display_name,pin_hash,must_change_pin,locked_until
      FROM core.filiale_pins WHERE is_active=true ORDER BY id`);
    const matches = await findActivePinMatches(candidates.rows, pin, bcrypt.compare);
    if (matches.length > 1) {
      throw Object.assign(new Error('Dieser PIN ist mehreren Mitarbeitern zugeordnet. Bitte die betroffenen PINs in der Verwaltung zurücksetzen.'), { statusCode: 409, code: 'PIN_AMBIGUOUS' });
    }
    const matched = matches[0];
    if (!matched) throw Object.assign(new Error('PIN ungültig.'), { statusCode: 401 });
    if (excludedId && Number(matched.id) === excludedId) {
      throw Object.assign(new Error('Für diese Nachzählung muss eine andere Person ihren PIN eingeben.'), { statusCode: 400 });
    }
    verificationAttempts.delete(attempt.key);
    await pool.query('UPDATE core.filiale_pins SET last_used_at=NOW(),updated_at=NOW() WHERE id=$1', [matched.id]);
    const employee = { id: Number(matched.id), name: matched.display_name };
    return res.json({ employee: { ...employee, proofToken: createEmployeePinProof(employee) } });
  } catch (error) {
    return sendError(res, error);
  }
});

router.get('/first-use-candidates', verifyToken(), async (req, res) => {
  try {
    const result = await pool.query(`SELECT id,display_name FROM core.filiale_pins
      WHERE is_active=true AND must_change_pin=true
      ORDER BY display_name`);
    return res.json({
      filiale: 'Unternehmensweit',
      employees: result.rows.map((row) => ({ id: Number(row.id), display_name: row.display_name })),
    });
  } catch (error) {
    return sendError(res, error);
  }
});

router.get('/cash-candidates', verifyToken(), async (req, res) => {
  try {
    const branchName = String(req.query?.branch || '').trim();
    if (!branchName) throw Object.assign(new Error('Filiale fehlt.'), { statusCode: 400 });

    const [branchResult, employeeResult] = await Promise.all([
      pool.query('SELECT id,name FROM public.filialen WHERE aktiv=true AND LOWER(name)=LOWER($1) LIMIT 1', [branchName]),
      pool.query(`SELECT id,display_name,filiale_id FROM core.filiale_pins
        WHERE is_active=true AND must_change_pin=false
        ORDER BY LOWER(display_name),id`),
    ]);
    const branch = branchResult.rows[0];
    if (!branch) throw Object.assign(new Error('Filiale wurde nicht gefunden.'), { statusCode: 404 });

    return res.json({
      filiale: branch.name,
      employees: visibleCashEmployees(employeeResult.rows, branch.id)
        .map((row) => ({ id: Number(row.id), display_name: row.display_name })),
    });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post('/activate', verifyToken(), async (req, res) => {
  const employeeId = Number(req.body?.employee_id);
  if (!consumeActivationAttempt(req, employeeId)) {
    return res.status(429).json({ message: 'Zu viele PIN-Versuche. Bitte 15 Minuten warten.' });
  }
  const client = await pool.connect();
  try {
    const id = positiveEmployeeId(employeeId);
    const currentPin = fourDigitPin(req.body?.current_pin, 'Der Standard-PIN');
    const newPin = personalPin(req.body?.new_pin);
    const confirmation = fourDigitPin(req.body?.confirm_pin, 'Die PIN-Bestätigung');
    if (newPin !== confirmation) throw Object.assign(new Error('Die beiden persönlichen PIN-Eingaben stimmen nicht überein.'), { statusCode: 400 });

    await client.query('BEGIN');
    await client.query('LOCK TABLE core.filiale_pins IN SHARE ROW EXCLUSIVE MODE');
    const result = await client.query(`SELECT id,filiale_id,display_name,pin_hash,must_change_pin,is_active
      FROM core.filiale_pins WHERE id=$1 FOR UPDATE`, [id]);
    const employee = result.rows[0];
    if (!employee || employee.is_active !== true) throw Object.assign(new Error('Mitarbeiter wurde nicht gefunden.'), { statusCode: 404 });
    if (employee.must_change_pin !== true) throw Object.assign(new Error('Der persönliche PIN wurde bereits eingerichtet.'), { statusCode: 409 });
    if (!(await bcrypt.compare(currentPin, employee.pin_hash))) throw Object.assign(new Error('Der Standard-PIN ist nicht korrekt.'), { statusCode: 401 });
    if (await pinAlreadyUsed(client, id, newPin)) {
      throw Object.assign(new Error('Dieser PIN wird bereits von einem anderen Mitarbeiter verwendet. Bitte einen anderen wählen.'), { statusCode: 409 });
    }
    await client.query(`UPDATE core.filiale_pins SET pin_hash=$1,must_change_pin=false,
      failed_attempts=0,locked_until=NULL,last_failed_at=NULL,last_used_at=NOW(),updated_at=NOW()
      WHERE id=$2`, [await bcrypt.hash(newPin, 10), id]);
    await client.query('COMMIT');
    activationAttempts.delete(activationRateKey(req, id));
    return res.json({
      message: 'Persönlicher PIN wurde gespeichert.',
      employee: { id, name: employee.display_name },
    });
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch { /* original error wins */ }
    return sendError(res, error);
  } finally {
    client.release();
  }
});

module.exports = router;
