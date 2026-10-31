const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const { assertValidPassword, PASSWORD_POLICY } = require('./passwordPolicy');
const { TOKEN_MODE_DUAL_LEGACY, integrationMetadata } = require('./productionAuthIntegration');

const JWT_EXPIRES_IN = '8h';
const DUMMY_PASSWORD_HASH = '$2b$12$NFrQCbmTD7vLUWOPczvBIOr2uxr9DT4Bvr83T/i1vuR04ODwfWAN2';
const GENERIC_LOGIN_ERROR = 'Anmeldung fehlgeschlagen. Bitte Zugangsdaten prüfen.';

function setNoStore(res) {
  res.setHeader('Cache-Control', 'no-store');
}

function clientIp(req) {
  return String(req.ip || req.socket?.remoteAddress || 'unknown').slice(0, 100);
}

function createRateLimiter({ limit, windowMs, now = Date.now, maxEntries = 10_000 }) {
  const entries = new Map();
  return {
    consume(key) {
      const current = now();
      const previous = entries.get(key);
      if (!previous && entries.size >= maxEntries) {
        for (const [candidate, value] of entries) {
          if (value.resetAt <= current) entries.delete(candidate);
        }
        if (entries.size >= maxEntries) entries.delete(entries.keys().next().value);
      }
      const entry = !previous || previous.resetAt <= current
        ? { count: 0, resetAt: current + windowMs }
        : previous;
      entry.count += 1;
      entries.set(key, entry);
      return entry.count <= limit;
    },
    reset(key) { entries.delete(key); },
  };
}

function publicSessionUser(row) {
  return {
    id: Number(row.id), name: row.name, role: row.role, filiale: row.filiale || null,
    active: row.active === true, forcePasswordChange: row.force_password_change === true,
  };
}

function signUserToken(row, secret, sign = jwt.sign, options = {}) {
  const sessionEpoch = Number(row.session_epoch ?? row.sessionEpoch ?? 0);
  const payload = {
    id: Number(row.id), tokenVersion: Number(row.token_version),
    sessionEpoch: Number.isInteger(sessionEpoch) && sessionEpoch >= 0 ? sessionEpoch : 0,
  };
  if (options.tokenMode === TOKEN_MODE_DUAL_LEGACY) {
    Object.assign(payload, { name: row.name, role: row.role, filiale: row.filiale || null });
  }
  return sign(payload, secret, { algorithm: 'HS256', expiresIn: JWT_EXPIRES_IN });
}

async function recordSecurityAudit(queryable, event) {
  const details = event.details && typeof event.details === 'object' ? event.details : {};
  await queryable.query(
    `INSERT INTO public.user_security_audit
      (actor_user_id, actor_name, target_user_id, target_name, action, details)
     VALUES ($1, $2, $3, $4, $5, $6::jsonb)`,
    [event.actorUserId || null, event.actorName || null, event.targetUserId || null,
      event.targetName || null, event.action, JSON.stringify(details)]
  );
}

async function withTransaction(pool, work) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch { /* preserve original error */ }
    throw error;
  } finally {
    client.release();
  }
}

function createAuthHandlers(options) {
  const pool = options.pool;
  const jwtSecret = options.jwtSecret;
  const comparePassword = options.comparePassword || bcrypt.compare;
  const hashPassword = options.hashPassword || ((value) => bcrypt.hash(value, 12));
  const assertPassword = options.assertPassword || assertValidPassword;
  const integration = integrationMetadata(options.integration);
  const signToken = options.signToken || ((row) => signUserToken(row, jwtSecret, jwt.sign, integration));
  const loginLimiter = options.loginLimiter || createRateLimiter({ limit: 5, windowMs: 15 * 60_000 });
  const passwordLimiter = options.passwordLimiter || createRateLimiter({ limit: 5, windowMs: 15 * 60_000 });
  if (!pool || !jwtSecret) throw new TypeError('Auth handlers require pool and jwtSecret.');

  return {
    login: async (req, res) => {
      setNoStore(res);
      const name = typeof req.body?.name === 'string' ? req.body.name.trim() : '';
      const password = typeof req.body?.password === 'string' ? req.body.password : '';
      if (!name || !password || name.length > 128 || password.length > 128
          || Buffer.byteLength(password, 'utf8') > PASSWORD_POLICY.maxUtf8Bytes) {
        return res.status(400).json({ message: 'Name und Passwort erforderlich.' });
      }
      const rateKey = `${clientIp(req)}:${name.toLowerCase()}`;
      if (!loginLimiter.consume(rateKey)) {
        return res.status(429).json({ message: 'Zu viele Anmeldeversuche. Bitte später erneut versuchen.' });
      }
      try {
        const result = await pool.query(
          `SELECT u.id,u.name,u.password,u.role,u.filiale,u.force_password_change,u.active,u.token_version,
             c.mode AS runtime_mode,c.session_epoch
           FROM public.users u CROSS JOIN core.app_runtime_control c
           WHERE u.name=$1 AND c.control_key='global' LIMIT 1`, [name]
        );
        const user = result.rows[0] || null;
        const passwordMatches = await comparePassword(password, user?.password || DUMMY_PASSWORD_HASH);
        if (!user || !passwordMatches || user.active !== true) {
          return res.status(401).json({ message: GENERIC_LOGIN_ERROR });
        }
        if (user.runtime_mode === 'maintenance' && user.role !== 'Admin') {
          return res.status(503).json({ code: 'MAINTENANCE_ACTIVE', message: 'Die Anwendung befindet sich vorübergehend im Wartungsmodus.' });
        }
        loginLimiter.reset(rateKey);
        const publicUser = publicSessionUser(user);
        return res.json({ token: signToken(user), ...publicUser, auth: integration });
      } catch (error) {
        console.error('Login-Fehler:', error?.code || error?.name || 'unknown');
        return res.status(500).json({ message: 'Serverfehler' });
      }
    },

    session: async (req, res) => {
      setNoStore(res);
      return res.json({ user: publicSessionUser(req.user), auth: integration });
    },

    changePassword: async (req, res) => {
      setNoStore(res);
      const oldPassword = typeof req.body?.oldPassword === 'string' ? req.body.oldPassword : '';
      const newPassword = typeof req.body?.newPassword === 'string' ? req.body.newPassword : '';
      const rateKey = `${clientIp(req)}:${req.user.id}`;
      if (!passwordLimiter.consume(rateKey)) {
        return res.status(429).json({ message: 'Zu viele Versuche. Bitte später erneut versuchen.' });
      }
      try {
        assertPassword(newPassword);
        if (!oldPassword) throw Object.assign(new Error('Das aktuelle Passwort ist erforderlich.'), { statusCode: 400 });
        if (Buffer.byteLength(oldPassword, 'utf8') > PASSWORD_POLICY.maxUtf8Bytes) {
          throw Object.assign(new Error('Das aktuelle Passwort ist ungültig.'), { statusCode: 400 });
        }
        if (oldPassword === newPassword) throw Object.assign(new Error('Das neue Passwort muss sich vom aktuellen unterscheiden.'), { statusCode: 400 });
        const changed = await withTransaction(pool, async (client) => {
          const result = await client.query(
            `SELECT id, name, password, role, filiale, force_password_change, active, token_version
             FROM public.users WHERE id = $1 FOR UPDATE`, [req.user.id]
          );
          const user = result.rows[0];
          if (!user || user.active !== true) throw Object.assign(new Error('Sitzung ungültig.'), { statusCode: 401 });
          if (!(await comparePassword(oldPassword, user.password))) {
            throw Object.assign(new Error('Das aktuelle Passwort ist falsch.'), { statusCode: 401 });
          }
          const update = await client.query(
            `UPDATE public.users SET password=$1, force_password_change=false, token_version=token_version+1
             WHERE id=$2 RETURNING id, name, role, filiale, force_password_change, active, token_version`,
            [await hashPassword(newPassword), user.id]
          );
          const updated = update.rows[0];
          await recordSecurityAudit(client, {
            actorUserId: user.id, actorName: user.name, targetUserId: user.id,
            targetName: user.name, action: 'password.changed', details: { selfService: true },
          });
          updated.session_epoch = req.user.session_epoch;
          return { updated, token: signToken(updated) };
        });
        passwordLimiter.reset(rateKey);
        return res.json({ ok: true, token: changed.token, user: publicSessionUser(changed.updated), auth: integration });
      } catch (error) {
        const status = Number(error?.statusCode) || 500;
        if (status >= 500) console.error('Change-Password-Fehler:', error?.code || error?.name || 'unknown');
        return res.status(status).json({ message: status >= 500 ? 'Serverfehler' : error.message });
      }
    },
  };
}

module.exports = {
  DUMMY_PASSWORD_HASH, GENERIC_LOGIN_ERROR, JWT_EXPIRES_IN, createAuthHandlers,
  createRateLimiter, publicSessionUser, recordSecurityAudit, signUserToken,
};
