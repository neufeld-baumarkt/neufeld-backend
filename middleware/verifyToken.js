const jwt = require('jsonwebtoken');
const { FORCE_MODE_ENFORCE, MAX_LEGACY_WINDOW_MS, TOKEN_MODE_DUAL_LEGACY } = require('../lib/productionAuthIntegration');

let defaultOptions = null;

function createVerifyToken(options = {}) {
  const jwtSecret = options.jwtSecret || process.env.JWT_SECRET;
  const integration = options.integration || {};
  const tokenMode = integration.tokenMode || 'strict';
  const forceMode = integration.forcePasswordChangeMode || FORCE_MODE_ENFORCE;
  const now = options.now || Date.now;
  if (!jwtSecret) throw new Error('JWT_SECRET is not set in environment variables');

  return function verifyToken(requiredRoleOrOptions = null) {
    const routeOptions = typeof requiredRoleOrOptions === 'string'
      ? { requiredRole: requiredRoleOrOptions }
      : (requiredRoleOrOptions || {});
    return async (req, res, next) => {
      res.setHeader('Cache-Control', 'no-store');
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ message: 'Kein Zugriffstoken gefunden. Bitte erneut anmelden.' });
      }
      try {
        const decoded = jwt.verify(authHeader.slice(7), jwtSecret, { algorithms: ['HS256'] });
        const { id, tokenVersion, sessionEpoch } = decoded;
        const isVersioned = Number.isInteger(tokenVersion) && tokenVersion >= 0;
        const issuedAtMs = Number(decoded.iat) * 1000;
        const expiresAtMs = Number(decoded.exp) * 1000;
        const currentTime = now();
        const validLifetime = Number.isFinite(issuedAtMs) && Number.isFinite(expiresAtMs)
          && issuedAtMs <= currentTime + 60_000 && expiresAtMs > issuedAtMs
          && expiresAtMs - issuedAtMs <= MAX_LEGACY_WINDOW_MS;
        const legacy = tokenVersion === undefined && tokenMode === TOKEN_MODE_DUAL_LEGACY
          && validLifetime && issuedAtMs <= Number(integration.legacyAcceptUntil)
          && currentTime <= Number(integration.legacyAcceptUntil);
        if (!Number.isInteger(id) || id <= 0 || !validLifetime || (!isVersioned && !legacy)) {
          throw new Error('Invalid token claims');
        }
        const pool = options.pool || require('../db');
        const result = await pool.query(
          `SELECT u.id,u.name,u.role,u.filiale,u.email,u.force_password_change,u.active,u.token_version,
             c.mode AS runtime_mode,c.session_epoch
           FROM public.users u CROSS JOIN core.app_runtime_control c
           WHERE u.id=$1 AND c.control_key='global' LIMIT 1`, [id]
        );
        const current = result.rows[0];
        if (!current || current.active !== true
            || (isVersioned && Number(current.token_version) !== tokenVersion)
            || (legacy && Number(current.token_version) !== 0)
            || Number(current.session_epoch ?? 0) !== Number(sessionEpoch ?? 0)) {
          return res.status(401).json({ message: 'Sitzung ungültig. Bitte erneut anmelden.' });
        }
        req.user = {
          id: Number(current.id), name: current.name, role: current.role,
          filiale: current.filiale || null, email: current.email, active: true,
          force_password_change: current.force_password_change === true,
          token_version: Number(current.token_version),
          session_epoch: Number(current.session_epoch ?? 0),
        };
        req.authTokenKind = legacy ? 'legacy' : 'versioned';
        if (forceMode === FORCE_MODE_ENFORCE && req.user.force_password_change && routeOptions.allowForcePasswordChange !== true) {
          return res.status(403).json({ code: 'PASSWORD_CHANGE_REQUIRED', message: 'Passwortänderung erforderlich.' });
        }
        if (routeOptions.requiredRole && req.user.role !== routeOptions.requiredRole) {
          return res.status(403).json({ message: 'Zugriff verweigert.' });
        }
        if (current.runtime_mode === 'maintenance'
            && !(routeOptions.allowMaintenanceAdmin === true && req.user.role === 'Admin')) {
          return res.status(503).json({ code: 'MAINTENANCE_ACTIVE', message: 'Die Anwendung befindet sich vorübergehend im Wartungsmodus.' });
        }
        return next();
      } catch (error) {
        if (error.name === 'TokenExpiredError') {
          return res.status(401).json({ message: 'Token abgelaufen. Bitte erneut anmelden.' });
        }
        return res.status(401).json({ message: 'Token ungültig.' });
      }
    };
  };
}

function verifyToken(requiredRoleOrOptions) {
  return createVerifyToken(defaultOptions || {})(requiredRoleOrOptions);
}
verifyToken.createVerifyToken = createVerifyToken;
verifyToken.configureDefault = function configureDefault(options) {
  if (!options?.pool || !options?.jwtSecret) {
    throw new TypeError('Default token verification requires pool and jwtSecret.');
  }
  defaultOptions = Object.freeze({
    pool: options.pool,
    jwtSecret: options.jwtSecret,
    integration: options.integration || {},
  });
};

module.exports = verifyToken;
