const assert = require('node:assert/strict');
const test = require('node:test');
const jwt = require('jsonwebtoken');
const { createVerifyToken } = require('./verifyToken');

const secret = ['test', 'only', 'secret', 'that', 'is', 'long', 'enough'].join('-');
function responseRecorder() { return { statusCode: 200, headers: {}, body: null, setHeader(name, value) { this.headers[name] = value; }, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } }; }
function row(overrides = {}) { return { id: 5, name: 'sandbox-user', role: 'Supervisor', filiale: null, email: 'user@example.invalid', force_password_change: false, active: true, token_version: 2, ...overrides }; }
function token(version = 2) { return jwt.sign({ id: 5, tokenVersion: version }, secret, { expiresIn: '1h' }); }

async function run(current, routeOptions, version = 2) {
  return runToken(current, routeOptions, token(version));
}

async function runToken(current, routeOptions, encodedToken, options = {}) {
  const verifyToken = createVerifyToken({ jwtSecret: secret, pool: { query: async () => ({ rows: current ? [current] : [] }) }, ...options });
  const req = { headers: { authorization: `Bearer ${encodedToken}` } };
  const res = responseRecorder();
  let next = false;
  await verifyToken(routeOptions)(req, res, () => { next = true; });
  return { req, res, next };
}

test('middleware accepts current session and takes authorization data from DB', async () => {
  const result = await run(row({ role: 'Admin' }), 'Admin');
  assert.equal(result.next, true);
  assert.equal(result.req.user.role, 'Admin');
  assert.equal(result.res.headers['Cache-Control'], 'no-store');
});

test('middleware revokes old tokens after password reset, role change or deactivation', async () => {
  assert.equal((await run(row({ token_version: 3 }), null, 2)).res.statusCode, 401);
  assert.equal((await run(row({ role: 'Filiale' }), 'Admin')).res.statusCode, 403);
  assert.equal((await run(row({ active: false }))).res.statusCode, 401);
});

test('global session epoch revokes every older token and admits only maintenance Admin control', async () => {
  const oldToken = jwt.sign({ id: 5, tokenVersion: 2, sessionEpoch: 0 }, secret, { expiresIn: '1h' });
  assert.equal((await runToken(row({ session_epoch: 1 }), null, oldToken)).res.statusCode, 401);

  const currentToken = jwt.sign({ id: 5, tokenVersion: 2, sessionEpoch: 1 }, secret, { expiresIn: '1h' });
  const blocked = await runToken(row({ session_epoch: 1, runtime_mode: 'maintenance', role: 'Admin' }), null, currentToken);
  assert.equal(blocked.res.statusCode, 503);
  assert.equal(blocked.res.body.code, 'MAINTENANCE_ACTIVE');
  const control = await runToken(
    row({ session_epoch: 1, runtime_mode: 'maintenance', role: 'Admin' }),
    { requiredRole: 'Admin', allowMaintenanceAdmin: true }, currentToken
  );
  assert.equal(control.next, true);
});

test('forced password change blocks normal APIs but allows session and password routes', async () => {
  const blocked = await run(row({ force_password_change: true }));
  assert.equal(blocked.res.statusCode, 403);
  assert.equal(blocked.res.body.code, 'PASSWORD_CHANGE_REQUIRED');
  assert.equal((await run(row({ force_password_change: true }), { allowForcePasswordChange: true })).next, true);
});

test('middleware rejects invalid signatures, expiry, algorithms and malformed claim types', async () => {
  const otherSecret = ['different', 'test', 'secret', 'with', 'enough', 'length'].join('-');
  const cases = [
    jwt.sign({ id: 5, tokenVersion: 2 }, otherSecret, { algorithm: 'HS256' }),
    jwt.sign({ id: 5, tokenVersion: 2 }, secret, { algorithm: 'HS256', expiresIn: -1 }),
    jwt.sign({ id: 5, tokenVersion: 2 }, secret, { algorithm: 'HS384' }),
    jwt.sign({ id: 5 }, secret, { algorithm: 'HS256' }),
    jwt.sign({ id: '5', tokenVersion: 2 }, secret, { algorithm: 'HS256' }),
    jwt.sign({ id: 5, tokenVersion: '2' }, secret, { algorithm: 'HS256' }),
  ];
  for (const encoded of cases) {
    const result = await runToken(row(), null, encoded);
    assert.equal(result.next, false);
    assert.equal(result.res.statusCode, 401);
  }
});

test('deleted users are rejected and current DB branch replaces all stale client state', async () => {
  assert.equal((await run(null)).res.statusCode, 401);
  const result = await run(row({ role: 'Filiale', filiale: 'Ahaus' }));
  assert.equal(result.next, true);
  assert.equal(result.req.user.role, 'Filiale');
  assert.equal(result.req.user.filiale, 'Ahaus');
});

test('legacy tokens are accepted only inside the fixed window and while token_version is zero', async () => {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const legacyToken = jwt.sign({ id: 5, role: 'Admin', filiale: 'Alle', iat: nowSeconds }, secret, { expiresIn: '8h' });
  const integration = { tokenMode: 'dual-legacy-8h', legacyAcceptUntil: Date.now() + 60_000, forcePasswordChangeMode: 'observe' };
  assert.equal((await runToken(row({ token_version: 0 }), null, legacyToken, { integration })).next, true);
  const currentRole = await runToken(row({ token_version: 0, role: 'Filiale', filiale: 'Ahaus' }), 'Admin', legacyToken, { integration });
  assert.equal(currentRole.res.statusCode, 403);
  assert.equal(currentRole.req.user.role, 'Filiale');
  assert.equal((await runToken(row({ token_version: 0, active: false }), null, legacyToken, { integration })).res.statusCode, 401);
  assert.equal((await runToken(row({ token_version: 1 }), null, legacyToken, { integration })).res.statusCode, 401);
  assert.equal((await runToken(row({ token_version: 0 }), null, legacyToken, { integration: { ...integration, legacyAcceptUntil: Date.now() - 1 } })).res.statusCode, 401);
  const malformedVersion = jwt.sign({ id: 5, tokenVersion: '0' }, secret, { expiresIn: '1h' });
  assert.equal((await runToken(row({ token_version: 0 }), null, malformedVersion, { integration })).res.statusCode, 401);
  const manipulated = jwt.sign({ id: 5, role: 'Admin' }, 'different-test-secret-that-is-long-enough', { expiresIn: '1h' });
  assert.equal((await runToken(row({ token_version: 0 }), null, manipulated, { integration })).res.statusCode, 401);
  const overlong = jwt.sign({ id: 5 }, secret, { expiresIn: '28801s' });
  assert.equal((await runToken(row({ token_version: 0 }), null, overlong, { integration })).res.statusCode, 401);
  const earlierCutoff = { ...integration, legacyAcceptUntil: Date.now() + 10_000 };
  const afterCutoff = jwt.sign({ id: 5, iat: Math.floor(earlierCutoff.legacyAcceptUntil / 1000) + 1 }, secret, { expiresIn: '1h' });
  assert.equal((await runToken(row({ token_version: 0 }), null, afterCutoff, { integration: earlierCutoff, now: () => Date.now() })).res.statusCode, 401);
});

test('versioned tokens must also have bounded iat and exp claims', async () => {
  const overlong = jwt.sign({ id: 5, tokenVersion: 2 }, secret, { expiresIn: '9h' });
  const noTimestamp = jwt.sign({ id: 5, tokenVersion: 2 }, secret, { noTimestamp: true, expiresIn: '1h' });
  assert.equal((await runToken(row(), null, overlong)).res.statusCode, 401);
  assert.equal((await runToken(row(), null, noTimestamp)).res.statusCode, 401);
});

test('force phases observe and guide expose access while enforce blocks normal APIs', async () => {
  for (const mode of ['observe', 'guide']) {
    const result = await runToken(row({ force_password_change: true }), null, token(), { integration: { forcePasswordChangeMode: mode } });
    assert.equal(result.next, true);
  }
  assert.equal((await runToken(row({ force_password_change: true }), null, token(), { integration: { forcePasswordChangeMode: 'enforce' } })).res.statusCode, 403);
});
