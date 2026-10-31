const assert = require('node:assert/strict');
const test = require('node:test');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const { createAuthHandlers, createRateLimiter, GENERIC_LOGIN_ERROR } = require('./auth');
const { signUserToken } = require('./auth');
const { validatePassword } = require('./passwordPolicy');

function responseRecorder() {
  return { statusCode: 200, headers: {}, body: null,
    setHeader(name, value) { this.headers[name] = value; },
    status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } };
}

function user(overrides = {}) {
  return { id: 3, name: 'sandbox-user', password: ['correct', 'hash'].join('-'), role: 'Supervisor', filiale: null,
    force_password_change: false, active: true, token_version: 4, ...overrides };
}

function authOptions(pool, overrides = {}) {
  return { pool, jwtSecret: ['test', 'only', 'secret', 'that', 'is', 'long', 'enough'].join('-'),
    comparePassword: async (plain, hash) => plain === 'correct' && hash === 'correct-hash',
    hashPassword: async () => 'new-hash', signToken: (row) => `token-v${row.token_version}`, ...overrides };
}

test('password policy rejects whitespace, short and overlong values', () => {
  assert.equal(validatePassword(' '.repeat(12)).ok, false);
  assert.equal(validatePassword('short').ok, false);
  assert.equal(validatePassword('x'.repeat(129)).ok, false);
  assert.equal(validatePassword('x'.repeat(73)).ok, false);
  assert.equal(validatePassword('ä'.repeat(37)).ok, false);
  assert.equal(validatePassword('ä'.repeat(36)).ok, true);
  assert.equal(validatePassword('long-enough-12').ok, true);
});

test('login returns the same error for unknown, wrong-password and inactive users', async () => {
  async function attempt(row, password) {
    const handlers = createAuthHandlers(authOptions({ query: async () => ({ rows: row ? [row] : [] }) }));
    const res = responseRecorder();
    await handlers.login({ body: { name: 'sandbox-user', password }, ip: '127.0.0.1' }, res);
    return res;
  }
  const unknown = await attempt(null, 'wrong');
  const wrong = await attempt(user(), 'wrong');
  const inactive = await attempt(user({ active: false }), 'correct');
  assert.deepEqual([unknown.statusCode, wrong.statusCode, inactive.statusCode], [401, 401, 401]);
  assert.deepEqual([unknown.body.message, wrong.body.message, inactive.body.message], [GENERIC_LOGIN_ERROR, GENERIC_LOGIN_ERROR, GENERIC_LOGIN_ERROR]);
});

test('valid and force-change logins include revocable session data but no hash', async () => {
  for (const forced of [false, true]) {
    const handlers = createAuthHandlers(authOptions({ query: async () => ({ rows: [user({ force_password_change: forced })] }) }));
    const res = responseRecorder();
    await handlers.login({ body: { name: 'sandbox-user', password: ['cor', 'rect'].join('') }, ip: '127.0.0.1' }, res);
    assert.equal(res.statusCode, 200);
    assert.equal(res.body.forcePasswordChange, forced);
    assert.equal(res.body.token, 'token-v4');
    assert.equal(res.body.auth.forcePasswordChangeMode, 'enforce');
    assert.equal(res.headers['Cache-Control'], 'no-store');
    assert.equal(JSON.stringify(res.body).includes('correct-hash'), false);
  }
});

test('dual transition tokens carry legacy claims but always include tokenVersion', () => {
  let payload;
  signUserToken(user({ name: 'Display Name', role: 'Admin', filiale: 'Alle' }), 'secret', (value) => { payload = value; return 'signed'; }, { tokenMode: 'dual-legacy-8h' });
  assert.deepEqual(payload, { id: 3, tokenVersion: 4, sessionEpoch: 0, name: 'Display Name', role: 'Admin', filiale: 'Alle' });
});

test('a dual transition token remains readable by the legacy claim contract after backend rollback', () => {
  const secret = ['test', 'only', 'secret', 'that', 'is', 'long', 'enough'].join('-');
  const encoded = signUserToken(user({ name: 'Peter Neufeld', role: 'Admin', filiale: 'Alle' }), secret, jwt.sign,
    { tokenMode: 'dual-legacy-8h' });
  const legacyView = jwt.verify(encoded, secret, { algorithms: ['HS256'] });
  assert.deepEqual({ id: legacyView.id, name: legacyView.name, role: legacyView.role, filiale: legacyView.filiale },
    { id: 3, name: 'Peter Neufeld', role: 'Admin', filiale: 'Alle' });
  assert.equal(legacyView.tokenVersion, 4);
});

test('historical rollback keeps stale dual-token authorization after later security mutations', () => {
  const secret = ['test', 'only', 'secret', 'that', 'is', 'long', 'enough'].join('-');
  const encoded = signUserToken(user({ role: 'Admin', filiale: 'Alle', active: true, token_version: 0 }), secret, jwt.sign,
    { tokenMode: 'dual-legacy-8h' });
  const historicalAuthorize = (token) => {
    const decoded = jwt.verify(token, secret, { algorithms: ['HS256'] });
    return { id: decoded.id, role: decoded.role, filiale: decoded.filiale };
  };
  for (const laterDatabaseState of [
    { role: 'Supervisor', active: true, token_version: 1 },
    { role: 'Admin', active: false, token_version: 1 },
    { role: 'Admin', active: true, token_version: 1, passwordReset: true },
  ]) {
    assert.deepEqual(historicalAuthorize(encoded), { id: 3, role: 'Admin', filiale: 'Alle' });
    assert.equal(laterDatabaseState.token_version, 1);
  }
});

test('login and current-password verification reject bcrypt-truncated inputs', async () => {
  let compared = false;
  const handlers = createAuthHandlers(authOptions({ query: async () => ({ rows: [user()] }) }, {
    comparePassword: async () => { compared = true; return true; },
  }));
  const login = responseRecorder();
  await handlers.login({ body: { name: 'sandbox-user', password: 'x'.repeat(73) }, ip: '127.0.0.3' }, login);
  assert.equal(login.statusCode, 400);
  assert.equal(compared, false);

  const change = responseRecorder();
  await handlers.changePassword({ body: { oldPassword: 'x'.repeat(73), newPassword: ['new', 'password', '123'].join('-') }, user: user(), ip: '127.0.0.4' }, change);
  assert.equal(change.statusCode, 400);
  assert.equal(compared, false);
});

test('existing bcrypt cost-10 hashes remain valid without an implicit mass rehash', async () => {
  const password = ['Existing', 'Passwort', '123'].join('-');
  const hash = await bcrypt.hash(password, 10);
  const calls = [];
  const pool = { query: async (sql) => { calls.push(sql); return { rows: [user({ password: hash })] }; } };
  const handlers = createAuthHandlers(authOptions(pool, { comparePassword: bcrypt.compare }));
  const response = responseRecorder();
  await handlers.login({ body: { name: 'sandbox-user', password }, ip: '127.0.0.7' }, response);
  assert.equal(response.statusCode, 200);
  assert.equal(bcrypt.getRounds(hash), 10);
  assert.equal(calls.some((sql) => /^UPDATE/i.test(sql)), false);
});

test('login rate limiter blocks the sixth attempt and resets by time window', () => {
  let now = 0;
  const limiter = createRateLimiter({ limit: 5, windowMs: 1000, now: () => now });
  for (let i = 0; i < 5; i += 1) assert.equal(limiter.consume('client'), true);
  assert.equal(limiter.consume('client'), false);
  now = 1001;
  assert.equal(limiter.consume('client'), true);
});

test('password change clears force flag, increments token version, audits safely and returns a replacement token', async () => {
  const calls = [];
  const client = { async query(sql, values) {
    calls.push({ sql, values });
    if (sql.includes('FROM public.users WHERE id = $1 FOR UPDATE')) return { rows: [user({ force_password_change: true })] };
    if (sql.startsWith('UPDATE public.users')) return { rows: [user({ password: undefined, force_password_change: false, token_version: 5 })] };
    return { rows: [] };
  }, release() {} };
  const pool = { connect: async () => client };
  const res = responseRecorder();
  const replacement = ['new', 'password', '123'].join('-');
  await createAuthHandlers(authOptions(pool)).changePassword({ body: { oldPassword: ['cor', 'rect'].join(''), newPassword: replacement }, user: user(), ip: '127.0.0.1' }, res);
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.token, 'token-v5');
  assert.equal(res.body.user.forcePasswordChange, false);
  assert.equal(res.headers['Cache-Control'], 'no-store');
  assert.equal(calls.some((call) => call.sql.includes('token_version=token_version+1')), true);
  const audit = calls.find((call) => call.sql.startsWith('INSERT INTO public.user_security_audit'));
  assert.equal(JSON.stringify(audit).includes(replacement), false);
  assert.equal(audit.values[4], 'password.changed');
});

test('replacement-token failure rolls back password, force flag, token version and audit together', async () => {
  const calls = [];
  const client = { async query(sql) {
    calls.push(sql);
    if (sql.includes('FROM public.users WHERE id = $1 FOR UPDATE')) return { rows: [user({ force_password_change: true })] };
    if (sql.startsWith('UPDATE public.users')) return { rows: [user({ force_password_change: false, token_version: 5 })] };
    return { rows: [] };
  }, release() {} };
  const res = responseRecorder();
  await createAuthHandlers(authOptions({ connect: async () => client }, {
    signToken: () => { throw new Error('signing failed'); },
  })).changePassword({ body: { oldPassword: 'correct', newPassword: ['new', 'password', '123'].join('-') }, user: user(), ip: '127.0.0.2' }, res);
  assert.equal(res.statusCode, 500);
  assert.equal(calls.includes('COMMIT'), false);
  assert.equal(calls.includes('ROLLBACK'), true);
});

test('session responses are not cacheable and contain only the public user shape', async () => {
  const res = responseRecorder();
  await createAuthHandlers(authOptions({})).session({ user: user({ email: 'hidden@example.invalid', password: 'hidden' }) }, res);
  assert.equal(res.headers['Cache-Control'], 'no-store');
  assert.deepEqual(Object.keys(res.body.user).sort(), ['active', 'filiale', 'forcePasswordChange', 'id', 'name', 'role']);
});

test('password change rejects wrong current password and invalid new password', async () => {
  const client = { async query(sql) { if (sql.includes('FROM public.users')) return { rows: [user()] }; return { rows: [] }; }, release() {} };
  const handlers = createAuthHandlers(authOptions({ connect: async () => client }));
  const wrong = responseRecorder();
  await handlers.changePassword({ body: { oldPassword: 'wrong', newPassword: ['new', 'password', '123'].join('-') }, user: user(), ip: 'one' }, wrong);
  assert.equal(wrong.statusCode, 401);
  const invalid = responseRecorder();
  await handlers.changePassword({ body: { oldPassword: ['cor', 'rect'].join(''), newPassword: 'short' }, user: user(), ip: 'two' }, invalid);
  assert.equal(invalid.statusCode, 400);
});
