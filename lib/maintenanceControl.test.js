const test = require('node:test');
const assert = require('node:assert/strict');
const {
  createMaintenanceGate, mayContinueDuringDrain, publicRuntimeStatus,
} = require('./maintenanceControl');

function response() {
  return {
    statusCode: 200, body: null,
    status(value) { this.statusCode = value; return this; },
    json(value) { this.body = value; return this; },
  };
}

test('draining allows reads and cash completion but blocks unrelated writes', () => {
  assert.equal(mayContinueDuringDrain({ method: 'GET', originalUrl: '/api/budget' }), true);
  assert.equal(mayContinueDuringDrain({ method: 'PUT', originalUrl: '/api/tageskasse/workflow-drafts/x/y' }), true);
  assert.equal(mayContinueDuringDrain({ method: 'POST', originalUrl: '/api/budget/bookings' }), false);
});

test('public status exposes operational facts without session epoch', () => {
  const value = publicRuntimeStatus(
    { mode: 'draining', message: 'Bitte abschließen', revision: 2, updated_at: 'now', session_epoch: 9 },
    { activeCount: 3, lastActivityAt: 'later' }
  );
  assert.deepEqual(value, {
    mode: 'draining', message: 'Bitte abschließen', revision: 2,
    activeCashProcesses: 3, lastCashActivityAt: 'later', updatedAt: 'now',
  });
  assert.equal('sessionEpoch' in value, false);
});

test('maintenance gate fails closed and only lets the login endpoint through', async () => {
  const pool = { query: async () => ({ rows: [{ mode: 'maintenance' }] }) };
  const gate = createMaintenanceGate({ pool });
  let next = false;
  const loginRes = response();
  await gate({ method: 'POST', originalUrl: '/api/login' }, loginRes, () => { next = true; });
  assert.equal(next, true);

  next = false;
  const appRes = response();
  await gate({ method: 'GET', originalUrl: '/api/budget' }, appRes, () => { next = true; });
  assert.equal(next, false);
  assert.equal(appRes.statusCode, 503);
  assert.equal(appRes.body.code, 'MAINTENANCE_ACTIVE');
});
