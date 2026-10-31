const assert = require('node:assert/strict');
const test = require('node:test');
const { createProductionAuthIntegration } = require('./productionAuthIntegration');

test('production adapter requires explicit profile and bounds legacy acceptance to eight hours', () => {
  const now = Date.parse('2026-09-06T10:00:00Z');
  assert.throws(() => createProductionAuthIntegration({}, now), /not explicitly enabled/);
  assert.throws(() => createProductionAuthIntegration({ AUTH_RUNTIME_PROFILE: 'production-integration-candidate' }, now), /AUTH_TOKEN_MODE/);
  assert.throws(() => createProductionAuthIntegration({ AUTH_RUNTIME_PROFILE: 'production-integration-candidate', AUTH_TOKEN_MODE: 'strict' }, now), /AUTH_FORCE_PASSWORD_CHANGE_MODE/);
  assert.throws(() => createProductionAuthIntegration({ AUTH_RUNTIME_PROFILE: 'production-integration-candidate',
    AUTH_TOKEN_MODE: 'dual-legacy-8h', AUTH_LEGACY_ACCEPT_UNTIL: '2026-09-06T18:00:01Z',
    AUTH_FORCE_PASSWORD_CHANGE_MODE: 'observe' }, now), /at most eight hours/);
  const config = createProductionAuthIntegration({ AUTH_RUNTIME_PROFILE: 'production-integration-candidate',
    AUTH_TOKEN_MODE: 'dual-legacy-8h', AUTH_LEGACY_ACCEPT_UNTIL: '2026-09-06T18:00:00Z',
    AUTH_FORCE_PASSWORD_CHANGE_MODE: 'guide' }, now);
  assert.equal(config.legacyAcceptUntil, Date.parse('2026-09-06T18:00:00Z'));
  assert.equal(config.trustProxy, false);
});

test('proxy headers are trusted only after explicit hop verification', () => {
  const base = { AUTH_RUNTIME_PROFILE: 'production-integration-candidate', AUTH_TOKEN_MODE: 'strict', AUTH_FORCE_PASSWORD_CHANGE_MODE: 'observe' };
  assert.throws(() => createProductionAuthIntegration({ ...base, AUTH_TRUST_PROXY_HOPS: '1' }), /explicit verification/);
  assert.equal(createProductionAuthIntegration({ ...base, AUTH_TRUST_PROXY_VERIFIED: 'true', AUTH_TRUST_PROXY_HOPS: '1' }).trustProxy, 1);
});
