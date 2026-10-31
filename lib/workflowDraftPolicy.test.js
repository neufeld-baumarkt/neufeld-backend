const test = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeClosePayload,
  normalizeDraftKey,
  normalizeDraftPayload,
  normalizeWorkflowType,
} = require('./workflowDraftPolicy');

test('validiert Tageskassen-Entwürfe und ihre Revision', () => {
  assert.equal(normalizeWorkflowType('drawer_opening'), 'drawer_opening');
  assert.equal(normalizeDraftKey('active'), 'active');
  assert.deepEqual(
    normalizeDraftPayload({ stage: 'count', state: { counts: { 100: '2' } }, expectedRevision: 3 }),
    { stage: 'count', state: { counts: { 100: '2' } }, serialized: '{"counts":{"100":"2"}}', expectedRevision: 3 },
  );
  assert.deepEqual(normalizeClosePayload({ status: 'completed', expectedRevision: 4 }), { status: 'completed', expectedRevision: 4 });
});

test('weist PINs, Tokens und unzulässige Formen vollständig ab', () => {
  assert.throws(() => normalizeWorkflowType('unknown'), /ungültig/);
  assert.throws(() => normalizeDraftKey('../escape'), /ungültig/);
  assert.throws(() => normalizeDraftPayload({ stage: 'count', state: { firstPin: '1234' } }), /Geheimnisfeld/);
  assert.throws(() => normalizeDraftPayload({ stage: 'count', state: { nested: { proofToken: 'secret' } } }), /Geheimnisfeld/);
  assert.throws(() => normalizeDraftPayload({ stage: 'count', state: [], expectedRevision: 0 }), /Objekt/);
  assert.throws(() => normalizeClosePayload({ status: 'discarded', expectedRevision: 0 }), /revision/i);
});
