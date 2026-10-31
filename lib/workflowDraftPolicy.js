const WORKFLOW_TYPES = new Set(['drawer_opening', 'drawer_closing', 'safe_count', 'day_close']);
const SAFE_KEY = /^[A-Za-z0-9:_-]{1,80}$/;
const FORBIDDEN_KEY = /(pin|password|passwort|secret|token|proof|credential|authorization)/i;
const MAX_STATE_BYTES = 64 * 1024;

function problem(message, statusCode = 400) {
  return Object.assign(new Error(message), { statusCode });
}

function assertNoSecrets(value, path = 'state') {
  if (!value || typeof value !== 'object') return;
  for (const [key, nested] of Object.entries(value)) {
    if (FORBIDDEN_KEY.test(key)) throw problem(`Entwurf enthält ein verbotenes Geheimnisfeld: ${path}.${key}`);
    assertNoSecrets(nested, `${path}.${key}`);
  }
}

function normalizeWorkflowType(value) {
  const type = String(value || '');
  if (!WORKFLOW_TYPES.has(type)) throw problem('Entwurfstyp ist ungültig.');
  return type;
}

function normalizeDraftKey(value) {
  const key = String(value || 'active');
  if (!SAFE_KEY.test(key)) throw problem('Entwurfsschlüssel ist ungültig.');
  return key;
}

function normalizeDraftPayload(value = {}) {
  const stage = String(value.stage || '');
  const state = value.state;
  const expectedRevision = Number(value.expectedRevision ?? 0);
  if (!SAFE_KEY.test(stage)) throw problem('Entwurfsstufe ist ungültig.');
  if (!state || typeof state !== 'object' || Array.isArray(state)) throw problem('Entwurfszustand muss ein Objekt sein.');
  if (!Number.isInteger(expectedRevision) || expectedRevision < 0) throw problem('Entwurfsrevision ist ungültig.');
  assertNoSecrets(state);
  const serialized = JSON.stringify(state);
  if (Buffer.byteLength(serialized, 'utf8') > MAX_STATE_BYTES) throw problem('Entwurf ist zu groß.', 413);
  return { stage, state, serialized, expectedRevision };
}

function normalizeClosePayload(value = {}) {
  const expectedRevision = Number(value.expectedRevision);
  const status = String(value.status || '');
  if (!Number.isInteger(expectedRevision) || expectedRevision < 1) throw problem('Entwurfsrevision ist ungültig.');
  if (!['discarded', 'completed'].includes(status)) throw problem('Entwurfsabschluss ist ungültig.');
  return { expectedRevision, status };
}

module.exports = {
  MAX_STATE_BYTES,
  WORKFLOW_TYPES,
  assertNoSecrets,
  normalizeClosePayload,
  normalizeDraftKey,
  normalizeDraftPayload,
  normalizeWorkflowType,
};
