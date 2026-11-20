const crypto = require('node:crypto');

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])]));
  }
  return value;
}

function canonicalJson(value) {
  return JSON.stringify(canonical(value || {}));
}

function workerSignature({ secret, timestamp, method, path, payload }) {
  if (!secret) throw new Error('QLIK_IMPORT_SECRET fehlt.');
  const digest = crypto.createHash('sha256').update(canonicalJson(payload)).digest('hex');
  const message = [timestamp, String(method || '').toUpperCase(), path, digest].join('.');
  return crypto.createHmac('sha256', secret).update(message).digest('hex');
}

function verifyWorkerSignature({ secret, timestamp, signature, method, path, payload, now = Date.now() }) {
  if (typeof secret !== 'string' || secret.length < 32
      || !/^\d+$/.test(String(timestamp || '')) || !/^[0-9a-f]{64}$/i.test(String(signature || ''))) return false;
  const millis = Number(timestamp) * 1000;
  if (!Number.isFinite(millis) || Math.abs(now - millis) > 5 * 60_000) return false;
  const expected = workerSignature({ secret, timestamp, method, path, payload });
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(String(signature).toLowerCase()));
}

module.exports = { canonicalJson, workerSignature, verifyWorkerSignature };
