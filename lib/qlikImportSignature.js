const crypto = require('node:crypto');

function payloadSha256(payload) {
  if (!Buffer.isBuffer(payload)) return null;
  return crypto.createHash('sha256').update(payload).digest('hex');
}

function importSignature({ secret, timestamp, method, path, contentSha256, fileName }) {
  if (!secret) throw new Error('QLIK_IMPORT_SECRET fehlt.');
  const message = [timestamp, String(method || '').toUpperCase(), path, contentSha256, fileName].join('.');
  return crypto.createHmac('sha256', secret).update(message).digest('hex');
}

function verifyImportSignature({
  secret,
  timestamp,
  signature,
  method,
  path,
  contentSha256,
  fileName,
  now = Date.now(),
  maxAgeMs = 5 * 60_000,
}) {
  if (
    typeof secret !== 'string' || secret.length < 32
    || !/^\d+$/.test(String(timestamp || ''))
    || !/^[0-9a-f]{64}$/i.test(String(signature || ''))
    || !/^[0-9a-f]{64}$/i.test(String(contentSha256 || ''))
    || !fileName
  ) return false;
  const millis = Number(timestamp) * 1000;
  if (!Number.isFinite(millis) || Math.abs(now - millis) > maxAgeMs) return false;
  const expected = importSignature({ secret, timestamp, method, path, contentSha256, fileName });
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(String(signature).toLowerCase()));
}

module.exports = { payloadSha256, verifyImportSignature };
