const jwt = require('jsonwebtoken');

const AUDIENCE = 'cash-workflow';

function secret() {
  const value = process.env.JWT_SECRET;
  if (!value) throw new Error('JWT_SECRET is not set in environment variables');
  return value;
}

function createEmployeePinProof(employee) {
  return jwt.sign(
    { type: 'employee-pin', name: employee.name },
    secret(),
    { algorithm: 'HS256', audience: AUDIENCE, subject: String(employee.id), expiresIn: '20m' }
  );
}

function verifyEmployeePinProof(token) {
  try {
    const payload = jwt.verify(String(token || ''), secret(), { algorithms: ['HS256'], audience: AUDIENCE });
    const id = Number(payload.sub);
    if (payload.type !== 'employee-pin' || !Number.isInteger(id) || id <= 0) throw new Error('invalid proof');
    return { id, name: String(payload.name || '') };
  } catch {
    throw Object.assign(new Error('Die persönliche PIN-Bestätigung ist abgelaufen. Bitte PIN erneut eingeben.'), { statusCode: 401 });
  }
}

module.exports = { createEmployeePinProof, verifyEmployeePinProof };
