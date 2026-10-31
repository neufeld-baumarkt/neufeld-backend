const PASSWORD_POLICY = Object.freeze({ minLength: 12, maxLength: 128, maxUtf8Bytes: 72 });

function validatePassword(value) {
  if (typeof value !== 'string') {
    return { ok: false, message: 'Das Passwort muss als Text angegeben werden.' };
  }
  if (!value.trim()) {
    return { ok: false, message: 'Das Passwort darf nicht leer sein.' };
  }
  if (value.length < PASSWORD_POLICY.minLength || value.length > PASSWORD_POLICY.maxLength) {
    return {
      ok: false,
      message: `Das Passwort muss ${PASSWORD_POLICY.minLength} bis ${PASSWORD_POLICY.maxLength} Zeichen lang sein.`,
    };
  }
  if (Buffer.byteLength(value, 'utf8') > PASSWORD_POLICY.maxUtf8Bytes) {
    return { ok: false, message: `Das Passwort darf höchstens ${PASSWORD_POLICY.maxUtf8Bytes} UTF-8-Bytes lang sein.` };
  }
  return { ok: true };
}

function assertValidPassword(value) {
  const result = validatePassword(value);
  if (!result.ok) throw Object.assign(new Error(result.message), { statusCode: 400 });
  return value;
}

function assertSaveroomPassword(value) {
  assertValidPassword(value);
  if (!value.startsWith('sandbox-')) {
    throw Object.assign(new Error('DEV-Passwörter müssen mit sandbox- beginnen.'), { statusCode: 400 });
  }
  return value;
}

module.exports = { PASSWORD_POLICY, assertSaveroomPassword, assertValidPassword, validatePassword };
