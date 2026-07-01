// routes/devDb.js
const express = require('express');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

const router = express.Router();

const MAX_SQL_LENGTH = 5000;

function isAdmin(req) {
  return req.user && req.user.role === 'Admin';
}

function normalizeSql(sql) {
  return String(sql || '').trim();
}

function containsForbiddenComment(sql) {
  return sql.includes('--') || sql.includes('/*') || sql.includes('*/');
}

function hasMultipleStatements(sql) {
  const withoutTrailingSemicolon = sql.trim().replace(/;+\s*$/, '');
  return withoutTrailingSemicolon.includes(';');
}

function isSelectAllowed(sql) {
  return /^select\s+/i.test(sql.trim());
}

function isCreateSchemaAllowed(sql) {
  const pattern =
    /^create\s+schema\s+if\s+not\s+exists\s+[a-zA-Z_][a-zA-Z0-9_]*\s*;?$/i;

  return pattern.test(sql.trim());
}

function isCreateTableAllowed(sql) {
  const normalized = sql.trim();

  if (!/^create\s+table\s+if\s+not\s+exists\s+[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s*\(/i.test(normalized)) {
    return false;
  }

  if (!/\)\s*;?$/i.test(normalized)) {
    return false;
  }

  const forbiddenKeywords = [
    /\bdrop\b/i,
    /\btruncate\b/i,
    /\bdelete\b/i,
    /\bupdate\b/i,
    /\balter\b/i,
    /\bgrant\b/i,
    /\brevoke\b/i,
    /\bcopy\b/i,
    /\bexecute\b/i,
    /\bcall\b/i,
  ];

  return !forbiddenKeywords.some((pattern) => pattern.test(normalized));
}

function isCreateIndexAllowed(sql) {
  const pattern =
    /^create\s+index\s+if\s+not\s+exists\s+[a-zA-Z_][a-zA-Z0-9_]*\s+on\s+[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s*\(.+\)\s*;?$/i;

  return pattern.test(sql.trim());
}

function isInsertAllowed(sql) {
  const normalized = sql.trim();

  if (!/^insert\s+into\s+[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s*\(/i.test(normalized)) {
    return false;
  }

  if (!/\)\s+values\s*\(.+\)\s*(on\s+conflict\s*\(.+\)\s+do\s+(nothing|update\s+set\s+.+))?\s*;?$/i.test(normalized)) {
    return false;
  }

  const forbiddenKeywords = [
    /\bdrop\b/i,
    /\btruncate\b/i,
    /\bdelete\b/i,
    /\bgrant\b/i,
    /\brevoke\b/i,
    /\bcopy\b/i,
    /\bexecute\b/i,
    /\bcall\b/i,
  ];

  return !forbiddenKeywords.some((pattern) => pattern.test(normalized));
}

function isAllowedDropConstraint(sql) {
  const pattern =
    /^alter\s+table\s+[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s+drop\s+constraint\s+if\s+exists\s+[a-zA-Z_][a-zA-Z0-9_]*\s*;?$/i;

  return pattern.test(sql.trim());
}

function isAllowedAddConstraint(sql) {
  const normalized = sql.trim();

  if (!/^alter\s+table\s+[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s+add\s+constraint\s+[a-zA-Z_][a-zA-Z0-9_]*\s+/i.test(normalized)) {
    return false;
  }

  if (!/\s*;?$/i.test(normalized)) {
    return false;
  }

  const allowedConstraintTypes = [
    /\bunique\s*\(.+\)/i,
    /\bforeign\s+key\s*\(.+\)\s+references\s+[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s*\(.+\)/i,
    /\bcheck\s*\(.+\)/i,
  ];

  if (!allowedConstraintTypes.some((pattern) => pattern.test(normalized))) {
    return false;
  }

  const forbiddenKeywords = [
    /\bdrop\b/i,
    /\btruncate\b/i,
    /\bdelete\b/i,
    /\bupdate\b/i,
    /\binsert\b/i,
    /\bgrant\b/i,
    /\brevoke\b/i,
    /\bcopy\b/i,
    /\bexecute\b/i,
    /\bcall\b/i,
  ];

  return !forbiddenKeywords.some((pattern) => pattern.test(normalized));
}

function validateSql(sql) {
  if (!sql) {
    return { ok: false, message: 'SQL-Befehl fehlt.' };
  }

  if (sql.length > MAX_SQL_LENGTH) {
    return { ok: false, message: `SQL-Befehl ist zu lang. Maximum: ${MAX_SQL_LENGTH} Zeichen.` };
  }

  if (containsForbiddenComment(sql)) {
    return { ok: false, message: 'SQL-Kommentare sind aus Sicherheitsgründen nicht erlaubt.' };
  }

  if (hasMultipleStatements(sql)) {
    return { ok: false, message: 'Mehrere SQL-Statements sind nicht erlaubt.' };
  }

  if (isSelectAllowed(sql)) {
    return { ok: true, mode: 'select' };
  }

  if (isCreateSchemaAllowed(sql)) {
    return { ok: true, mode: 'create_schema' };
  }

  if (isCreateTableAllowed(sql)) {
    return { ok: true, mode: 'create_table' };
  }

  if (isCreateIndexAllowed(sql)) {
    return { ok: true, mode: 'create_index' };
  }

  if (isInsertAllowed(sql)) {
    return { ok: true, mode: 'insert' };
  }

  if (isAllowedDropConstraint(sql)) {
    return { ok: true, mode: 'drop_constraint' };
  }

  if (isAllowedAddConstraint(sql)) {
    return { ok: true, mode: 'add_constraint' };
  }

  return {
    ok: false,
    message:
      'Dieser SQL-Befehl ist nicht erlaubt. Erlaubt sind SELECT, CREATE SCHEMA, CREATE TABLE, CREATE INDEX, INSERT INTO oder gezielte ALTER TABLE CONSTRAINT-Befehle.',
  };
}

router.post('/execute', verifyToken(), async (req, res) => {
  if (process.env.DEV_DB_CONSOLE_ENABLED !== 'true') {
    return res.status(403).json({
      ok: false,
      message: 'DEV-DB-Konsole ist serverseitig deaktiviert.',
    });
  }

  if (!isAdmin(req)) {
    return res.status(403).json({
      ok: false,
      message: 'Zugriff verweigert. Nur Admin darf DEV-DB-Befehle ausführen.',
    });
  }

  const { sql, pin } = req.body || {};

  if (!process.env.DEV_DB_CONSOLE_PIN) {
    return res.status(500).json({
      ok: false,
      message: 'DEV_DB_CONSOLE_PIN ist serverseitig nicht gesetzt.',
    });
  }

  if (!pin || String(pin) !== String(process.env.DEV_DB_CONSOLE_PIN)) {
    return res.status(401).json({
      ok: false,
      message: 'PIN falsch oder fehlt.',
    });
  }

  const normalizedSql = normalizeSql(sql);
  const validation = validateSql(normalizedSql);

  if (!validation.ok) {
    return res.status(400).json({
      ok: false,
      message: validation.message,
    });
  }

  const startedAt = new Date();

  try {
    const result = await pool.query(normalizedSql);
    const finishedAt = new Date();

    console.warn('[DEV-DB-CONSOLE]', {
      userId: req.user.id,
      userName: req.user.name,
      role: req.user.role,
      mode: validation.mode,
      sql: normalizedSql,
      rowCount: result.rowCount,
      startedAt: startedAt.toISOString(),
      finishedAt: finishedAt.toISOString(),
    });

    return res.json({
      ok: true,
      mode: validation.mode,
      rowCount: result.rowCount,
      rows: result.rows || [],
      fields: result.fields ? result.fields.map((field) => field.name) : [],
      executedAt: finishedAt.toISOString(),
    });
  } catch (err) {
    console.error('[DEV-DB-CONSOLE-ERROR]', {
      userId: req.user?.id,
      userName: req.user?.name,
      sql: normalizedSql,
      error: err.message,
      code: err.code,
    });

    return res.status(500).json({
      ok: false,
      message: 'SQL-Ausführung fehlgeschlagen.',
      error: err.message,
      code: err.code || null,
    });
  }
});

module.exports = router;