const express = require('express');
const verifyToken = require('../middleware/verifyToken');
const { importQlikCsv } = require('../lib/qlikImport');
const { payloadSha256, verifyImportSignature } = require('../lib/qlikImportSignature');
const {
  CONTROLLING_ROLES,
  loadDayCloseReconciliation,
  loadDerivedValues,
} = require('../lib/qlikDerivedValues');

const IMPORT_PATH = '/api/qlik/internal/import';
const csvParser = express.raw({
  type: ['text/csv', 'application/csv', 'application/octet-stream'],
  limit: '1mb',
});

function createQlikImportRouter({ pool, secret, now = () => Date.now(), authenticate = verifyToken() }) {
  const router = express.Router();

  router.use((_req, res, next) => {
    res.setHeader('Cache-Control', 'no-store');
    next();
  });

  function requireControlling(req, res, next) {
    if (req.user && CONTROLLING_ROLES.has(req.user.role)) return next();
    return res.status(403).json({ message: 'Zugriff verweigert.' });
  }

  function sendReadError(res, error, fallback) {
    const status = Number(error?.statusCode) || 500;
    if (status >= 500) console.error('[QLIK_VALUE_ERROR]', error?.code || error?.message);
    return res.status(status).json({ message: status >= 500 ? fallback : error.message });
  }

  router.get('/values', authenticate, requireControlling, async (req, res) => {
    try {
      const result = await loadDerivedValues(pool, {
        branch: req.query.branch,
        businessDate: req.query.date,
        consumerCode: req.query.consumer,
      });
      return res.json({ ok: true, ...result });
    } catch (error) {
      return sendReadError(res, error, 'Qlik-Fachwerte konnten nicht geladen werden.');
    }
  });

  router.get('/reconciliation/day-close', authenticate, requireControlling, async (req, res) => {
    try {
      const result = await loadDayCloseReconciliation(pool, {
        branch: req.query.branch,
        businessDate: req.query.date,
      });
      return res.json({ ok: true, ...result });
    } catch (error) {
      return sendReadError(res, error, 'Qlik-Gegenprüfung konnte nicht geladen werden.');
    }
  });

  router.post('/internal/import', csvParser, async (req, res) => {
    const payload = req.body;
    const fileName = String(req.get('x-qlik-file-name') || '').trim();
    const suppliedHash = String(req.get('x-qlik-content-sha256') || '').toLowerCase();
    const actualHash = payloadSha256(payload);
    const signatureValid = verifyImportSignature({
      secret,
      timestamp: req.get('x-qlik-timestamp'),
      signature: req.get('x-qlik-signature'),
      method: req.method,
      path: IMPORT_PATH,
      contentSha256: suppliedHash,
      fileName,
      now: now(),
    });
    if (!signatureValid) return res.status(401).json({ message: 'Ungültige Qlik-Import-Signatur.' });
    if (!actualHash || actualHash !== suppliedHash) {
      return res.status(400).json({ message: 'Der Qlik-Dateihash stimmt nicht mit dem Dateiinhalt überein.' });
    }

    try {
      const result = await importQlikCsv({ pool, payload, fileName });
      return res.status(result.deduplicated ? 200 : 201).json({
        ok: true,
        deduplicated: result.deduplicated,
        import: {
          id: result.run.id,
          status: result.run.status,
          reportDate: result.report.date,
          rowCount: result.report.rowCount,
          customerCount: result.report.customerNumbers.length,
          paymentTypeCount: new Set(result.report.rows.map((row) => row.paymentType)).size,
          sourceSha256: suppliedHash,
          derivedValuesInserted: Number(result.materialized?.inserted_values || 0),
          sourceLinksInserted: Number(result.materialized?.inserted_sources || 0),
        },
      });
    } catch (error) {
      const clientError = ['INVALID_QLIK_FILIALBERICHT', 'QLIK_IMPORT_FAILED'].includes(error?.code);
      if (!clientError) console.error('[QLIK_IMPORT_ERROR]', error?.code || error?.message);
      return res.status(clientError ? 400 : 500).json({
        message: clientError ? error.message : 'Qlik-Import fehlgeschlagen.',
      });
    }
  });

  return router;
}

module.exports = { IMPORT_PATH, createQlikImportRouter };
