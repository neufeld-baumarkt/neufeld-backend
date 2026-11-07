const express = require('express');
const { importQlikCsv } = require('../lib/qlikImport');
const { payloadSha256, verifyImportSignature } = require('../lib/qlikImportSignature');

const IMPORT_PATH = '/api/qlik/internal/import';
const csvParser = express.raw({
  type: ['text/csv', 'application/csv', 'application/octet-stream'],
  limit: '1mb',
});

function createQlikImportRouter({ pool, secret, now = () => Date.now() }) {
  if (typeof secret !== 'string' || secret.length < 32) {
    throw new Error('QLIK_IMPORT_SECRET fehlt oder ist zu kurz.');
  }
  const router = express.Router();

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

module.exports = { createQlikImportRouter };
