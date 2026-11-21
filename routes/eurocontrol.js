const crypto = require('node:crypto');
const express = require('express');
const { canonicalJson, verifyWorkerSignature } = require('../lib/qlikWorkerSignature');
const { importEuroControlReport } = require('../lib/eurocontrolImport');

const IMPORT_PATH = '/api/eurocontrol/internal/import';

function createEuroControlRouter({ pool, secret, now = () => Date.now() }) {
  const router = express.Router();
  router.use((_req, res, next) => {
    res.setHeader('Cache-Control', 'no-store');
    next();
  });
  router.post('/internal/import', createImportHandler({ pool, secret, now }));
  return router;
}

function createImportHandler({ pool, secret, now = () => Date.now() }) {
  return async (req, res) => {
    const suppliedDigest = String(req.get('x-eurocontrol-content-sha256') || '').toLowerCase();
    const actualDigest = crypto.createHash('sha256').update(canonicalJson(req.body)).digest('hex');
    const valid = suppliedDigest === actualDigest && verifyWorkerSignature({
      secret,
      timestamp: req.get('x-eurocontrol-timestamp'),
      signature: req.get('x-eurocontrol-signature'),
      method: req.method,
      path: IMPORT_PATH,
      payload: req.body,
      now: now(),
    });
    if (!valid) return res.status(401).json({ message: 'Ungültige EuroControl-Worker-Signatur.' });
    try {
      const result = await importEuroControlReport({ pool, payload: req.body });
      return res.status(result.deduplicated ? 200 : 201).json({
        ok: true,
        deduplicated: result.deduplicated,
        import: {
          id: result.id,
          status: result.status,
          sourceSha256: result.source_sha256,
          reportDate: result.report_date,
          branchName: result.branch_name,
          ecCashCents: Number(result.ec_cash_cents),
          creditCardCents: Number(result.credit_card_cents),
        },
      });
    } catch (error) {
      const status = Number(error?.statusCode) || 500;
      if (status >= 500) console.error('[EUROCONTROL_IMPORT_ERROR]', error?.code || error?.message);
      return res.status(status).json({
        message: status >= 500 ? 'EuroControl-Bericht konnte nicht gespeichert werden.' : error.message,
      });
    }
  };
}

module.exports = { createEuroControlRouter, createImportHandler, IMPORT_PATH };
