const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { PDFDocument } = require('pdf-lib');
const { canonicalJson, workerSignature } = require('../lib/qlikWorkerSignature');
const { createImportHandler, IMPORT_PATH } = require('./eurocontrol');

const secret = 'S'.repeat(32);
const nowMillis = 4_070_908_800_000;

async function makePayload() {
  const document = await PDFDocument.create();
  document.addPage();
  const source = Buffer.from(await document.save({ useObjectStreams: false }));
  return {
    fileName: 'synthetic.pdf',
    sourceSha256: crypto.createHash('sha256').update(source).digest('hex'),
    sourceSizeBytes: source.length,
    sourcePdfBase64: source.toString('base64'),
    report: {
      reportType: 'FILIALBERICHT', branchIdentifier: '041129', branchLabel: 'Synthetic A',
      reportDate: '2099-01-02', pageCount: 1, reportVersion: null, reportCreatedAt: null,
      parserVersion: '2.0.0',
      paymentRows: [
        { rowNumber: 1, label: 'BAR', sollCents: 1000, istCents: 1000, differenceCents: 0, withdrawalCents: 0 },
        { rowNumber: 2, label: 'Kartenzahlung', sollCents: 2200, istCents: 2200, differenceCents: 0, withdrawalCents: 0 },
        { rowNumber: 3, label: 'Visa', sollCents: 400, istCents: 400, differenceCents: 0, withdrawalCents: 0 },
      ],
      totals: { sollCents: 3600, istCents: 3600, differenceCents: 0, withdrawalCents: 0 },
      reportFields: [
        { fieldCode: 'REPORT_REVENUE_CENTS', valueKind: 'MONEY_CENTS', amountCents: 10000, pageNumber: 1, explicit: true },
        { fieldCode: 'PAYOUTS_CENTS', valueKind: 'MONEY_CENTS', amountCents: 0, pageNumber: 1, explicit: true },
        { fieldCode: 'CUSTOMER_COUNT', valueKind: 'COUNT', countValue: 4, pageNumber: 1, explicit: true },
        { fieldCode: 'AVERAGE_RECEIPT_CENTS', valueKind: 'MONEY_CENTS', amountCents: 2500, pageNumber: 1, explicit: true },
        { fieldCode: 'BANK_DEPOSIT_CENTS', valueKind: 'MONEY_CENTS', amountCents: 1000, pageNumber: 1, explicit: true },
        { fieldCode: 'VAT_7_GROSS_CENTS', valueKind: 'MONEY_CENTS', amountCents: 3000, pageNumber: 1, explicit: true },
        { fieldCode: 'VAT_19_GROSS_CENTS', valueKind: 'MONEY_CENTS', amountCents: 7000, pageNumber: 1, explicit: true },
      ],
    },
  };
}

function poolFake() {
  const client = {
    async query(sql, params = []) {
      if (sql.includes('WHERE imported.source_sha256')) return { rows: [] };
      if (sql.includes('branch_source_identifiers')) return { rowCount: 1, rows: [{ branch_id: 2, branch_name: 'Synthetic A' }] };
      if (sql.includes('eurocontrol_payment_mappings')) return { rows: [
        { external_label: 'BAR', mapping_role: 'IGNORE', provider_code: null },
        { external_label: 'Kartenzahlung', mapping_role: 'EC_CASH', provider_code: null },
        { external_label: 'Visa', mapping_role: 'CREDIT_PROVIDER', provider_code: 'VISA' },
      ] };
      if (sql.includes('FROM controlling.eurocontrol_derived_daily_values')) return { rows: [] };
      if (sql.includes('INSERT INTO controlling.eurocontrol_imports')) return { rows: [{
        id: 'import-http', status: 'IMPORTED', report_date: '2099-01-02', source_sha256: params[3],
      }] };
      return { rows: [] };
    },
    release() {},
  };
  return { connect: async () => client };
}

async function invoke(payload, suppliedHeaders) {
  const normalized = Object.fromEntries(Object.entries(suppliedHeaders).map(([key, value]) => [key.toLowerCase(), value]));
  const req = {
    body: payload,
    method: 'POST',
    get(name) { return normalized[String(name).toLowerCase()]; },
  };
  const result = { statusCode: 200, body: null };
  const res = {
    status(value) { result.statusCode = value; return this; },
    json(value) { result.body = value; return this; },
  };
  const importer = async () => ({
    deduplicated: false,
    id: 'import-http',
    status: 'IMPORTED',
    report_date: payload.report.reportDate,
    branch_name: 'Synthetic A',
    source_sha256: payload.sourceSha256,
    ec_cash_cents: 2200,
    credit_card_cents: 400,
  });
  await createImportHandler({
    pool: poolFake(), secret, now: () => nowMillis, importer,
  })(req, res);
  return result;
}

function headers(payload, signatureSecret = secret) {
  const timestamp = String(nowMillis / 1000);
  return {
    'content-type': 'application/json',
    'x-eurocontrol-timestamp': timestamp,
    'x-eurocontrol-content-sha256': crypto.createHash('sha256').update(canonicalJson(payload)).digest('hex'),
    'x-eurocontrol-signature': workerSignature({
      secret: signatureSecret, timestamp, method: 'POST', path: IMPORT_PATH, payload,
    }),
  };
}

test('nimmt nur signierte EuroControl-Berichte an und gibt keine PDF-Rohdaten zurück', async () => {
  const payload = await makePayload();
  const response = await invoke(payload, headers(payload));
  assert.equal(response.statusCode, 201);
  assert.equal(response.body.import.ecCashCents, 2200);
  assert.equal(response.body.import.creditCardCents, 400);
  assert.equal(response.body.import.sourceSha256, payload.sourceSha256);
  assert.equal(JSON.stringify(response.body).includes('sourcePdfBase64'), false);
});

test('weist manipulierten Inhalt und eine falsche Signatur vor dem Import ab', async () => {
  const payload = await makePayload();
  const badHeaders = headers(payload);
  badHeaders['x-eurocontrol-signature'] = '0'.repeat(64);
  const response = await invoke(payload, badHeaders);
  assert.equal(response.statusCode, 401);

  const altered = structuredClone(payload);
  altered.report.paymentRows[0].sollCents += 1;
  const response2 = await invoke(altered, headers(payload));
  assert.equal(response2.statusCode, 401);
});
