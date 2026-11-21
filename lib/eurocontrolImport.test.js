const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { PDFDocument } = require('pdf-lib');
const {
  calculateDailyValues,
  importEuroControlReport,
  validatePayload,
} = require('./eurocontrolImport');

async function payload(overrides = {}) {
  const document = await PDFDocument.create();
  document.addPage();
  const source = Buffer.from(await document.save({ useObjectStreams: false }));
  const paymentRows = [
    { rowNumber: 1, label: 'BAR', sollCents: 1000, istCents: 1000, differenceCents: 0, withdrawalCents: 0 },
    { rowNumber: 2, label: 'Kartenzahlung', sollCents: 2200, istCents: 2200, differenceCents: 0, withdrawalCents: 0 },
    { rowNumber: 3, label: 'Mastercard', sollCents: 300, istCents: 300, differenceCents: 0, withdrawalCents: 0 },
    { rowNumber: 4, label: 'Visa', sollCents: 400, istCents: 400, differenceCents: 0, withdrawalCents: 0 },
  ];
  return {
    fileName: '20990102_41129_filialbericht.pdf',
    sourceSha256: crypto.createHash('sha256').update(source).digest('hex'),
    sourceSizeBytes: source.length,
    sourcePdfBase64: source.toString('base64'),
    report: {
      reportType: 'FILIALBERICHT', branchIdentifier: '041129', branchLabel: 'Synthetic A',
      reportDate: '2099-01-02', pageCount: 1, reportVersion: 'test', reportCreatedAt: null,
      paymentRows,
      totals: { sollCents: 3900, istCents: 3900, differenceCents: 0, withdrawalCents: 0 },
    },
    ...overrides,
  };
}

function fakePool(handler) {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      return handler(sql, params, calls);
    },
    release() { calls.push({ sql: 'RELEASE', params: [] }); },
  };
  return { calls, connect: async () => client };
}

test('validiert Quelle, Summen und providerbezogene Fachwerte centgenau', async () => {
  const parsed = validatePayload(await payload());
  assert.equal(parsed.report.branchIdentifier, '041129');
  const mappings = new Map([
    ['BAR', { mapping_role: 'IGNORE', provider_code: null }],
    ['Kartenzahlung', { mapping_role: 'EC_CASH', provider_code: null }],
    ['Mastercard', { mapping_role: 'CREDIT_PROVIDER', provider_code: 'MASTERCARD' }],
    ['Visa', { mapping_role: 'CREDIT_PROVIDER', provider_code: 'VISA' }],
  ]);
  assert.deepEqual(calculateDailyValues(parsed.report.paymentRows, mappings), {
    mapped: [
      { ...parsed.report.paymentRows[0], mappingRole: 'IGNORE', providerCode: null },
      { ...parsed.report.paymentRows[1], mappingRole: 'EC_CASH', providerCode: null },
      { ...parsed.report.paymentRows[2], mappingRole: 'CREDIT_PROVIDER', providerCode: 'MASTERCARD' },
      { ...parsed.report.paymentRows[3], mappingRole: 'CREDIT_PROVIDER', providerCode: 'VISA' },
    ],
    ecCashCents: 2200,
    creditCardCents: 700,
    providerBreakdown: { MASTERCARD: 300, VISA: 400 },
  });
});

test('importiert Rohquelle, Einzelzeilen und Fachwerte in genau einer Transaktion', async () => {
  const pool = fakePool(async (sql) => {
    if (sql.includes('WHERE imported.source_sha256')) return { rows: [] };
    if (sql.includes('branch_source_identifiers')) return { rowCount: 1, rows: [{ branch_id: 2, branch_name: 'Synthetic A' }] };
    if (sql.includes('eurocontrol_payment_mappings')) return { rows: [
      { external_label: 'BAR', mapping_role: 'IGNORE', provider_code: null },
      { external_label: 'Kartenzahlung', mapping_role: 'EC_CASH', provider_code: null },
      { external_label: 'Mastercard', mapping_role: 'CREDIT_PROVIDER', provider_code: 'MASTERCARD' },
      { external_label: 'Visa', mapping_role: 'CREDIT_PROVIDER', provider_code: 'VISA' },
    ] };
    if (sql.includes('FROM controlling.eurocontrol_daily_values')) return { rows: [] };
    if (sql.includes('INSERT INTO controlling.eurocontrol_imports')) return { rows: [{ id: 'import-1', status: 'IMPORTED', report_date: '2099-01-02', source_sha256: 'a'.repeat(64) }] };
    return { rows: [] };
  });
  const source = await payload();
  const result = await importEuroControlReport({ pool, payload: source });
  assert.equal(result.ec_cash_cents, 2200);
  assert.equal(result.credit_card_cents, 700);
  assert.equal(pool.calls.filter((call) => call.sql.includes('eurocontrol_import_payment_rows')).length, 4);
  assert.equal(pool.calls.filter((call) => call.sql.includes('INSERT INTO controlling.eurocontrol_daily_values')).length, 1);
  assert.deepEqual(pool.calls.map((call) => call.sql).filter((sql) => ['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)), ['BEGIN', 'COMMIT']);
});

test('weist unbekannte Zahlungsarten und widersprüchliche Summen hart zurück', async () => {
  const source = await payload();
  source.report.paymentRows[0].label = 'Unbekannt';
  const validated = validatePayload(source);
  assert.throws(() => calculateDailyValues(validated.report.paymentRows, new Map([
    ['Kartenzahlung', { mapping_role: 'EC_CASH', provider_code: null }],
    ['Mastercard', { mapping_role: 'CREDIT_PROVIDER', provider_code: 'MASTERCARD' }],
    ['Visa', { mapping_role: 'CREDIT_PROVIDER', provider_code: 'VISA' }],
  ])), /nicht freigegeben/);
  source.report.totals.sollCents += 1;
  assert.throws(() => validatePayload(source), /Gesamtsumme/);
});

test('verwirft manipulierte PDF-Bytes vor jeder DB-Verbindung', async () => {
  const source = await payload();
  source.sourcePdfBase64 = Buffer.from('%PDF-not-really-a-pdf').toString('base64');
  source.sourceSizeBytes = Buffer.from('%PDF-not-really-a-pdf').length;
  source.sourceSha256 = crypto.createHash('sha256').update(Buffer.from('%PDF-not-really-a-pdf')).digest('hex');
  await assert.rejects(
    importEuroControlReport({ pool: { connect: async () => { throw new Error('must not connect'); } }, payload: source }),
    /kein lesbares PDF/,
  );
});
