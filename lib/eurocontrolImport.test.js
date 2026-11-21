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
    { rowNumber: 1, label: 'BAR', sollCents: 1000, istCents: 1000, differenceCents: -5, withdrawalCents: 0 },
    { rowNumber: 2, label: 'Kartenzahlung', sollCents: 2100, istCents: 2200, differenceCents: 100, withdrawalCents: 0 },
    { rowNumber: 3, label: 'Mastercard', sollCents: 250, istCents: 300, differenceCents: 50, withdrawalCents: 0 },
    { rowNumber: 4, label: 'Visa', sollCents: 350, istCents: 400, differenceCents: 50, withdrawalCents: 0 },
  ];
  return {
    fileName: '20990102_41129_filialbericht.pdf',
    sourceSha256: crypto.createHash('sha256').update(source).digest('hex'),
    sourceSizeBytes: source.length,
    sourcePdfBase64: source.toString('base64'),
    report: {
      reportType: 'FILIALBERICHT', branchIdentifier: '041129', branchLabel: 'Synthetic A',
      reportDate: '2099-01-02', pageCount: 1, reportVersion: 'test', reportCreatedAt: null,
      parserVersion: '2.0.0',
      paymentRows,
      totals: { sollCents: 3700, istCents: 3900, differenceCents: 195, withdrawalCents: 0 },
      reportFields: [
        { fieldCode: 'REPORT_REVENUE_CENTS', valueKind: 'MONEY_CENTS', amountCents: 10000, pageNumber: 1, explicit: true },
        { fieldCode: 'PAYOUTS_CENTS', valueKind: 'MONEY_CENTS', amountCents: 50, pageNumber: 1, explicit: true },
        { fieldCode: 'CUSTOMER_COUNT', valueKind: 'COUNT', countValue: 4, pageNumber: 1, explicit: true },
        { fieldCode: 'AVERAGE_RECEIPT_CENTS', valueKind: 'MONEY_CENTS', amountCents: 2500, pageNumber: 1, explicit: true },
        { fieldCode: 'BANK_DEPOSIT_CENTS', valueKind: 'MONEY_CENTS', amountCents: 950, pageNumber: 1, explicit: true },
        { fieldCode: 'VAT_7_GROSS_CENTS', valueKind: 'MONEY_CENTS', amountCents: 3000, pageNumber: 1, explicit: true },
        { fieldCode: 'VAT_19_GROSS_CENTS', valueKind: 'MONEY_CENTS', amountCents: 7000, pageNumber: 1, explicit: true },
      ],
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
  const calculated = calculateDailyValues(parsed.report.paymentRows, mappings, parsed.report.reportFields);
  assert.equal(calculated.ecCashCents, 2200);
  assert.equal(calculated.creditCardCents, 700);
  assert.deepEqual(calculated.providerBreakdown, {
    MASTERCARD: 300, VISA: 400, AMEX: 0, MAESTRO: 0,
  });
  assert.equal(calculated.values.length, 8);
  assert.equal(calculated.values.find((value) => value.metricCode === 'KASSENDIFFERENZ').numericValue, -5);
  assert.equal(calculated.values.find((value) => value.metricCode === 'GESAMTUMSATZ_QLIK').numericValue, 10000);
});

test('importiert Rohquelle, Einzelzeilen und Fachwerte in genau einer Transaktion', async () => {
  const syncCalls = [];
  const pool = fakePool(async (sql) => {
    if (sql.includes('WHERE imported.source_sha256')) return { rows: [] };
    if (sql.includes('branch_source_identifiers')) return { rowCount: 1, rows: [{ branch_id: 2, branch_name: 'Synthetic A' }] };
    if (sql.includes('eurocontrol_payment_mappings')) return { rows: [
      { external_label: 'BAR', mapping_role: 'IGNORE', provider_code: null },
      { external_label: 'Kartenzahlung', mapping_role: 'EC_CASH', provider_code: null },
      { external_label: 'Mastercard', mapping_role: 'CREDIT_PROVIDER', provider_code: 'MASTERCARD' },
      { external_label: 'Visa', mapping_role: 'CREDIT_PROVIDER', provider_code: 'VISA' },
    ] };
    if (sql.includes('FROM controlling.eurocontrol_derived_daily_values')) return { rows: [] };
    if (sql.includes('INSERT INTO controlling.eurocontrol_imports')) return { rows: [{ id: 'import-1', status: 'IMPORTED', report_date: '2099-01-02', source_sha256: 'a'.repeat(64) }] };
    return { rows: [] };
  });
  const source = await payload();
  const result = await importEuroControlReport({
    pool,
    payload: source,
    synchronizePaymentDays: async (client, period) => {
      syncCalls.push({ client, period, callCount: pool.calls.length });
      return { synchronized: 2 };
    },
  });
  assert.equal(result.ec_cash_cents, 2200);
  assert.equal(result.credit_card_cents, 700);
  assert.deepEqual(result.alexModule2, { synchronized: 2 });
  assert.deepEqual(syncCalls.map((call) => call.period), [{ jahr: 2099, monat: 1 }]);
  assert.equal(pool.calls.filter((call) => call.sql.includes('eurocontrol_import_payment_rows')).length, 4);
  assert.equal(pool.calls.filter((call) => call.sql.includes('eurocontrol_import_fields')).length, 7);
  assert.equal(pool.calls.filter((call) => call.sql.includes('eurocontrol_derived_daily_values')).length, 9);
  assert.equal(pool.calls.filter((call) => call.sql.includes('INSERT INTO controlling.eurocontrol_daily_values')).length, 1);
  const importCall = pool.calls.find((call) => call.sql.includes('INSERT INTO controlling.eurocontrol_imports'));
  assert.match(importCall.sql, /VALUES \(\$1,\$2,\$3,\$4,\$5,NULL/);
  assert.deepEqual(pool.calls.map((call) => call.sql).filter((sql) => ['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)), ['BEGIN', 'COMMIT']);
  assert.equal(syncCalls[0].callCount, pool.calls.findIndex((call) => call.sql === 'COMMIT'));
});

test('ergänzt bereits importierte PDF-Hashes idempotent um die kanonischen Fachwerte', async () => {
  const syncCalls = [];
  const pool = fakePool(async (sql) => {
    if (sql.includes('WHERE imported.source_sha256')) return { rows: [{
      id: 'legacy-import', status: 'IMPORTED', report_date: '2099-01-02',
      source_sha256: 'a'.repeat(64), branch_name: 'Synthetic A',
      ec_cash_cents: 2200, credit_card_cents: 700,
    }] };
    if (sql.includes('branch_source_identifiers')) return { rowCount: 1, rows: [{ branch_id: 2, branch_name: 'Synthetic A' }] };
    if (sql.includes('eurocontrol_payment_mappings')) return { rows: [
      { external_label: 'BAR', mapping_role: 'IGNORE', provider_code: null },
      { external_label: 'Kartenzahlung', mapping_role: 'EC_CASH', provider_code: null },
      { external_label: 'Mastercard', mapping_role: 'CREDIT_PROVIDER', provider_code: 'MASTERCARD' },
      { external_label: 'Visa', mapping_role: 'CREDIT_PROVIDER', provider_code: 'VISA' },
    ] };
    if (sql.includes('SELECT metric_code, numeric_value')) return { rows: [] };
    if (sql.includes('AS field_count')) return { rows: [{ field_count: '0', derived_count: '0' }] };
    return { rows: [] };
  });
  const result = await importEuroControlReport({
    pool,
    payload: await payload(),
    synchronizePaymentDays: async (_client, period) => {
      syncCalls.push({ period, callCount: pool.calls.length });
      return { synchronized: 2 };
    },
  });
  assert.equal(result.deduplicated, true);
  assert.equal(result.upgraded, true);
  assert.deepEqual(result.alexModule2, { synchronized: 2 });
  assert.deepEqual(syncCalls.map((call) => call.period), [{ jahr: 2099, monat: 1 }]);
  assert.equal(pool.calls.filter((call) => call.sql.includes('INSERT INTO controlling.eurocontrol_import_fields')).length, 7);
  assert.equal(pool.calls.filter((call) => call.sql.includes('INSERT INTO controlling.eurocontrol_derived_daily_values')).length, 8);
  assert.equal(pool.calls.filter((call) => call.sql.includes('INSERT INTO controlling.eurocontrol_imports (')).length, 0);
  assert.equal(pool.calls.filter((call) => call.sql.includes('INSERT INTO controlling.eurocontrol_import_payment_rows')).length, 0);
  assert.equal(pool.calls.filter((call) => call.sql.includes('INSERT INTO controlling.eurocontrol_daily_values')).length, 0);
  assert.equal(pool.calls.filter((call) => call.sql.includes('DUPLICATE_CANONICAL_BACKFILL')).length, 1);
  assert.deepEqual(pool.calls.map((call) => call.sql).filter((sql) => ['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)), ['BEGIN', 'COMMIT']);
  assert.equal(syncCalls[0].callCount, pool.calls.findIndex((call) => call.sql === 'COMMIT'));
});

test('weist unbekannte Zahlungsarten und widersprüchliche Summen hart zurück', async () => {
  const source = await payload();
  source.report.paymentRows[0].label = 'Unbekannt';
  const validated = validatePayload(source);
  assert.throws(() => calculateDailyValues(validated.report.paymentRows, new Map([
    ['Kartenzahlung', { mapping_role: 'EC_CASH', provider_code: null }],
    ['Mastercard', { mapping_role: 'CREDIT_PROVIDER', provider_code: 'MASTERCARD' }],
    ['Visa', { mapping_role: 'CREDIT_PROVIDER', provider_code: 'VISA' }],
  ]), validated.report.reportFields), /nicht freigegeben/);
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
