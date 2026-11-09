const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('crypto');
const express = require('express');
const { createQlikImportRouter, IMPORT_PATH } = require('./qlik');
const { payloadSha256 } = require('../lib/qlikImportSignature');

const secret = 'S'.repeat(32);
const nowMillis = 1791450000 * 1000;
const payload = Buffer.from([
  '\uFEFFDatum;KundeNr;Zahlart;Zahlbetrag',
  '02.01.2099;041117;Umsatz;10,00', '02.01.2099;041117;Gegenbuchung;-10,00',
  '02.01.2099;041129;Umsatz;20,00', '02.01.2099;041129;Gegenbuchung;-20,00',
  '02.01.2099;041135;Umsatz;30,00', '02.01.2099;041135;Gegenbuchung;-30,00',
  '02.01.2099;041140;Umsatz;40,00', '02.01.2099;041140;Gegenbuchung;-40,00',
].join('\r\n'));

function poolFake() {
  const client = {
    async query(sql) {
      if (sql.includes('SELECT id,status,report_date')) return { rows: [] };
      if (sql.includes('INSERT INTO controlling.qlik_import_runs')) return { rows: [{ id: 'run-http', status: 'VALIDATED', report_date: '2099-01-02' }] };
      if (sql.includes('UPDATE controlling.qlik_import_runs')) return { rows: [{ id: 'run-http', status: 'IMPORTED', report_date: '2099-01-02' }] };
      if (sql.includes('materialize_qlik_derived_daily_values')) return { rows: [{ inserted_values: 32, inserted_sources: 8 }] };
      return { rows: [] };
    },
    release() {},
  };
  return {
    connect: async () => client,
    async query(sql) {
      if (sql.includes('qlik_derived_daily_aggregates')) return { rows: [{
        id: 'aggregate-1', metric_code: 'GESAMTUMSATZ_ALLE_FILIALEN_QLIK',
        display_name: 'Gesamtumsatz aller Filialen', value_kind: 'MONEY_CENTS',
        numeric_value: '4006194', availability_status: 'VORHANDEN',
        quality_status: 'NICHT_GEPRUEFT', expected_member_count: '4',
        actual_member_count: '4', rule_version: '1', source_count: '4',
      }] };
      if (sql.includes('WITH candidates')) return { rows: [{ id: 'v1', metric_code: 'GESAMTUMSATZ_QLIK', display_name: 'Gesamtumsatz', value_kind: 'MONEY_CENTS', numeric_value: '961610', availability_status: 'VORHANDEN', quality_status: 'NICHT_GEPRUEFT', rule_version: 1, source_count: 2 }] };
      return { rows: [{ daily_revenue_cents: 961610, eurocontrol_bank_cents: 279507, cash_difference_cents: 7, business_day_status: 'final' }] };
    },
  };
}

async function withServer(work, role = 'Supervisor') {
  const app = express();
  const authenticate = (req, _res, next) => { req.user = { id: 6, role }; next(); };
  app.use('/api/qlik', createQlikImportRouter({ pool: poolFake(), secret, now: () => nowMillis, authenticate }));
  const server = await new Promise((resolve) => {
    const instance = app.listen(0, '127.0.0.1', () => resolve(instance));
  });
  try {
    const address = server.address();
    await work(`http://127.0.0.1:${address.port}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

function headers(body = payload, fileName = 'synthetic.csv') {
  const contentSha256 = payloadSha256(body);
  const timestamp = String(nowMillis / 1000);
  return {
    'content-type': 'text/csv',
    'x-qlik-file-name': fileName,
    'x-qlik-content-sha256': contentSha256,
    'x-qlik-timestamp': timestamp,
    'x-qlik-signature': crypto.createHmac('sha256', secret)
      .update([timestamp, 'POST', IMPORT_PATH, contentSha256, fileName].join('.'))
      .digest('hex'),
  };
}

test('nimmt nur hashgebundene Qlik-CSV-Dateien an und liefert keine Rohdaten zurueck', async () => {
  await withServer(async (baseUrl) => {
    const response = await fetch(`${baseUrl}${IMPORT_PATH}`, { method: 'POST', headers: headers(), body: payload });
    const result = await response.json();
    assert.equal(response.status, 201);
    assert.equal(result.import.rowCount, 8);
    assert.equal(result.import.customerCount, 4);
    assert.equal(result.import.paymentTypeCount, 2);
    assert.equal(result.import.derivedValuesInserted, 32);
    assert.equal(result.import.alexModule1RevenueStatus, 'QLIK_NOT_AVAILABLE');
    assert.equal(JSON.stringify(result).includes('10,00'), false);
  });
});

test('liefert das vollstaendige Alex-1-Tagesaggregat ohne Filial-Rohdaten', async () => {
  await withServer(async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/qlik/aggregate-values?date=2026-11-07&consumer=ALEX_MODULE_1`);
    const result = await response.json();
    assert.equal(response.status, 200);
    assert.equal(result.values[0].metricCode, 'GESAMTUMSATZ_ALLE_FILIALEN_QLIK');
    assert.equal(result.values[0].bookable, true);
    assert.equal(result.values[0].actualMemberCount, 4);
    assert.equal(JSON.stringify(result).includes('raw_fields'), false);
  });
});

test('liefert veredelte Werte nur an Controlling-Rollen und ohne Rohdaten', async () => {
  await withServer(async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/qlik/values?branch=Vreden&date=2026-11-07`);
    const result = await response.json();
    assert.equal(response.status, 200);
    assert.equal(result.values[0].metricCode, 'GESAMTUMSATZ_QLIK');
    assert.equal(result.values[0].valueCents, 961610);
    assert.equal(JSON.stringify(result).includes('raw_fields'), false);
  });
});

test('verweigert Filialrollen den direkten Controlling-Zugriff', async () => {
  await withServer(async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/qlik/values?branch=Vreden&date=2026-11-07`);
    assert.equal(response.status, 403);
  }, 'Filiale');
});

test('weist ungueltige Signaturen und einen manipulierten Dateiinhalt vor dem Import ab', async () => {
  await withServer(async (baseUrl) => {
    const invalidSignature = headers();
    invalidSignature['x-qlik-signature'] = '0'.repeat(64);
    const unauthorized = await fetch(`${baseUrl}${IMPORT_PATH}`, { method: 'POST', headers: invalidSignature, body: payload });
    assert.equal(unauthorized.status, 401);

    const changedPayload = Buffer.concat([payload, Buffer.from('\r\n')]);
    const tampered = await fetch(`${baseUrl}${IMPORT_PATH}`, { method: 'POST', headers: headers(), body: changedPayload });
    assert.equal(tampered.status, 400);
  });
});
