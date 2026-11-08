const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { CUSTOMER_BRANCHES, importQlikCsv, validateFileName } = require('./qlikImport');

const payload = Buffer.from([
  '\uFEFFDatum;KundeNr;Zahlart;Zahlbetrag',
  '02.01.2099;041117;Umsatz;10,00',
  '02.01.2099;041117;Gegenbuchung;-10,00',
  '02.01.2099;041129;Umsatz;20,00',
  '02.01.2099;041129;Gegenbuchung;-20,00',
  '02.01.2099;041135;Umsatz;30,00',
  '02.01.2099;041135;Gegenbuchung;-30,00',
  '02.01.2099;041140;Umsatz;40,00',
  '02.01.2099;041140;Gegenbuchung;-40,00',
].join('\r\n'), 'utf8');

function fakePool(handler) {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      return handler(sql, params, calls);
    },
    release() {
      calls.push({ sql: 'RELEASE', params: [] });
    },
  };
  return { calls, connect: async () => client };
}

test('importiert einen geprueften Gesamtexport atomar und centgenau', async () => {
  const pool = fakePool(async (sql) => {
    if (sql.includes('SELECT id,status,report_date')) return { rows: [] };
    if (sql.includes('INSERT INTO controlling.qlik_import_runs')) {
      return { rows: [{ id: 'run-1', status: 'VALIDATED', report_date: '2099-01-02' }] };
    }
    if (sql.includes('UPDATE controlling.qlik_import_runs')) {
      return { rows: [{ id: 'run-1', status: 'IMPORTED', report_date: '2099-01-02', row_count: 8, customer_count: 4, payment_type_count: 2 }] };
    }
    if (sql.includes('materialize_qlik_derived_daily_values')) return { rows: [{ inserted_values: 32, inserted_sources: 8 }] };
    return { rows: [] };
  });

  const result = await importQlikCsv({ pool, payload, fileName: 'Filialbericht_AlleFilialen_02012099.csv' });

  assert.equal(result.deduplicated, false);
  assert.equal(result.run.status, 'IMPORTED');
  assert.deepEqual(CUSTOMER_BRANCHES, {
    '041117': 'Telgte', '041129': 'Ahaus', '041135': 'Vreden', '041140': 'Münster',
  });
  const rowInserts = pool.calls.filter((call) => call.sql.includes('INSERT INTO controlling.qlik_import_rows'));
  assert.equal(rowInserts.length, 8);
  assert.equal(rowInserts[0].params[6], 1000);
  assert.equal(rowInserts[0].params[4], 'Telgte');
  assert.deepEqual(result.materialized, { inserted_values: 32, inserted_sources: 8 });
  assert.ok(pool.calls.find((call) => call.sql.includes('materialize_qlik_derived_daily_values')));
  assert.deepEqual(pool.calls.map((call) => call.sql).filter((sql) => ['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)), ['BEGIN', 'COMMIT']);
});

test('erkennt denselben Dateiinhalt am Hash ohne zweite Rohzeilen', async () => {
  const expectedHash = crypto.createHash('sha256').update(payload).digest('hex');
  const pool = fakePool(async (sql, params) => {
    if (sql.includes('SELECT id,status,report_date')) {
      assert.equal(params[0], expectedHash);
      return { rows: [{ id: 'run-existing', status: 'IMPORTED', report_date: '2099-01-02' }] };
    }
    if (sql.includes('materialize_qlik_derived_daily_values')) return { rows: [{ inserted_values: 0, inserted_sources: 0 }] };
    return { rows: [] };
  });

  const result = await importQlikCsv({ pool, payload, fileName: 'again.csv' });

  assert.equal(result.deduplicated, true);
  assert.equal(pool.calls.some((call) => call.sql.includes('INSERT INTO controlling.qlik_import_rows')), false);
  assert.equal(pool.calls.some((call) => call.sql.includes('materialize_qlik_derived_daily_values')), true);
  assert.deepEqual(pool.calls.map((call) => call.sql).filter((sql) => ['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)), ['BEGIN', 'COMMIT']);
});

test('rollt bei einem Zeilenfehler den gesamten Import zurueck', async () => {
  let rowInsertCount = 0;
  const pool = fakePool(async (sql) => {
    if (sql.includes('SELECT id,status,report_date')) return { rows: [] };
    if (sql.includes('INSERT INTO controlling.qlik_import_runs')) {
      return { rows: [{ id: 'run-1', status: 'VALIDATED', report_date: '2099-01-02' }] };
    }
    if (sql.includes('INSERT INTO controlling.qlik_import_rows')) {
      rowInsertCount += 1;
      if (rowInsertCount === 3) throw new Error('synthetic database failure');
    }
    return { rows: [] };
  });

  await assert.rejects(
    importQlikCsv({ pool, payload, fileName: 'test.csv' }),
    /synthetic database failure/,
  );
  assert.deepEqual(pool.calls.map((call) => call.sql).filter((sql) => ['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)), ['BEGIN', 'ROLLBACK']);
});

test('weist fehlende Filialen und Pfad-Dateinamen vor jeder DB-Verbindung ab', async () => {
  const incomplete = Buffer.from(payload.toString('utf8').split('\r\n').filter((line) => !line.includes('041140')).join('\r\n'));
  const pool = { connect: async () => { throw new Error('must not connect'); } };
  await assert.rejects(importQlikCsv({ pool, payload: incomplete, fileName: 'test.csv' }), /KundeNr-Menge/);
  assert.throws(() => validateFileName('../test.csv'), /Dateiname/);
});
