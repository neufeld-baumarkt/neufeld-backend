const test = require('node:test');
const assert = require('node:assert/strict');
const { PGlite } = require('@electric-sql/pglite');
const { amountSearchQuery, buildSqlAmountSuggestions } = require('./cashflowAmountSearchSql');
const { buildAmountSuggestions } = require('./cashflowAmountSearch');

// Real PostgreSQL engine in memory. No application pool or environment credentials.
test('SQL amount candidates preserve the full-scan results', async (t) => {
  const db = new PGlite();
  t.after(() => db.close());
  await db.exec(`
    CREATE SCHEMA cashflow;
    CREATE TABLE cashflow.kategorien (id integer PRIMARY KEY, name text, typ text, aktiv boolean);
    CREATE TABLE cashflow.buchungen (
      id integer PRIMARY KEY, kategorie_id integer, betrag numeric(12,2), zahlbetrag numeric(12,2),
      avis_nummer text, rechnungsnummer text, status text, eintrag_typ text,
      jahr integer, kw integer, datum date, tag text, filiale text, notiz text,
      rechnungsdatum date, zahlungsart text, buchungsgruppe_id text,
      rechnungsbetrag_brutto numeric(12,2), skonto_prozent numeric(4,2), skonto_genutzt boolean,
      zahlungsdatum date, erstellt_von text, erstellt_am timestamp, geaendert_am timestamp
    );
    INSERT INTO cashflow.kategorien VALUES
      (1, 'Ausgaben', 'Ausgabe', true), (2, 'Inaktiv', 'Ausgabe', false),
      (3, 'Einnahmen', 'Einnahme', true);
  `);
  const fixtures = [
    // Actual payment overrides plan; null falls back, zero does not.
    [1, '98', '100', null, 'gebucht'],
    [2, '40', null, ' AV-1 ', 'angekuendigt'],
    [3, '60', null, 'AV-1', 'gebucht'],
    // Invoice matches, but its complete Avis does not (no false partial Avis).
    [4, '100', null, 'TOO-LARGE', 'gebucht'],
    [5, '1', null, 'TOO-LARGE', 'gebucht'],
    [6, '100', '0', 'AV-1', 'gebucht'],
    [7, '-10', null, 'AV-1', 'gebucht'],
    [8, '100', null, 'SINGLE', 'gebucht'],
    [9, '100', null, '  ', 'angekuendigt'],
    [10, '0.10', null, '\t CENT \u00a0', 'angekuendigt'],
    [11, '0.20', null, 'CENT', 'angekuendigt'],
    [12, '100', null, 'AV-1', 'gebucht', 2],
    [13, '100', null, 'AV-1', 'gebucht', 3],
    [14, '100', null, 'AV-1', 'gebucht', 1, 'feiertag'],
    // Same Avis deliberately spans branches/years: preserve global grouping.
    [15, '0.01', null, 'CROSS-YEAR', 'gebucht'],
    [16, '99.99', null, 'CROSS-YEAR', 'gebucht'],
  ];
  for (const [id, amount, paid, avis, status, category = 1, type = null] of fixtures) {
    await db.query(`INSERT INTO cashflow.buchungen
      (id, betrag, zahlbetrag, avis_nummer, status, kategorie_id, eintrag_typ, rechnungsnummer, jahr, filiale)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [id, amount, paid, avis, status, category, type, `R-${id}`, id % 2 ? 2025 : 2026, id % 2 ? 'Vreden' : 'Telgte']);
  }
  // Unrelated data must not cross the DB/application boundary.
  await db.exec(`INSERT INTO cashflow.buchungen (id, kategorie_id, betrag, avis_nummer)
    SELECT n, 1, 17.23, 'OTHER-' || (n / 3) FROM generate_series(1000, 10999) n`);
  await db.exec('ANALYZE cashflow.buchungen; ANALYZE cashflow.kategorien');
  const baseline = await db.query(`SELECT b.*, k.name AS kategorie, k.typ
    FROM cashflow.buchungen b JOIN cashflow.kategorien k ON k.id = b.kategorie_id
    WHERE k.aktiv = true AND k.typ = 'Ausgabe'
      AND COALESCE(b.eintrag_typ, 'betrag') = 'betrag'
      AND COALESCE(b.zahlbetrag, b.betrag) > 0
    ORDER BY b.avis_nummer NULLS LAST, b.rechnungsdatum NULLS LAST,
             b.rechnungsnummer NULLS LAST, b.id`);
  for (const cents of [10000, 30, 1, 1723, 5169, 999999]) {
    const candidates = await db.query(amountSearchQuery, [cents]);
    const actual = buildSqlAmountSuggestions(candidates.rows, cents);
    assert.deepEqual(actual, buildAmountSuggestions(baseline.rows, cents), `target ${cents}`);
    assert.ok(candidates.rows.every((row) => row.amount_avis_match ||
      Math.round(Number(row.zahlbetrag ?? row.betrag) * 100) === cents));
    if (cents === 10000) {
      assert.equal(candidates.rows.length, 8);
      const explanation = await db.query(
        `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${amountSearchQuery}`, [cents]);
      const plan = explanation.rows[0]['QUERY PLAN'][0];
      assert.equal(plan.Plan['Actual Rows'], 8);
      const checkAggregates = (node) => {
        if (node['Node Type'] === 'Aggregate') assert.equal(node['Actual Loops'], 1);
        for (const child of node.Plans || []) checkAggregates(child);
      };
      checkAggregates(plan.Plan);
      if (process.env.CASHFLOW_TEST_EXPLAIN === '1') t.diagnostic(JSON.stringify(plan));
      t.diagnostic(`Eligible rows: ${baseline.rows.length}; returned details: 8; synthetic SQL execution: ${plan['Execution Time']} ms`);
      assert.ok(!actual.avis.some((avis) => avis.avis_nummer === 'TOO-LARGE'));
      assert.ok(actual.rechnungen.some((row) => row.id === 4));
      assert.ok(actual.avis.some((avis) => avis.avis_nummer === 'SINGLE'));
    }
  }
});
