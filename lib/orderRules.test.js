'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  allocateCents,
  calculateOrderPlan,
  isValidIsoDate,
  normalizeMellerudArticleIdentity,
  normalizeMellerudArticleMaster,
  normalizeSupplierCode,
  supportsMellerudWorkflow,
} = require('./orderRules');

const articles = [
  { id: 'a', supplier_article_no: 'MEL-A', kunden_art_nr: 'NF-A', ve_stueck: 6, ek_pro_karton: '12.00' },
  { id: 'b', supplier_article_no: 'MEL-B', kunden_art_nr: 'NF-B', ve_stueck: 5, ek_pro_karton: '10.01' },
];
const branches = ['Ahaus', 'Münster', 'Telgte', 'Vreden'];

test('normalizes supplier codes and validates real calendar dates', () => {
  assert.equal(normalizeSupplierCode(' MELLERUD '), 'mellerud');
  assert.equal(isValidIsoDate('2026-02-28'), true);
  assert.equal(isValidIsoDate('2026-02-30'), false);
  assert.equal(isValidIsoDate('2026-13-01'), false);
});

test('accepts the established Mellerud form type for the binding workflow', () => {
  assert.equal(supportsMellerudWorkflow({ code: 'mellerud', formular_typ: 'mell_classic' }), true);
  assert.equal(supportsMellerudWorkflow({ code: 'MELLERUD', formular_typ: 'mellerud' }), true);
  assert.equal(supportsMellerudWorkflow({ code: 'mellerud', formular_typ: 'other' }), false);
  assert.equal(supportsMellerudWorkflow({ code: 'other', formular_typ: 'mell_classic' }), false);
});

test('requires the hard-linked Mellerud and Neufeld article identities', () => {
  assert.deepEqual(normalizeMellerudArticleIdentity({
    supplier_article_no: ' 2001005047 ',
    kunden_art_nr: ' NF-4711 ',
  }), {
    supplier_article_no: '2001005047',
    kunden_art_nr: 'NF-4711',
  });
  assert.throws(() => normalizeMellerudArticleIdentity({ kunden_art_nr: 'NF-4711' }), /Mellerud-Art.-Nr/);
  assert.throws(() => normalizeMellerudArticleIdentity({ supplier_article_no: '2001005047' }), /Neufeld-Art.-Nr/);
});

test('validates and normalizes complete Mellerud article master data', () => {
  assert.deepEqual(normalizeMellerudArticleMaster({
    supplier_article_no: ' 2001005047 ', kunden_art_nr: ' NF-4711 ',
    ean: ' 4004666005047 ', name: ' Schimmel Schutz 0,5l ', ve_stueck: '6',
    ek_einzel: '2.1234', ek_pro_karton: '12.74', sort_index: '20',
  }), {
    supplier_article_no: '2001005047', kunden_art_nr: 'NF-4711',
    ean: '4004666005047', name: 'Schimmel Schutz 0,5l', ve_stueck: 6,
    ek_einzel: 2.1234, ek_pro_karton: 12.74, sort_index: 20,
  });
  assert.throws(() => normalizeMellerudArticleMaster({
    supplier_article_no: 'M-1', kunden_art_nr: 'N-1', ean: 'abc', name: 'Test',
    ve_stueck: 1, ek_einzel: 1, ek_pro_karton: 1,
  }), /EAN/);
});

test('requires at least two whole VE across the complete order', () => {
  assert.throws(() => calculateOrderPlan({
    articles,
    requestedPositions: [{ articleId: 'a', menge_kartons: 1 }],
    splitDetails: {}, sourceFiliale: 'Ahaus', validBranches: branches,
  }), /mindestens 2 VE/);

  const result = calculateOrderPlan({
    articles,
    requestedPositions: [
      { articleId: 'a', menge_kartons: 1 },
      { articleId: 'b', menge_kartons: 1 },
    ],
    splitDetails: {}, sourceFiliale: 'Ahaus', validBranches: branches,
  });
  assert.equal(result.totalVe, 2);
  assert.equal(result.totalNet, 22.01);
});

test('splits pieces internally while the supplier order remains whole VE', () => {
  const result = calculateOrderPlan({
    articles,
    requestedPositions: [{ articleId: 'a', menge_kartons: 2 }],
    splitDetails: {
      a: { zeilen: [
        { target_filiale: 'Münster', einheit: 'stueck', menge: 4 },
        { target_filiale: 'Telgte', einheit: 'stueck', menge: 3 },
      ] },
    },
    sourceFiliale: 'Ahaus', validBranches: branches,
  });

  assert.equal(result.positions[0].quantityVe, 2);
  assert.deepEqual(result.budgetSplits, [
    { filiale: 'Münster', betrag: 8 },
    { filiale: 'Telgte', betrag: 6 },
  ]);
  assert.equal(result.splitSnapshot.artikel.a.rest.menge_stueck, 5);
  assert.equal(result.splitSnapshot.rest.betrag, 10);
});

test('allows every ordered piece to be assigned to other branches', () => {
  const result = calculateOrderPlan({
    articles,
    requestedPositions: [{ articleId: 'a', menge_kartons: 2 }],
    splitDetails: {
      a: { zeilen: [
        { target_filiale: 'Münster', einheit: 'karton', menge: 1 },
        { target_filiale: 'Telgte', einheit: 'stueck', menge: 6 },
      ] },
    },
    sourceFiliale: 'Ahaus', validBranches: branches,
  });

  assert.equal(result.sourceNet, 0);
  assert.deepEqual(result.budgetSplits, [
    { filiale: 'Münster', betrag: 12 },
    { filiale: 'Telgte', betrag: 12 },
  ]);
  assert.equal(result.splitSnapshot.artikel.a.rest.menge_stueck, 0);
});

test('cent allocation always conserves the exact order total', () => {
  const allocations = allocateCents(1001, [
    { key: 'A', units: 1 }, { key: 'B', units: 1 }, { key: 'C', units: 3 },
  ]);
  assert.equal(allocations.reduce((sum, item) => sum + item.cents, 0), 1001);
  assert.deepEqual(allocations.map((item) => item.cents), [200, 200, 601]);
});

test('rejects invalid split targets, duplicates and over-allocation', () => {
  const base = {
    articles,
    requestedPositions: [{ articleId: 'a', menge_kartons: 2 }],
    sourceFiliale: 'Ahaus', validBranches: branches,
  };
  assert.throws(() => calculateOrderPlan({ ...base, splitDetails: {
    a: { zeilen: [{ target_filiale: 'Ahaus', einheit: 'stueck', menge: 1 }] },
  } }), /verschieden/);
  assert.throws(() => calculateOrderPlan({ ...base, splitDetails: {
    a: { zeilen: [
      { target_filiale: 'Münster', einheit: 'stueck', menge: 1 },
      { target_filiale: 'Münster', einheit: 'stueck', menge: 1 },
    ] },
  } }), /doppelt/);
  assert.throws(() => calculateOrderPlan({ ...base, splitDetails: {
    a: { zeilen: [{ target_filiale: 'Münster', einheit: 'karton', menge: 3 }] },
  } }), /überschreitet/);
});
