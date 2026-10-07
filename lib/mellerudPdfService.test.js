'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildFormRows } = require('../services/pdf/mellerudPdfService');

test('supplier PDF rows use the snapshotted Mellerud number, not the Neufeld number', () => {
  const [row] = buildFormRows([{
    ean_snapshot: '4004666005047',
    supplier_article_no_snapshot: '2001005047',
    kunden_art_nr_snapshot: 'NF-4711',
    name_snapshot: 'Schimmel Schutz 0,5l',
    ve_stueck_snapshot: 6,
    ek_einzel_snapshot: 5.04,
    ek_pro_karton_snapshot: 30.24,
    menge_kartons: 1,
  }]);

  assert.equal(row.ean, '4004666005047');
  assert.equal(row.supplierArticleNo, '2001005047');
  assert.equal(Object.hasOwn(row, 'kundenArtNr'), false);
});
