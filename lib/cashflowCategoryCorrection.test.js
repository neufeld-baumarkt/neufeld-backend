const test = require('node:test');
const assert = require('node:assert/strict');

const { validateCategoryCorrection } = require('./cashflowCategoryCorrection');

test('erlaubt die Korrektur zwischen zwei Ausgabenkategorien', () => {
  assert.equal(validateCategoryCorrection({
    currentCategoryId: 3,
    targetCategoryId: 2,
    currentType: 'Ausgabe',
    targetType: 'Ausgabe',
  }), '');
});

test('verhindert eine Umklassifizierung zwischen Einnahme und Ausgabe', () => {
  assert.match(validateCategoryCorrection({
    currentCategoryId: 1,
    targetCategoryId: 2,
    currentType: 'Einnahme',
    targetType: 'Ausgabe',
  }), /Buchungstyps/);
});
