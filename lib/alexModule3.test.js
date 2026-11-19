const test = require('node:test');
const assert = require('node:assert/strict');
const { buildAlex3Month, manualStatus } = require('./alexModule3');

test('keeps classic branches manual while Vreden EZ uses the complete automatic chain', () => {
  const result = buildAlex3Month({
    year: 2026,
    month: 2,
    branches: ['Telgte', 'Vreden'],
    manualRows: [{
      id: 'manual-1', business_date: '2026-02-03', filiale: 'Telgte', movement_type: 'EZ',
      soll_cents: 12300, ist_cents: 12300, note: 'Altbestand',
    }],
    businessDays: [{
      business_date: '2026-02-03', filiale: 'Vreden', business_day_status: 'final',
      bab_rounded_cents: 20000, bab_safe_skim_cents: 5000, bab_booked_cents: 25000,
    }],
    receipts: [{ business_date: '2026-02-03', filiale: 'Vreden', receipt_total_cents: 25000, receipt_count: 1 }],
    qlikValues: [{
      business_date: '2026-02-03', filiale: 'Vreden', metric_code: 'BAR_AN_BANK_QLIK',
      numeric_value: 20000, availability_status: 'VORHANDEN', quality_status: 'PLAUSIBEL',
    }],
  });

  const ez = result.days[2].movements[0].cells;
  assert.equal(ez.Telgte.sourceMode, 'MANUELL');
  assert.equal(ez.Telgte.status, 'PASST');
  assert.equal(ez.Vreden.sourceMode, 'AUTOMATISCH');
  assert.equal(ez.Vreden.editable, false);
  assert.equal(ez.Vreden.status, 'AUTOMATISCH_GRUEN');
  assert.deepEqual(ez.Vreden.evidence.checks, {
    qlikMatchesRounded: true,
    bookedMatchesChain: true,
    receiptMatchesBooked: true,
  });
});

test('marks incomplete and contradictory automatic chains explicitly', () => {
  const result = buildAlex3Month({
    year: 2026,
    month: 1,
    branches: ['Vreden'],
    businessDays: [{
      business_date: '2026-01-02', filiale: 'Vreden', business_day_status: 'final',
      bab_rounded_cents: 10000, bab_safe_skim_cents: 0, bab_booked_cents: 10000,
    }],
    qlikValues: [{
      business_date: '2026-01-02', filiale: 'Vreden', metric_code: 'BAR_AN_BANK_QLIK',
      numeric_value: 9500, availability_status: 'VORHANDEN', quality_status: 'PLAUSIBEL',
    }],
  });

  const dayOne = result.days[0].movements[0].cells.Vreden;
  const dayTwo = result.days[1].movements[0].cells.Vreden;
  assert.equal(dayOne.status, 'OFFEN');
  assert.equal(dayTwo.status, 'ABWEICHUNG');
});

test('exposes the AZ Qlik value only as a suggestion for human review', () => {
  const result = buildAlex3Month({
    year: 2026,
    month: 1,
    branches: ['Ahaus'],
    manualRows: [{
      business_date: '2026-01-05', filiale: 'Ahaus', movement_type: 'AZ',
      soll_cents: 2492, ist_cents: 2492,
    }],
    qlikValues: [{
      business_date: '2026-01-05', filiale: 'Ahaus', metric_code: 'KASSENAUSZAHLUNGEN',
      numeric_value: 2492, availability_status: 'VORHANDEN', quality_status: 'PLAUSIBEL',
    }],
  });
  const az = result.days[4].movements[1].cells.Ahaus;
  assert.equal(az.status, 'PASST');
  assert.equal(az.evidence.qlikSuggestionCents, 2492);
  assert.equal(az.evidence.externalReceiptRequired, true);
});

test('manual status distinguishes blank, partial, matching and different values', () => {
  assert.equal(manualStatus(null, null), 'OFFEN');
  assert.equal(manualStatus(100, null), 'UNVOLLSTAENDIG');
  assert.equal(manualStatus(100, 100), 'PASST');
  assert.equal(manualStatus(100, 99), 'ABWEICHUNG');
});
