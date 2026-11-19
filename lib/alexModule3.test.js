const test = require('node:test');
const assert = require('node:assert/strict');
const { buildAlex3Month, manualStatus } = require('./alexModule3');

test('shows Qlik and day-close values immediately and closes only after receipt verification', () => {
  const result = buildAlex3Month({
    year: 2026,
    month: 2,
    branches: ['Telgte', 'Vreden'],
    businessDays: [{
      business_date: '2026-02-03', filiale: 'Vreden', business_day_status: 'final',
      bab_safe_skim_cents: 5000, bab_booked_cents: 25000,
      confirmed_closing_count: 2, linked_closing_count: 2,
    }],
    qlikValues: [{
      qlik_derived_value_id: 'qlik-1',
      business_date: '2026-02-03', filiale: 'Vreden', metric_code: 'BAR_AN_BANK_QLIK',
      numeric_value: 20000, availability_status: 'VORHANDEN', quality_status: 'PLAUSIBEL',
      qlik_confirmed_at: null,
    }],
  });

  const ez = result.days[2].movements[0].cells;
  assert.equal(ez.Telgte.sourceMode, 'AUTOMATISCH');
  assert.equal(ez.Telgte.status, 'OFFEN');
  assert.equal(ez.Vreden.sourceMode, 'AUTOMATISCH');
  assert.equal(ez.Vreden.editable, false);
  assert.equal(ez.Vreden.status, 'ABGESCHLOSSEN');
  assert.deepEqual(ez.Vreden.valueStates, { soll: 'OFFEN', ist: 'BESTAETIGT' });
  assert.equal(ez.Vreden.evidence.differenceCents, 5000);
  assert.equal(ez.Vreden.evidence.plausible, true);
  assert.equal(ez.Vreden.evidence.receiptVerified, true);
});

test('escalates an implausible difference before the deposit receipt exists', () => {
  const result = buildAlex3Month({
    year: 2026,
    month: 1,
    branches: ['Vreden'],
    businessDays: [{
      business_date: '2026-01-02', filiale: 'Vreden', business_day_status: 'final',
      bab_safe_skim_cents: 5000, bab_booked_cents: 10000,
      confirmed_closing_count: 1, linked_closing_count: 0,
    }],
    qlikValues: [{
      business_date: '2026-01-02', filiale: 'Vreden', metric_code: 'BAR_AN_BANK_QLIK',
      numeric_value: 9500, availability_status: 'VORHANDEN', quality_status: 'PLAUSIBEL',
    }],
  });

  const dayOne = result.days[0].movements[0].cells.Vreden;
  const dayTwo = result.days[1].movements[0].cells.Vreden;
  assert.equal(dayOne.status, 'OFFEN');
  assert.equal(dayTwo.status, 'ALARM');
  assert.equal(dayTwo.valueStates.ist, 'OFFEN');
  assert.equal(dayTwo.evidence.differenceCents, 500);
});

test('keeps a plausible day open until every confirmed closing has a receipt link', () => {
  const result = buildAlex3Month({
    year: 2026,
    month: 1,
    branches: ['Vreden'],
    businessDays: [{
      business_date: '2026-01-04', filiale: 'Vreden', business_day_status: 'final',
      bab_safe_skim_cents: 0, bab_booked_cents: 10000,
      confirmed_closing_count: 2, linked_closing_count: 1,
    }],
    qlikValues: [{
      business_date: '2026-01-04', filiale: 'Vreden', metric_code: 'BAR_AN_BANK_QLIK',
      numeric_value: 10000, qlik_confirmed_at: '2026-01-05T08:00:00Z', qlik_confirmed_by: 'Mirko',
    }],
  });
  const ez = result.days[3].movements[0].cells.Vreden;
  assert.equal(ez.status, 'PLAUSIBEL_BELEG_OFFEN');
  assert.deepEqual(ez.valueStates, { soll: 'BESTAETIGT', ist: 'OFFEN' });
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
