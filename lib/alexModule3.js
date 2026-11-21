const ALEX3_MOVEMENT_TYPES = Object.freeze(['EZ', 'AZ']);
const ALEX3_QLIK_METRICS = Object.freeze(['BAR_AN_BANK_QLIK', 'KASSENAUSZAHLUNGEN']);

function dateOnly(value) {
  if (!value) return null;
  if (typeof value === 'string') return value.slice(0, 10);
  return value.toISOString().slice(0, 10);
}

function key(date, branch) {
  return `${dateOnly(date)}|${String(branch || '').toLowerCase()}`;
}

function manualKey(date, branch, movementType) {
  return `${key(date, branch)}|${movementType}`;
}

function qlikKey(date, branch, metricCode) {
  return `${key(date, branch)}|${metricCode}`;
}

function cents(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function manualStatus(sollCents, istCents) {
  if (sollCents === null && istCents === null) return 'OFFEN';
  if (sollCents === null || istCents === null) return 'UNVOLLSTAENDIG';
  return sollCents === istCents ? 'PASST' : 'ABWEICHUNG';
}

function qlikSignal(row) {
  if (!row) return {
    id: null,
    valueCents: null,
    availabilityStatus: 'FEHLT',
    qualityStatus: null,
    confirmed: false,
    confirmedAt: null,
    confirmedBy: null,
  };
  return {
    id: row.qlik_derived_value_id || row.id || null,
    valueCents: cents(row.numeric_value),
    availabilityStatus: row.availability_status || 'UNVOLLSTAENDIG',
    qualityStatus: row.quality_status || null,
    selectedSource: row.selected_source || 'QLIK',
    sourceStatus: row.source_status || 'QLIK_PRIMARY',
    confirmed: Boolean(row.qlik_confirmed_at),
    confirmedAt: row.qlik_confirmed_at || null,
    confirmedBy: row.qlik_confirmed_by || null,
  };
}

function receiptVerified(businessDay) {
  const confirmed = Number(businessDay?.confirmed_closing_count || 0);
  const linked = Number(businessDay?.linked_closing_count || 0);
  return confirmed > 0 && linked === confirmed;
}

function buildAutomaticEz({ businessDay, qlik }) {
  const dayReportCents = cents(businessDay?.bab_booked_cents);
  const confirmedClosingCents = cents(businessDay?.confirmed_closing_bank_cents);
  const bookedCents = dayReportCents ?? confirmedClosingCents;
  const skimCents = cents(businessDay?.bab_safe_skim_cents);
  const qlikCents = qlik.valueCents;
  const differenceCents = qlikCents !== null && bookedCents !== null ? bookedCents - qlikCents : null;
  const exactMatch = differenceCents === 0;
  const expectedBookedCents = qlikCents !== null && skimCents !== null
    ? Math.floor(qlikCents / 500) * 500 + skimCents
    : null;
  const safeSkimMatch = bookedCents !== null && expectedBookedCents !== null
    && bookedCents === expectedBookedCents;
  const plausible = differenceCents === null ? null : exactMatch || safeSkimMatch;
  const istVerified = receiptVerified(businessDay);

  let status = 'OFFEN';
  if (qlikCents === null && bookedCents !== null) status = 'WARTET_AUF_QLIK';
  else if (qlikCents !== null && bookedCents === null) status = 'WARTET_AUF_TAGESABRECHNUNG';
  else if (plausible === false) status = 'ALARM';
  else if (plausible === true && !istVerified) status = 'PLAUSIBEL_BELEG_OFFEN';
  else if (plausible === true && istVerified) status = 'ABGESCHLOSSEN';

  return {
    sourceMode: 'AUTOMATISCH',
    editable: false,
    sollCents: qlikCents,
    istCents: bookedCents,
    status,
    note: null,
    qlik,
    valueStates: {
      soll: qlik.confirmed ? 'BESTAETIGT' : qlikCents === null ? 'FEHLT' : 'OFFEN',
      ist: istVerified ? 'BESTAETIGT' : bookedCents === null ? 'FEHLT' : 'OFFEN',
    },
    evidence: {
      bookedCents,
      expectedBookedCents,
      bookedSource: dayReportCents !== null
        ? 'TAGESABRECHNUNG'
        : confirmedClosingCents !== null ? 'BESTAETIGTE_KASSENABSCHLUESSE' : null,
      differenceCents,
      plausible,
      receiptVerified: istVerified,
      confirmedClosingCount: Number(businessDay?.confirmed_closing_count || 0),
      linkedClosingCount: Number(businessDay?.linked_closing_count || 0),
      businessDayStatus: businessDay?.business_day_status || null,
    },
  };
}

function buildManualCell({ manual, qlik, movementType }) {
  const sollCents = cents(manual?.soll_cents);
  const istCents = cents(manual?.ist_cents);
  return {
    id: manual?.id || null,
    sourceMode: 'MANUELL',
    editable: true,
    sollCents,
    istCents,
    status: manualStatus(sollCents, istCents),
    note: manual?.note || null,
    qlik,
    evidence: movementType === 'AZ'
      ? { qlikSuggestionCents: qlik.valueCents, externalReceiptRequired: true }
      : { rolloutPending: true },
  };
}

function buildAlex3Month({ year, month, branches, manualRows = [], businessDays = [], qlikValues = [] }) {
  const manualByKey = new Map(manualRows.map((row) => [manualKey(row.business_date, row.filiale, row.movement_type), row]));
  const businessDayByKey = new Map(businessDays.map((row) => [key(row.business_date, row.filiale), row]));
  const qlikByKey = new Map(qlikValues.map((row) => [qlikKey(row.business_date, row.filiale, row.metric_code), row]));
  const daysInMonth = new Date(year, month, 0).getDate();
  const days = [];
  const summary = { closed: 0, alarms: 0, receiptOpen: 0, waiting: 0, manualOpen: 0 };

  for (let day = 1; day <= daysInMonth; day += 1) {
    const businessDate = `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    const movements = ALEX3_MOVEMENT_TYPES.map((movementType) => {
      const metricCode = movementType === 'EZ' ? 'BAR_AN_BANK_QLIK' : 'KASSENAUSZAHLUNGEN';
      const cells = {};
      for (const branch of branches) {
        const qlik = qlikSignal(qlikByKey.get(qlikKey(businessDate, branch, metricCode)));
        const cell = movementType === 'EZ'
          ? buildAutomaticEz({
              businessDay: businessDayByKey.get(key(businessDate, branch)),
              qlik,
            })
          : buildManualCell({
              manual: manualByKey.get(manualKey(businessDate, branch, movementType)),
              qlik,
              movementType,
            });

        if (cell.status === 'ABGESCHLOSSEN') summary.closed += 1;
        else if (cell.status === 'ALARM') summary.alarms += 1;
        else if (cell.status === 'PLAUSIBEL_BELEG_OFFEN') summary.receiptOpen += 1;
        else if (cell.sourceMode === 'AUTOMATISCH' && cell.status !== 'OFFEN') summary.waiting += 1;
        else if (cell.status === 'OFFEN' && cell.sourceMode === 'MANUELL'
          && [cell.sollCents, cell.istCents, cell.qlik?.valueCents].some((value) => value !== null)) {
          summary.manualOpen += 1;
        }
        cells[branch] = cell;
      }
      return { movementType, label: movementType, cells };
    });
    days.push({ businessDate, day, movements });
  }

  return {
    ok: true,
    year,
    month,
    branches,
    days,
    summary,
    rollout: {
      automaticEzBranches: branches,
      manualEzBranches: [],
      azMode: 'MANUAL_WITH_QLIK_SIGNAL',
    },
  };
}

module.exports = {
  ALEX3_MOVEMENT_TYPES,
  ALEX3_QLIK_METRICS,
  buildAlex3Month,
  manualStatus,
};
