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
  if (!row) return { valueCents: null, availabilityStatus: 'FEHLT', qualityStatus: null };
  return {
    valueCents: cents(row.numeric_value),
    availabilityStatus: row.availability_status || 'UNVOLLSTAENDIG',
    qualityStatus: row.quality_status || null,
  };
}

function buildAutomaticEz({ businessDay, qlik, receipt }) {
  const roundedCents = cents(businessDay?.bab_rounded_cents);
  const bookedCents = cents(businessDay?.bab_booked_cents);
  const skimCents = cents(businessDay?.bab_safe_skim_cents);
  const receiptCents = cents(receipt?.receipt_total_cents);
  const qlikCents = qlik.valueCents;
  const checks = {
    qlikMatchesRounded: qlikCents !== null && roundedCents !== null
      ? qlikCents === roundedCents
      : null,
    bookedMatchesChain: roundedCents !== null && bookedCents !== null && skimCents !== null
      ? bookedCents === roundedCents + skimCents
      : null,
    receiptMatchesBooked: receiptCents !== null && bookedCents !== null
      ? receiptCents === bookedCents
      : null,
  };

  const hasEvidence = [qlikCents, roundedCents, bookedCents, skimCents, receiptCents]
    .some((value) => value !== null);
  let status = hasEvidence ? 'UNVOLLSTAENDIG' : 'OFFEN';
  if (Object.values(checks).includes(false)) status = 'ABWEICHUNG';
  else if (Object.values(checks).every((value) => value === true)) status = 'AUTOMATISCH_GRUEN';
  else if (bookedCents !== null && receiptCents === null) status = 'BELEG_FEHLT';

  return {
    sourceMode: 'AUTOMATISCH',
    editable: false,
    sollCents: roundedCents ?? qlikCents,
    istCents: bookedCents,
    status,
    note: null,
    qlik,
    evidence: {
      roundedCents,
      bookedCents,
      safeSkimCents: skimCents,
      receiptCents,
      receiptCount: Number(receipt?.receipt_count || 0),
      businessDayStatus: businessDay?.business_day_status || null,
      checks,
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

function buildAlex3Month({ year, month, branches, manualRows = [], businessDays = [], receipts = [], qlikValues = [] }) {
  const manualByKey = new Map(manualRows.map((row) => [manualKey(row.business_date, row.filiale, row.movement_type), row]));
  const businessDayByKey = new Map(businessDays.map((row) => [key(row.business_date, row.filiale), row]));
  const receiptByKey = new Map(receipts.map((row) => [key(row.business_date, row.filiale), row]));
  const qlikByKey = new Map(qlikValues.map((row) => [qlikKey(row.business_date, row.filiale, row.metric_code), row]));
  const daysInMonth = new Date(year, month, 0).getDate();
  const days = [];
  const summary = { automaticGreen: 0, deviations: 0, missing: 0, manualOpen: 0 };

  for (let day = 1; day <= daysInMonth; day += 1) {
    const businessDate = `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    const movements = ALEX3_MOVEMENT_TYPES.map((movementType) => {
      const metricCode = movementType === 'EZ' ? 'BAR_AN_BANK_QLIK' : 'KASSENAUSZAHLUNGEN';
      const cells = {};
      for (const branch of branches) {
        const qlik = qlikSignal(qlikByKey.get(qlikKey(businessDate, branch, metricCode)));
        const isAutomaticVredenEz = branch === 'Vreden' && movementType === 'EZ';
        const cell = isAutomaticVredenEz
          ? buildAutomaticEz({
              businessDay: businessDayByKey.get(key(businessDate, branch)),
              receipt: receiptByKey.get(key(businessDate, branch)),
              qlik,
            })
          : buildManualCell({
              manual: manualByKey.get(manualKey(businessDate, branch, movementType)),
              qlik,
              movementType,
            });

        if (cell.status === 'AUTOMATISCH_GRUEN') summary.automaticGreen += 1;
        else if (cell.status === 'ABWEICHUNG') summary.deviations += 1;
        else if (cell.status === 'OFFEN' && cell.sourceMode === 'MANUELL'
          && [cell.sollCents, cell.istCents, cell.qlik?.valueCents].some((value) => value !== null)) {
          summary.manualOpen += 1;
        }
        else if (['UNVOLLSTAENDIG', 'BELEG_FEHLT'].includes(cell.status)) summary.missing += 1;
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
      automaticEzBranches: ['Vreden'],
      manualEzBranches: branches.filter((branch) => branch !== 'Vreden'),
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
