const crypto = require('node:crypto');
const { PDFDocument } = require('pdf-lib');

const MAX_SOURCE_BYTES = 512 * 1024;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const IDENTIFIER_PATTERN = /^[0-9]{6}$/;
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const LOCAL_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/;
const FILE_NAME_PATTERN = /^[^/\\\x00-\x1f\x7f]{1,255}\.pdf$/i;
const PARSER_VERSION_PATTERN = /^\d+\.\d+\.\d+$/;
const REQUIRED_REPORT_FIELDS = new Map([
  ['REPORT_REVENUE_CENTS', 'MONEY_CENTS'],
  ['PAYOUTS_CENTS', 'MONEY_CENTS'],
  ['CUSTOMER_COUNT', 'COUNT'],
  ['AVERAGE_RECEIPT_CENTS', 'MONEY_CENTS'],
  ['BANK_DEPOSIT_CENTS', 'MONEY_CENTS'],
  ['VAT_7_GROSS_CENTS', 'MONEY_CENTS'],
  ['VAT_19_GROSS_CENTS', 'MONEY_CENTS'],
]);
const QLIK_CREDIT_PROVIDERS = new Set(['AMEX', 'MAESTRO', 'MASTERCARD', 'VISA']);

function importProblem(code, message, statusCode = 400) {
  const error = new Error(message);
  error.code = code;
  error.statusCode = statusCode;
  return error;
}

function integer(value, code) {
  if (!Number.isSafeInteger(value)) throw importProblem(code, 'EuroControl-Betrag ist ungültig.');
  return value;
}

function validDate(value) {
  if (!DATE_PATTERN.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.valueOf()) && parsed.toISOString().slice(0, 10) === value;
}

function validatePayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw importProblem('EUROCONTROL_PAYLOAD_INVALID', 'EuroControl-Nutzdaten fehlen.');
  }
  const fileName = String(payload.fileName || '');
  const sourceSha256 = String(payload.sourceSha256 || '').toLowerCase();
  const sourceSizeBytes = payload.sourceSizeBytes;
  const report = payload.report;
  if (!FILE_NAME_PATTERN.test(fileName)) {
    throw importProblem('EUROCONTROL_FILE_NAME_INVALID', 'EuroControl-Dateiname ist ungültig.');
  }
  if (!SHA256_PATTERN.test(sourceSha256)) {
    throw importProblem('EUROCONTROL_HASH_INVALID', 'EuroControl-Dateihash ist ungültig.');
  }
  if (!Number.isSafeInteger(sourceSizeBytes) || sourceSizeBytes < 1 || sourceSizeBytes > MAX_SOURCE_BYTES) {
    throw importProblem('EUROCONTROL_SOURCE_SIZE_INVALID', 'EuroControl-Dateigröße ist ungültig.');
  }
  let sourcePayload;
  try {
    sourcePayload = Buffer.from(String(payload.sourcePdfBase64 || ''), 'base64');
  } catch (_error) {
    throw importProblem('EUROCONTROL_SOURCE_BASE64_INVALID', 'EuroControl-PDF ist ungültig codiert.');
  }
  if (sourcePayload.length !== sourceSizeBytes
      || sourcePayload.subarray(0, 5).toString('ascii') !== '%PDF-'
      || sourcePayload.toString('base64') !== payload.sourcePdfBase64) {
    throw importProblem('EUROCONTROL_SOURCE_INVALID', 'EuroControl-PDF stimmt nicht mit den Metadaten überein.');
  }
  const calculatedHash = crypto.createHash('sha256').update(sourcePayload).digest('hex');
  if (calculatedHash !== sourceSha256) {
    throw importProblem('EUROCONTROL_SOURCE_HASH_MISMATCH', 'EuroControl-Dateihash stimmt nicht.');
  }
  if (!report || report.reportType !== 'FILIALBERICHT'
      || !IDENTIFIER_PATTERN.test(String(report.branchIdentifier || ''))
      || !validDate(String(report.reportDate || ''))
      || !Number.isSafeInteger(report.pageCount) || report.pageCount < 1
      || !Array.isArray(report.paymentRows) || report.paymentRows.length < 1
      || !report.totals || typeof report.totals !== 'object') {
    throw importProblem('EUROCONTROL_REPORT_INVALID', 'EuroControl-Bericht ist unvollständig.');
  }
  const paymentRows = report.paymentRows.map((row, index) => {
    const label = String(row?.label || '').trim();
    if (!label || label.length > 100 || row?.rowNumber !== index + 1) {
      throw importProblem('EUROCONTROL_PAYMENT_ROW_INVALID', 'EuroControl-Zahlungszeile ist ungültig.');
    }
    return {
      rowNumber: row.rowNumber,
      label,
      sollCents: integer(row.sollCents, 'EUROCONTROL_PAYMENT_AMOUNT_INVALID'),
      istCents: integer(row.istCents, 'EUROCONTROL_PAYMENT_AMOUNT_INVALID'),
      differenceCents: integer(row.differenceCents, 'EUROCONTROL_PAYMENT_AMOUNT_INVALID'),
      withdrawalCents: integer(row.withdrawalCents, 'EUROCONTROL_PAYMENT_AMOUNT_INVALID'),
    };
  });
  if (new Set(paymentRows.map((row) => row.label)).size !== paymentRows.length) {
    throw importProblem('EUROCONTROL_PAYMENT_LABEL_DUPLICATE', 'EuroControl-Zahlungsart ist doppelt vorhanden.');
  }
  const totals = {
    sollCents: integer(report.totals.sollCents, 'EUROCONTROL_TOTAL_INVALID'),
    istCents: integer(report.totals.istCents, 'EUROCONTROL_TOTAL_INVALID'),
    differenceCents: integer(report.totals.differenceCents, 'EUROCONTROL_TOTAL_INVALID'),
    withdrawalCents: integer(report.totals.withdrawalCents, 'EUROCONTROL_TOTAL_INVALID'),
  };
  if (paymentRows.reduce((sum, row) => sum + row.sollCents, 0) !== totals.sollCents
      || paymentRows.reduce((sum, row) => sum + row.istCents, 0) !== totals.istCents) {
    throw importProblem('EUROCONTROL_TOTAL_MISMATCH', 'EuroControl-Gesamtsumme stimmt nicht.');
  }
  if (report.reportCreatedAt !== null && report.reportCreatedAt !== undefined
      && !LOCAL_TIMESTAMP_PATTERN.test(String(report.reportCreatedAt))) {
    throw importProblem('EUROCONTROL_REPORT_TIMESTAMP_INVALID', 'EuroControl-Erstellzeit ist ungültig.');
  }
  if (!PARSER_VERSION_PATTERN.test(String(report.parserVersion || ''))
      || !Array.isArray(report.reportFields)
      || report.reportFields.length !== REQUIRED_REPORT_FIELDS.size) {
    throw importProblem('EUROCONTROL_REPORT_FIELDS_INVALID', 'EuroControl-Berichtsfelder sind unvollständig.');
  }
  const reportFields = report.reportFields.map((field) => {
    const fieldCode = String(field?.fieldCode || '');
    const valueKind = String(field?.valueKind || '');
    if (REQUIRED_REPORT_FIELDS.get(fieldCode) !== valueKind
        || !Number.isSafeInteger(field.pageNumber) || field.pageNumber < 1
        || field.pageNumber > report.pageCount || field.explicit !== true) {
      throw importProblem('EUROCONTROL_REPORT_FIELD_INVALID', 'EuroControl-Berichtsfeld ist ungültig.');
    }
    const normalized = { fieldCode, valueKind, pageNumber: field.pageNumber, explicit: true };
    if (valueKind === 'COUNT') {
      normalized.countValue = integer(field.countValue, 'EUROCONTROL_REPORT_FIELD_VALUE_INVALID');
      if (normalized.countValue < 0) throw importProblem('EUROCONTROL_REPORT_FIELD_VALUE_INVALID', 'EuroControl-Zählwert ist negativ.');
    } else {
      normalized.amountCents = integer(field.amountCents, 'EUROCONTROL_REPORT_FIELD_VALUE_INVALID');
      if (normalized.amountCents < 0) throw importProblem('EUROCONTROL_REPORT_FIELD_VALUE_INVALID', 'EuroControl-Betrag ist negativ.');
    }
    return normalized;
  });
  if (new Set(reportFields.map((field) => field.fieldCode)).size !== REQUIRED_REPORT_FIELDS.size
      || [...REQUIRED_REPORT_FIELDS.keys()].some((code) => !reportFields.some((field) => field.fieldCode === code))) {
    throw importProblem('EUROCONTROL_REPORT_FIELD_DUPLICATE', 'EuroControl-Berichtsfelder sind nicht eindeutig.');
  }
  const fields = new Map(reportFields.map((field) => [field.fieldCode, field]));
  if (fields.get('VAT_7_GROSS_CENTS').amountCents + fields.get('VAT_19_GROSS_CENTS').amountCents
      !== fields.get('REPORT_REVENUE_CENTS').amountCents) {
    throw importProblem('EUROCONTROL_REVENUE_CONTROL_MISMATCH', 'EuroControl-Umsatz stimmt nicht mit den Steuersätzen überein.');
  }
  return {
    fileName,
    sourceSha256,
    sourceSizeBytes,
    sourcePayload,
    report: {
      ...report,
      branchIdentifier: String(report.branchIdentifier),
      branchLabel: String(report.branchLabel || '').trim().slice(0, 160),
      reportDate: String(report.reportDate),
      reportVersion: report.reportVersion ? String(report.reportVersion).slice(0, 100) : null,
      reportCreatedAt: report.reportCreatedAt || null,
      parserVersion: String(report.parserVersion),
      paymentRows,
      totals,
      reportFields,
    },
  };
}

function calculateDailyValues(rows, mappings, reportFields) {
  const mapped = rows.map((row) => {
    const mapping = mappings.get(row.label);
    if (!mapping) {
      throw importProblem(
        'EUROCONTROL_PAYMENT_MAPPING_MISSING',
        `EuroControl-Zahlungsart ist nicht freigegeben: ${row.label}`,
        422,
      );
    }
    return { ...row, mappingRole: mapping.mapping_role, providerCode: mapping.provider_code };
  });
  const ecRows = mapped.filter((row) => row.mappingRole === 'EC_CASH');
  if (ecRows.length !== 1) {
    throw importProblem('EUROCONTROL_EC_ROW_INVALID', 'EuroControl-Kartenzahlung fehlt oder ist doppelt.', 422);
  }
  const barRows = mapped.filter((row) => row.label === 'BAR');
  if (barRows.length !== 1) {
    throw importProblem('EUROCONTROL_BAR_ROW_INVALID', 'EuroControl-BAR-Zeile fehlt oder ist doppelt.', 422);
  }
  const providerBreakdown = {};
  for (const row of mapped.filter((item) => item.mappingRole === 'CREDIT_PROVIDER')) {
    providerBreakdown[row.providerCode] = (providerBreakdown[row.providerCode] || 0) + row.istCents;
  }
  for (const provider of QLIK_CREDIT_PROVIDERS) {
    if (!(provider in providerBreakdown)) providerBreakdown[provider] = 0;
  }
  const ecCashCents = ecRows[0].istCents;
  const creditCardCents = [...QLIK_CREDIT_PROVIDERS]
    .reduce((sum, provider) => sum + providerBreakdown[provider], 0);
  if (ecCashCents < 0 || creditCardCents < 0) {
    throw importProblem('EUROCONTROL_REFINED_AMOUNT_NEGATIVE', 'EuroControl-Fachwert darf nicht negativ sein.', 422);
  }
  const fields = new Map((reportFields || []).map((field) => [field.fieldCode, field]));
  if ([...REQUIRED_REPORT_FIELDS.keys()].some((code) => !fields.has(code))) {
    throw importProblem('EUROCONTROL_REPORT_FIELDS_INVALID', 'EuroControl-Berichtsfelder sind unvollständig.', 422);
  }
  const values = [
    ['EC_ZAHLUNGEN', ecCashCents, ['PAYMENT:Kartenzahlung:IST'], {}],
    ['KREDITKARTEN_ZAHLUNGEN', creditCardCents,
      [...QLIK_CREDIT_PROVIDERS].map((provider) => `PAYMENT_PROVIDER:${provider}:IST`), providerBreakdown],
    ['KASSENDIFFERENZ', barRows[0].differenceCents, ['PAYMENT:BAR:DIFFERENCE'], {}],
    ['BAR_AN_BANK_QLIK', fields.get('BANK_DEPOSIT_CENTS').amountCents, ['FIELD:BANK_DEPOSIT_CENTS'], {}],
    ['KASSENAUSZAHLUNGEN', fields.get('PAYOUTS_CENTS').amountCents, ['FIELD:PAYOUTS_CENTS'], {}],
    ['UMSATZ_7_PROZENT', fields.get('VAT_7_GROSS_CENTS').amountCents, ['FIELD:VAT_7_GROSS_CENTS'], {}],
    ['UMSATZ_19_PROZENT', fields.get('VAT_19_GROSS_CENTS').amountCents, ['FIELD:VAT_19_GROSS_CENTS'], {}],
    ['GESAMTUMSATZ_QLIK', fields.get('VAT_7_GROSS_CENTS').amountCents
      + fields.get('VAT_19_GROSS_CENTS').amountCents,
    ['FIELD:VAT_7_GROSS_CENTS', 'FIELD:VAT_19_GROSS_CENTS', 'CONTROL:REPORT_REVENUE_CENTS'], {}],
  ].map(([metricCode, numericValue, sourceFields, breakdown]) => ({
    metricCode, numericValue, sourceFields, providerBreakdown: breakdown,
  }));
  return { mapped, ecCashCents, creditCardCents, providerBreakdown, values };
}

async function verifySourcePdf(validated) {
  let document;
  try {
    document = await PDFDocument.load(validated.sourcePayload, { ignoreEncryption: false });
  } catch (_error) {
    throw importProblem('EUROCONTROL_SOURCE_PDF_INVALID', 'EuroControl-Quelldatei ist kein lesbares PDF.');
  }
  if (document.getPageCount() !== validated.report.pageCount) {
    throw importProblem('EUROCONTROL_SOURCE_PAGE_COUNT_MISMATCH', 'EuroControl-Seitenzahl stimmt nicht.');
  }
}

async function insertCanonicalFacts({ client, importId, branchId, validated, calculated, quality }) {
  for (const field of validated.report.reportFields) {
    await client.query(
      `INSERT INTO controlling.eurocontrol_import_fields (
         import_id, field_code, value_kind, amount_cents, count_value,
         page_number, explicitly_reported
       ) VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (import_id, field_code) DO NOTHING`,
      [importId, field.fieldCode, field.valueKind,
        field.amountCents ?? null, field.countValue ?? null,
        field.pageNumber, field.explicit],
    );
  }
  for (const value of calculated.values) {
    const fingerprint = crypto.createHash('sha256').update([
      validated.sourceSha256, value.metricCode, value.numericValue,
      validated.report.parserVersion,
    ].join('|')).digest('hex');
    await client.query(
      `INSERT INTO controlling.eurocontrol_derived_daily_values (
         import_id, branch_id, customer_number, business_date, metric_code,
         numeric_value, provider_breakdown, source_fields, quality_status,
         source_fingerprint, parser_version
       ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11)
       ON CONFLICT (import_id, metric_code) DO NOTHING`,
      [importId, branchId, validated.report.branchIdentifier,
        validated.report.reportDate, value.metricCode, value.numericValue,
        JSON.stringify(value.providerBreakdown), JSON.stringify(value.sourceFields),
        quality, fingerprint, validated.report.parserVersion],
    );
  }
}

async function importEuroControlReport({ pool, payload }) {
  if (!pool || typeof pool.connect !== 'function') {
    throw importProblem('EUROCONTROL_POOL_REQUIRED', 'Ein Datenbank-Pool ist erforderlich.', 500);
  }
  const validated = validatePayload(payload);
  await verifySourcePdf(validated);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [
      `eurocontrol:${validated.report.branchIdentifier}:${validated.report.reportDate}`,
    ]);
    const duplicate = await client.query(
      `SELECT imported.id, imported.status, imported.report_date, imported.source_sha256,
              branch.name AS branch_name,
              COALESCE(MAX(values.amount_cents) FILTER (WHERE values.payment_type_code='EC_CASH'), 0) AS ec_cash_cents,
              COALESCE(MAX(values.amount_cents) FILTER (WHERE values.payment_type_code='KREDITKARTE'), 0) AS credit_card_cents
         FROM controlling.eurocontrol_imports imported
         JOIN public.filialen branch ON branch.id=imported.branch_id
         LEFT JOIN controlling.eurocontrol_daily_values values ON values.import_id=imported.id
        WHERE imported.source_sha256=$1
        GROUP BY imported.id, branch.name`,
      [validated.sourceSha256],
    );
    const branchResult = await client.query(
      `SELECT mapping.branch_id, branch.name AS branch_name
         FROM controlling.branch_source_identifiers mapping
         JOIN public.filialen branch ON branch.id=mapping.branch_id
        WHERE mapping.source_system='EUROCONTROL'
          AND mapping.external_identifier=$1
          AND mapping.active=true
          AND branch.aktiv=true`,
      [validated.report.branchIdentifier],
    );
    if (branchResult.rowCount !== 1) {
      throw importProblem('EUROCONTROL_BRANCH_MAPPING_MISSING', 'EuroControl-Filialkennung ist nicht eindeutig zugeordnet.', 422);
    }
    const mappingsResult = await client.query(
      `SELECT external_label, mapping_role, provider_code
         FROM controlling.eurocontrol_payment_mappings
        WHERE active=true`,
    );
    const mappings = new Map(mappingsResult.rows.map((row) => [row.external_label, row]));
    const calculated = calculateDailyValues(
      validated.report.paymentRows, mappings, validated.report.reportFields,
    );
    const branch = branchResult.rows[0];
    const existing = await client.query(
      `SELECT metric_code, numeric_value
         FROM controlling.eurocontrol_derived_daily_values
        WHERE branch_id=$1 AND business_date=$2 AND quality_status='VALIDATED'`,
      [branch.branch_id, validated.report.reportDate],
    );
    const expected = new Map(calculated.values.map((value) => [value.metricCode, value.numericValue]));
    const conflict = existing.rows.some((row) => Number(row.numeric_value) !== expected.get(row.metric_code));
    const status = conflict ? 'CONFLICT' : 'IMPORTED';
    const quality = conflict ? 'CONFLICT' : 'VALIDATED';
    if (duplicate.rows[0]) {
      const canonicalFacts = await client.query(
        `SELECT
           (SELECT count(*) FROM controlling.eurocontrol_import_fields WHERE import_id=$1) AS field_count,
           (SELECT count(*) FROM controlling.eurocontrol_derived_daily_values WHERE import_id=$1) AS derived_count`,
        [duplicate.rows[0].id],
      );
      const counts = canonicalFacts.rows[0] || {};
      const complete = Number(counts.field_count) === REQUIRED_REPORT_FIELDS.size
        && Number(counts.derived_count) === calculated.values.length;
      if (!complete) {
        await insertCanonicalFacts({
          client,
          importId: duplicate.rows[0].id,
          branchId: branch.branch_id,
          validated,
          calculated,
          quality,
        });
        await client.query(
          `INSERT INTO controlling.eurocontrol_import_events (import_id, event_type, details)
           VALUES ($1,'DUPLICATE_CANONICAL_BACKFILL',$2::jsonb)`,
          [duplicate.rows[0].id, JSON.stringify({
            derivedMetricCount: calculated.values.length,
            parserVersion: validated.report.parserVersion,
            branchIdentifier: validated.report.branchIdentifier,
          })],
        );
      }
      await client.query('COMMIT');
      return { deduplicated: true, upgraded: !complete, ...duplicate.rows[0] };
    }
    const inserted = await client.query(
      `INSERT INTO controlling.eurocontrol_imports (
         branch_id, report_date, source_file_name, source_sha256, source_size_bytes,
         source_payload, external_identifier, external_label, page_count, report_version,
         report_created_at, parser_version, parsed_payload, status
       ) VALUES ($1,$2,$3,$4,$5,NULL,$6,$7,$8,$9,$10,$11,$12::jsonb,$13)
       RETURNING id, status, report_date, source_sha256`,
      [
        branch.branch_id, validated.report.reportDate, validated.fileName,
        validated.sourceSha256, validated.sourceSizeBytes,
        validated.report.branchIdentifier, validated.report.branchLabel || branch.branch_name,
        validated.report.pageCount, validated.report.reportVersion,
        validated.report.reportCreatedAt, validated.report.parserVersion,
        JSON.stringify(validated.report), status,
      ],
    );
    const importRow = inserted.rows[0];
    for (const row of calculated.mapped) {
      await client.query(
        `INSERT INTO controlling.eurocontrol_import_payment_rows (
           import_id, row_number, external_label, mapping_role, provider_code,
           soll_cents, ist_cents, difference_cents, withdrawal_cents
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [importRow.id, row.rowNumber, row.label, row.mappingRole, row.providerCode,
          row.sollCents, row.istCents, row.differenceCents, row.withdrawalCents],
      );
    }
    await insertCanonicalFacts({
      client,
      importId: importRow.id,
      branchId: branch.branch_id,
      validated,
      calculated,
      quality,
    });
    await client.query(
      `INSERT INTO controlling.eurocontrol_daily_values (
         import_id, branch_id, business_date, payment_type_code, amount_cents,
         provider_breakdown, quality_status
       ) VALUES
         ($1,$2,$3,'EC_CASH',$4,'{}'::jsonb,$6),
         ($1,$2,$3,'KREDITKARTE',$5,$7::jsonb,$6)`,
      [importRow.id, branch.branch_id, validated.report.reportDate,
        calculated.ecCashCents, calculated.creditCardCents, quality,
        JSON.stringify(calculated.providerBreakdown)],
    );
    await client.query(
      `INSERT INTO controlling.eurocontrol_import_events (import_id, event_type, details)
       VALUES ($1,$2,$3::jsonb)`,
      [importRow.id, conflict ? 'SOURCE_CONFLICT_DETECTED' : 'IMPORT_VALIDATED', JSON.stringify({
        ecCashCents: calculated.ecCashCents,
        creditCardCents: calculated.creditCardCents,
        derivedMetricCount: calculated.values.length,
        parserVersion: validated.report.parserVersion,
        sourcePayloadStored: false,
        branchIdentifier: validated.report.branchIdentifier,
      })],
    );
    await client.query('COMMIT');
    return {
      deduplicated: false,
      ...importRow,
      branch_name: branch.branch_name,
      ec_cash_cents: calculated.ecCashCents,
      credit_card_cents: calculated.creditCardCents,
    };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  calculateDailyValues,
  importEuroControlReport,
  importProblem,
  validatePayload,
  verifySourcePdf,
};
