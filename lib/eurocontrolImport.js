const crypto = require('node:crypto');
const { PDFDocument } = require('pdf-lib');

const MAX_SOURCE_BYTES = 512 * 1024;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const IDENTIFIER_PATTERN = /^[0-9]{6}$/;
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const LOCAL_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/;
const FILE_NAME_PATTERN = /^[^/\\\x00-\x1f\x7f]{1,255}\.pdf$/i;

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
      paymentRows,
      totals,
    },
  };
}

function calculateDailyValues(rows, mappings) {
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
  const providerBreakdown = {};
  for (const row of mapped.filter((item) => item.mappingRole === 'CREDIT_PROVIDER')) {
    providerBreakdown[row.providerCode] = (providerBreakdown[row.providerCode] || 0) + row.sollCents;
  }
  const ecCashCents = ecRows[0].sollCents;
  const creditCardCents = Object.values(providerBreakdown).reduce((sum, amount) => sum + amount, 0);
  if (ecCashCents < 0 || creditCardCents < 0) {
    throw importProblem('EUROCONTROL_REFINED_AMOUNT_NEGATIVE', 'EuroControl-Fachwert darf nicht negativ sein.', 422);
  }
  return { mapped, ecCashCents, creditCardCents, providerBreakdown };
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
    if (duplicate.rows[0]) {
      await client.query('COMMIT');
      return { deduplicated: true, ...duplicate.rows[0] };
    }
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
    const calculated = calculateDailyValues(validated.report.paymentRows, mappings);
    const branch = branchResult.rows[0];
    const existing = await client.query(
      `SELECT payment_type_code, amount_cents
         FROM controlling.eurocontrol_daily_values
        WHERE branch_id=$1 AND business_date=$2 AND quality_status='VALIDATED'`,
      [branch.branch_id, validated.report.reportDate],
    );
    const expected = new Map([
      ['EC_CASH', calculated.ecCashCents],
      ['KREDITKARTE', calculated.creditCardCents],
    ]);
    const conflict = existing.rows.some((row) => Number(row.amount_cents) !== expected.get(row.payment_type_code));
    const status = conflict ? 'CONFLICT' : 'IMPORTED';
    const quality = conflict ? 'CONFLICT' : 'VALIDATED';
    const inserted = await client.query(
      `INSERT INTO controlling.eurocontrol_imports (
         branch_id, report_date, source_file_name, source_sha256, source_size_bytes,
         source_payload, external_identifier, external_label, page_count, report_version,
         report_created_at, parsed_payload, status
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13)
       RETURNING id, status, report_date, source_sha256`,
      [
        branch.branch_id, validated.report.reportDate, validated.fileName,
        validated.sourceSha256, validated.sourceSizeBytes, validated.sourcePayload,
        validated.report.branchIdentifier, validated.report.branchLabel || branch.branch_name,
        validated.report.pageCount, validated.report.reportVersion,
        validated.report.reportCreatedAt, JSON.stringify(validated.report), status,
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
