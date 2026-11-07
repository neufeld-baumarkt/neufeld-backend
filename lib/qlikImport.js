const crypto = require('node:crypto');
const path = require('node:path');
const { validateFilialberichtCsv } = require('./qlikFilialberichtCsv');

const CUSTOMER_BRANCHES = Object.freeze({
  '041117': 'Telgte',
  '041129': 'Ahaus',
  '041135': 'Vreden',
  '041140': 'Münster',
});
const EXPECTED_CUSTOMER_NUMBERS = Object.freeze(Object.keys(CUSTOMER_BRANCHES).sort());

function importError(message) {
  return Object.assign(new Error(message), { code: 'QLIK_IMPORT_FAILED' });
}

function validateFileName(value) {
  const fileName = String(value || '').trim();
  if (!fileName || fileName.length > 255 || path.basename(fileName) !== fileName) {
    throw importError('Der Qlik-Dateiname ist ungueltig.');
  }
  if (!fileName.toLowerCase().endsWith('.csv')) throw importError('Der Qlik-Import muss eine CSV-Datei sein.');
  return fileName;
}

async function appendEvent(client, runId, eventType, details = {}) {
  await client.query(
    `INSERT INTO controlling.qlik_import_events (import_run_id,event_type,details)
     VALUES ($1,$2,$3::jsonb)`,
    [runId, eventType, JSON.stringify(details)],
  );
}

async function importQlikCsv({ pool, payload, fileName }) {
  if (!pool || typeof pool.connect !== 'function') throw importError('Ein Datenbank-Pool ist erforderlich.');
  if (!Buffer.isBuffer(payload) || payload.length === 0) throw importError('Der Qlik-Import ist leer.');
  const sourceFileName = validateFileName(fileName);
  const report = validateFilialberichtCsv(payload.toString('utf8'), {
    allowMultipleCustomers: true,
    expectedCustomerNumbers: EXPECTED_CUSTOMER_NUMBERS,
  });
  const sourceSha256 = crypto.createHash('sha256').update(payload).digest('hex');
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const existing = await client.query(
      `SELECT id,status,report_date FROM controlling.qlik_import_runs
       WHERE source_sha256=$1`,
      [sourceSha256],
    );
    if (existing.rows[0]) {
      await client.query('COMMIT');
      return { deduplicated: true, run: existing.rows[0], report };
    }

    const inserted = await client.query(
      `INSERT INTO controlling.qlik_import_runs
       (report_date,source_file_name,source_sha256,source_size_bytes,source_payload,status)
       VALUES (to_date($1,'DD.MM.YYYY'),$2,$3,$4,$5,'VALIDATED')
       RETURNING id,status,report_date`,
      [report.date, sourceFileName, sourceSha256, payload.length, payload],
    );
    const run = inserted.rows[0];
    if (!run) throw importError('Der Qlik-Importlauf konnte nicht angelegt werden.');
    await appendEvent(client, run.id, 'IMPORT_VALIDATED', {
      sourceSha256,
      rowCount: report.rowCount,
      customerCount: report.customerNumbers.length,
    });

    for (let index = 0; index < report.rows.length; index += 1) {
      const row = report.rows[index];
      await client.query(
        `INSERT INTO controlling.qlik_import_rows
         (import_run_id,source_row_number,report_date,customer_number,branch_name,
          payment_type_original,amount_cents,raw_fields)
         VALUES ($1,$2,to_date($3,'DD.MM.YYYY'),$4,$5,$6,$7,$8::jsonb)`,
        [
          run.id,
          index + 2,
          row.date,
          row.customerNumber,
          CUSTOMER_BRANCHES[row.customerNumber],
          row.paymentType,
          row.amountCents,
          JSON.stringify(row.rawFields),
        ],
      );
    }

    const completed = await client.query(
      `UPDATE controlling.qlik_import_runs
       SET status='IMPORTED',row_count=$2,customer_count=$3,payment_type_count=$4,completed_at=NOW()
       WHERE id=$1 AND status='VALIDATED'
       RETURNING id,status,report_date,row_count,customer_count,payment_type_count`,
      [
        run.id,
        report.rowCount,
        report.customerNumbers.length,
        new Set(report.rows.map((row) => row.paymentType)).size,
      ],
    );
    if (!completed.rows[0]) throw importError('Der Qlik-Importlauf konnte nicht abgeschlossen werden.');
    await appendEvent(client, run.id, 'IMPORT_COMPLETED', {
      rowCount: report.rowCount,
      customerCount: report.customerNumbers.length,
    });
    await client.query('COMMIT');
    return { deduplicated: false, run: completed.rows[0], report };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

module.exports = { importQlikCsv };
