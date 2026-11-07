const EXPECTED_HEADERS = Object.freeze(['Datum', 'KundeNr', 'Zahlart', 'Zahlbetrag']);

function reportError(message) {
  return Object.assign(new Error(message), { code: 'INVALID_QLIK_FILIALBERICHT' });
}

function parseDelimitedLine(line, delimiter = ';') {
  const fields = [];
  let field = '';
  let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    if (character === '"') {
      if (quoted && line[index + 1] === '"') {
        field += '"';
        index += 1;
      } else {
        quoted = !quoted;
      }
    } else if (character === delimiter && !quoted) {
      fields.push(field);
      field = '';
    } else {
      field += character;
    }
  }
  if (quoted) throw reportError('Die CSV enthaelt ein nicht geschlossenes Anfuehrungszeichen.');
  fields.push(field);
  return fields;
}

function parseGermanCents(value) {
  const normalized = String(value || '').trim();
  if (!/^-?(?:\d+|\d{1,3}(?:\.\d{3})+),\d{2}$/.test(normalized)) {
    throw reportError(`Ungueltiger Zahlbetrag: ${normalized || '(leer)'}`);
  }
  const sign = normalized.startsWith('-') ? -1 : 1;
  const unsigned = normalized.replace(/^-/, '').replace(/\./g, '');
  const [euros, cents] = unsigned.split(',');
  const result = sign * (Number(euros) * 100 + Number(cents));
  if (!Number.isSafeInteger(result)) throw reportError('Der Zahlbetrag ist ausserhalb des sicheren Wertebereichs.');
  return result;
}

function parseBusinessDate(value) {
  const match = /^(\d{2})\.(\d{2})\.(\d{4})$/.exec(String(value || '').trim());
  if (!match) throw reportError(`Ungueltiges Berichtsdatum: ${value || '(leer)'}`);
  const [, day, month, year] = match;
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
  if (
    date.getUTCFullYear() !== Number(year)
    || date.getUTCMonth() !== Number(month) - 1
    || date.getUTCDate() !== Number(day)
  ) {
    throw reportError(`Ungueltiges Berichtsdatum: ${value}`);
  }
  return `${day}.${month}.${year}`;
}

function validateFilialberichtCsv(csvText, options = {}) {
  const text = String(csvText || '').replace(/^\uFEFF/, '');
  const lines = text.split(/\r?\n/).filter((line) => line.trim() !== '');
  if (lines.length < 2) throw reportError('Der Filialbericht enthaelt keine Datenzeilen.');

  const headers = parseDelimitedLine(lines[0]);
  if (
    headers.length !== EXPECTED_HEADERS.length
    || headers.some((header, index) => header !== EXPECTED_HEADERS[index])
  ) {
    throw reportError(`Unerwartete Spalten. Erwartet: ${EXPECTED_HEADERS.join(';')}`);
  }

  const rows = lines.slice(1).map((line, index) => {
    const fields = parseDelimitedLine(line);
    if (fields.length !== EXPECTED_HEADERS.length) {
      throw reportError(`Datenzeile ${index + 2} hat nicht genau vier Spalten.`);
    }
    const [rawDate, rawCustomerNumber, rawPaymentType, rawAmount] = fields;
    const date = parseBusinessDate(rawDate);
    const customerNumber = String(rawCustomerNumber || '').trim();
    const paymentType = String(rawPaymentType || '').trim();
    if (!/^\d{6}$/.test(customerNumber)) {
      throw reportError(`Ungueltige KundeNr in Datenzeile ${index + 2}.`);
    }
    if (!paymentType) throw reportError(`Leere Zahlart in Datenzeile ${index + 2}.`);
    return {
      date,
      customerNumber,
      paymentType,
      amountCents: parseGermanCents(rawAmount),
      rawFields: [rawDate, rawCustomerNumber, rawPaymentType, rawAmount],
    };
  });

  const dates = new Set(rows.map((row) => row.date));
  const customerNumbers = new Set(rows.map((row) => row.customerNumber));
  if (dates.size !== 1) throw reportError('Der Filialbericht enthaelt mehrere Berichtsdatumswerte.');
  if (!options.allowMultipleCustomers && customerNumbers.size !== 1) {
    throw reportError('Der Filialbericht enthaelt mehrere Kundennummern.');
  }

  const [date] = dates;
  const sortedCustomerNumbers = [...customerNumbers].sort();
  const customerNumber = sortedCustomerNumbers.length === 1 ? sortedCustomerNumbers[0] : null;
  if (options.expectedDate && date !== parseBusinessDate(options.expectedDate)) {
    throw reportError(`Berichtsdatum ${date} stimmt nicht mit ${options.expectedDate} ueberein.`);
  }
  if (options.expectedCustomerNumber && customerNumber !== String(options.expectedCustomerNumber)) {
    throw reportError(`KundeNr ${customerNumber} stimmt nicht mit ${options.expectedCustomerNumber} ueberein.`);
  }
  if (options.expectedCustomerNumbers) {
    const expected = [...new Set(options.expectedCustomerNumbers.map(String))].sort();
    if (
      expected.length !== sortedCustomerNumbers.length
      || expected.some((value, index) => value !== sortedCustomerNumbers[index])
    ) {
      throw reportError(
        `KundeNr-Menge ${sortedCustomerNumbers.join(',')} stimmt nicht mit ${expected.join(',')} ueberein.`,
      );
    }
  }

  const uniqueKeys = new Set();
  for (const row of rows) {
    const key = `${row.date}\u0000${row.customerNumber}\u0000${row.paymentType}`;
    if (uniqueKeys.has(key)) throw reportError(`Doppelte Zahlart im Filialbericht: ${row.paymentType}`);
    uniqueKeys.add(key);
  }

  const netCents = rows.reduce((sum, row) => sum + row.amountCents, 0);
  const expectedNetCents = options.expectedNetCents ?? 0;
  if (netCents !== expectedNetCents) {
    throw reportError(`Kontrollsumme ${netCents} Cent statt ${expectedNetCents} Cent.`);
  }

  return {
    date,
    customerNumber,
    customerNumbers: sortedCustomerNumbers,
    rowCount: rows.length,
    nonZeroRowCount: rows.filter((row) => row.amountCents !== 0).length,
    netCents,
    rows,
  };
}

module.exports = { parseBusinessDate, parseGermanCents, validateFilialberichtCsv };
