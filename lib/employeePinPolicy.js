const DEFAULT_EMPLOYEE_PIN = '0000';

function cleanNamePart(value, label) {
  const clean = String(value ?? '').trim().replace(/\s+/g, ' ');
  if (clean.length < 2 || clean.length > 80 || /[\u0000-\u001f\u007f]/u.test(clean)) {
    throw Object.assign(new Error(`${label} muss 2 bis 80 Zeichen enthalten.`), { statusCode: 400 });
  }
  return clean;
}

function employeeName(body) {
  const firstName = cleanNamePart(body?.first_name, 'Vorname');
  const lastName = cleanNamePart(body?.last_name, 'Nachname');
  return { firstName, lastName, displayName: `${firstName} ${lastName}` };
}

function branchId(value) {
  const id = Number(value);
  if (!Number.isInteger(id) || id <= 0) {
    throw Object.assign(new Error('Bitte eine gültige Filiale auswählen.'), { statusCode: 400 });
  }
  return id;
}

function fourDigitPin(value, label = 'PIN') {
  const pin = String(value ?? '').trim();
  if (!/^\d{4}$/.test(pin)) {
    throw Object.assign(new Error(`${label} muss genau vier Ziffern haben.`), { statusCode: 400 });
  }
  return pin;
}

function personalPin(value) {
  const pin = fourDigitPin(value, 'Der persönliche PIN');
  if (pin === DEFAULT_EMPLOYEE_PIN) {
    throw Object.assign(new Error('0000 ist ausschließlich der einmalige Standard-PIN.'), { statusCode: 400 });
  }
  return pin;
}

module.exports = {
  DEFAULT_EMPLOYEE_PIN,
  branchId,
  employeeName,
  fourDigitPin,
  personalPin,
};
