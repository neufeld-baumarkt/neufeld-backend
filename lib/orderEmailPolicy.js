'use strict';

const ORDER_EMAILS = Object.freeze({
  sender: 'bestellungen@neufeldbaumarkt.de',
  supervisor: 'mklee@sonderpreis-baumarkt.de',
  supplier: 'innendienst@mellerud.de',
  branches: Object.freeze({
    ahaus: 'ahaus@sonderpreis-baumarkt.de',
    'münster': 'muenster@sonderpreis-baumarkt.de',
    telgte: 'telgte@sonderpreis-baumarkt.de',
    vreden: 'vreden@sonderpreis-baumarkt.de',
  }),
});

const normalizeEmail = (value) => typeof value === 'string' ? value.trim().toLowerCase() : '';
const normalizeBranch = (value) => typeof value === 'string' ? value.trim().toLocaleLowerCase('de-DE') : '';

const ALLOWED_ORDER_EMAILS = new Set([
  ORDER_EMAILS.sender,
  ORDER_EMAILS.supervisor,
  ORDER_EMAILS.supplier,
  ...Object.values(ORDER_EMAILS.branches),
]);

function expectedBranchEmail(branch) {
  const email = ORDER_EMAILS.branches[normalizeBranch(branch)];
  if (!email) throw new Error(`Keine freigegebene Bestelladresse für Filiale ${branch || '(leer)'} hinterlegt`);
  return email;
}

function requireExpectedEmail(label, actual, expected) {
  if (normalizeEmail(actual) !== expected) {
    throw new Error(`${label} weicht von der freigegebenen Bestelladresse ab`);
  }
  return expected;
}

function resolveCanonicalOrderRecipients({
  mode,
  branch,
  storedBranchEmail,
  storedSupplierEmail,
  configuredSupervisorEmail,
}) {
  const branchEmail = expectedBranchEmail(branch);
  requireExpectedEmail(`Filialadresse ${branch}`, storedBranchEmail, branchEmail);

  if (mode === 'final') {
    requireExpectedEmail('Mellerud-Bestelladresse', storedSupplierEmail, ORDER_EMAILS.supplier);
    return { to: [ORDER_EMAILS.supplier], cc: [branchEmail] };
  }

  requireExpectedEmail('Supervisor-Adresse', configuredSupervisorEmail, ORDER_EMAILS.supervisor);
  return { to: [ORDER_EMAILS.supervisor, branchEmail], cc: [] };
}

function validateMailEnvelope({ mode, sender, to, cc = [], testRecipient }) {
  requireExpectedEmail('SMTP-Absender', sender, ORDER_EMAILS.sender);
  if (!['live', 'test'].includes(mode)) throw new Error('MAIL_MODE muss live oder test sein');

  const effectiveTo = mode === 'test' ? [testRecipient] : (Array.isArray(to) ? to : [to]);
  const effectiveCc = mode === 'test' ? [] : (Array.isArray(cc) ? cc : [cc]);
  const recipients = [...effectiveTo, ...effectiveCc].map(normalizeEmail).filter(Boolean);
  if (recipients.length === 0) throw new Error('Kein Empfänger definiert');
  const blocked = recipients.find((email) => !ALLOWED_ORDER_EMAILS.has(email));
  if (blocked) throw new Error(`Nicht freigegebene Bestelladresse blockiert: ${blocked}`);

  return { to: effectiveTo.map(normalizeEmail), cc: effectiveCc.map(normalizeEmail) };
}

module.exports = {
  ALLOWED_ORDER_EMAILS,
  ORDER_EMAILS,
  expectedBranchEmail,
  resolveCanonicalOrderRecipients,
  validateMailEnvelope,
};
