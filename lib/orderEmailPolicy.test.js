'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  ORDER_EMAILS,
  resolveCanonicalOrderRecipients,
  validateMailEnvelope,
} = require('./orderEmailPolicy');

test('light dispatch only targets Mirko and the ordering branch', () => {
  assert.deepEqual(resolveCanonicalOrderRecipients({
    mode: 'light',
    branch: 'Münster',
    storedBranchEmail: 'muenster@sonderpreis-baumarkt.de',
    configuredSupervisorEmail: 'mklee@sonderpreis-baumarkt.de',
  }), {
    to: ['mklee@sonderpreis-baumarkt.de', 'muenster@sonderpreis-baumarkt.de'],
    cc: [],
  });
});

test('final dispatch only targets Mellerud with the ordering branch in CC', () => {
  assert.deepEqual(resolveCanonicalOrderRecipients({
    mode: 'final',
    branch: 'Ahaus',
    storedBranchEmail: 'ahaus@sonderpreis-baumarkt.de',
    storedSupplierEmail: 'innendienst@mellerud.de',
  }), {
    to: ['innendienst@mellerud.de'],
    cc: ['ahaus@sonderpreis-baumarkt.de'],
  });
});

test('recipient resolution blocks wrong or cross-wired addresses', () => {
  assert.throws(() => resolveCanonicalOrderRecipients({
    mode: 'light',
    branch: 'Ahaus',
    storedBranchEmail: 'vreden@sonderpreis-baumarkt.de',
    configuredSupervisorEmail: ORDER_EMAILS.supervisor,
  }), /Filialadresse Ahaus weicht/);
  assert.throws(() => resolveCanonicalOrderRecipients({
    mode: 'final',
    branch: 'Vreden',
    storedBranchEmail: ORDER_EMAILS.branches.vreden,
    storedSupplierEmail: 'alt@example.com',
  }), /Mellerud-Bestelladresse weicht/);
});

test('mail envelope blocks unknown recipients and a wrong sender', () => {
  assert.throws(() => validateMailEnvelope({
    mode: 'live', sender: 'alt@neufeldbaumarkt.de', to: [ORDER_EMAILS.supervisor],
  }), /SMTP-Absender weicht/);
  assert.throws(() => validateMailEnvelope({
    mode: 'live', sender: ORDER_EMAILS.sender, to: ['unknown@example.com'],
  }), /Nicht freigegebene Bestelladresse/);
});

test('test mode is restricted to an allowlisted test recipient', () => {
  assert.deepEqual(validateMailEnvelope({
    mode: 'test', sender: ORDER_EMAILS.sender,
    to: [ORDER_EMAILS.supervisor], cc: [ORDER_EMAILS.branches.ahaus],
    testRecipient: ORDER_EMAILS.sender,
  }), { to: [ORDER_EMAILS.sender], cc: [] });
  assert.throws(() => validateMailEnvelope({
    mode: 'test', sender: ORDER_EMAILS.sender,
    to: [ORDER_EMAILS.supervisor], testRecipient: 'private@example.com',
  }), /Nicht freigegebene Bestelladresse/);
});
