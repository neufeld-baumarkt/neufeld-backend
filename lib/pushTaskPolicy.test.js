'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  canCreatePushTasks, canManagePushTask, canReviewPushTask, canUsePushTasks,
  escalationAudience, escalationLevelFor, jobSignature, modeOf, normalizePushTaskInput, verifyJobSignature,
} = require('./pushTaskPolicy');
const { processPushTaskEscalations } = require('../services/pushTaskEscalationService');

test('off mode rejects all roles and skips database and email work', async () => {
  assert.equal(modeOf(undefined), 'off');
  for (const role of ['Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1', 'Filiale']) {
    assert.equal(canUsePushTasks(role, 'off'), false);
    assert.equal(canCreatePushTasks(role, 'off'), false);
  }
  const result = await processPushTaskEscalations({
    env: { PUSH_TASK_MODE: 'off' },
    pool: { query() { assert.fail('Paused escalation must not read the database'); } },
    mailer() { assert.fail('Paused escalation must not send mail'); },
  });
  assert.deepEqual(result, { inspected: 0, escalated: 0, emails: { sent: 0, blocked: 0, failed: 0 } });
});

test('pilot is visible and writable only for Admin and Supervisor', () => {
  for (const role of ['Admin', 'Supervisor']) {
    assert.equal(canUsePushTasks(role, 'pilot'), true);
    assert.equal(canCreatePushTasks(role, 'pilot'), true);
  }
  for (const role of ['Geschäftsführer', 'Manager-1', 'Filiale']) {
    assert.equal(canUsePushTasks(role, 'pilot'), false);
    assert.equal(canCreatePushTasks(role, 'pilot'), false);
  }
});

test('only creator, Admin and later Geschäftsführer may edit, but only creator reviews', () => {
  const task = { created_by_user_id: 7 };
  assert.equal(canManagePushTask(task, { id: 7, role: 'Supervisor' }, 'pilot'), true);
  assert.equal(canManagePushTask(task, { id: 8, role: 'Supervisor' }, 'pilot'), false);
  assert.equal(canManagePushTask(task, { id: 8, role: 'Admin' }, 'pilot'), true);
  assert.equal(canManagePushTask(task, { id: 8, role: 'Geschäftsführer' }, 'full'), true);
  assert.equal(canReviewPushTask(task, { id: 7, role: 'Supervisor' }), true);
  assert.equal(canReviewPushTask(task, { id: 8, role: 'Admin' }), false);
});

test('task input applies standard escalation times and unique assignees', () => {
  const value = normalizePushTaskInput({ title: ' Test ', description: ' Aufgabe ', due_at: '2026-10-20T10:00:00Z', assignee_user_ids: [1, 1, 2] });
  assert.deepEqual(value.assigneeIds, [1, 2]);
  assert.equal(value.reminderAt, '2026-10-19T10:00:00.000Z');
  assert.equal(value.urgentAt, '2026-10-20T10:00:00.000Z');
  assert.equal(value.hardAt, '2026-10-21T10:00:00.000Z');
});

test('submitted assignments pause escalation and delayed jobs select the highest level', () => {
  const task = { status: 'active', reminder_at: '2026-10-01T00:00:00Z', urgent_at: '2026-10-02T00:00:00Z', hard_escalation_at: '2026-10-03T00:00:00Z' };
  assert.equal(escalationLevelFor(task, { status: 'open', escalation_level: 0 }, new Date('2026-10-04T00:00:00Z')), 3);
  assert.equal(escalationLevelFor(task, { status: 'submitted', escalation_level: 2 }, new Date('2026-10-04T00:00:00Z')), 0);
});

test('pilot escalation audience never leaks to hidden roles', () => {
  const audience = escalationAudience(3, {
    mode: 'pilot',
    assignee: { id: 1, role: 'Supervisor' }, creator: { id: 2, role: 'Admin' },
    admins: [{ id: 2, role: 'Admin' }], directors: [{ id: 3, role: 'Geschäftsführer' }],
  });
  assert.deepEqual(audience.map((x) => x.id), [1, 2]);
});

test('NAS job signatures are time-bound and tamper evident', () => {
  const timestamp = '1791450000';
  const signature = jobSignature('secret', timestamp);
  assert.equal(verifyJobSignature({ secret: 'secret', timestamp, signature, now: 1791450000 * 1000 }), true);
  assert.equal(verifyJobSignature({ secret: 'wrong', timestamp, signature, now: 1791450000 * 1000 }), false);
  assert.equal(verifyJobSignature({ secret: 'secret', timestamp, signature, now: 1791451000 * 1000 }), false);
});
