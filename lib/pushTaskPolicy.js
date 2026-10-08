'use strict';

const crypto = require('crypto');

const PILOT_ROLES = new Set(['Admin', 'Supervisor']);
const FULL_CREATOR_ROLES = new Set(['Admin', 'Geschäftsführer', 'Supervisor', 'Manager-1']);
const PRIORITIES = new Set(['normal', 'high', 'critical']);
const PROOF_MODES = new Set(['confirm', 'photo', 'both']);

const clean = (value) => typeof value === 'string' ? value.trim() : '';
const modeOf = (value) => value === 'full' ? 'full' : 'pilot';

function canUsePushTasks(role, mode = 'pilot') {
  return modeOf(mode) === 'pilot' ? PILOT_ROLES.has(role) : Boolean(clean(role));
}

function canCreatePushTasks(role, mode = 'pilot') {
  return modeOf(mode) === 'pilot' ? PILOT_ROLES.has(role) : FULL_CREATOR_ROLES.has(role);
}

function canManagePushTask(task, user, mode = 'pilot') {
  if (!canUsePushTasks(user?.role, mode)) return false;
  return Number(task?.created_by_user_id) === Number(user?.id)
    || user?.role === 'Admin'
    || (modeOf(mode) === 'full' && user?.role === 'Geschäftsführer');
}

function canReviewPushTask(task, user) {
  return Number(task?.created_by_user_id) === Number(user?.id);
}

function parseDate(value, label) {
  const date = new Date(value);
  if (!value || Number.isNaN(date.getTime())) {
    throw Object.assign(new Error(`${label} ist ungültig.`), { statusCode: 400 });
  }
  return date;
}

function normalizePushTaskInput(input = {}) {
  const title = clean(input.title);
  const description = clean(input.description);
  const priority = clean(input.priority) || 'normal';
  const proofMode = clean(input.proof_mode) || 'confirm';
  if (!title || title.length > 160) throw Object.assign(new Error('Titel fehlt oder ist länger als 160 Zeichen.'), { statusCode: 400 });
  if (!description || description.length > 5000) throw Object.assign(new Error('Beschreibung fehlt oder ist länger als 5.000 Zeichen.'), { statusCode: 400 });
  if (!PRIORITIES.has(priority)) throw Object.assign(new Error('Priorität ist ungültig.'), { statusCode: 400 });
  if (!PROOF_MODES.has(proofMode)) throw Object.assign(new Error('Nachweisart ist ungültig.'), { statusCode: 400 });

  const due = parseDate(input.due_at, 'Fälligkeit');
  const reminder = input.reminder_at ? parseDate(input.reminder_at, 'Erinnerung') : new Date(due.getTime() - 24 * 3600_000);
  const urgent = input.urgent_at ? parseDate(input.urgent_at, 'Dringende Eskalation') : new Date(due);
  const hard = input.hard_escalation_at ? parseDate(input.hard_escalation_at, 'Harte Eskalation') : new Date(due.getTime() + 24 * 3600_000);
  if (!(reminder <= urgent && urgent <= hard)) {
    throw Object.assign(new Error('Eskalationszeitpunkte müssen chronologisch aufeinander folgen.'), { statusCode: 400 });
  }

  const assigneeIds = [...new Set((Array.isArray(input.assignee_user_ids) ? input.assignee_user_ids : [])
    .map(Number).filter((id) => Number.isInteger(id) && id > 0))];
  if (assigneeIds.length === 0 || assigneeIds.length > 100) {
    throw Object.assign(new Error('Mindestens ein und höchstens 100 Ausführende sind erforderlich.'), { statusCode: 400 });
  }
  return {
    title, description, priority, proofMode, assigneeIds,
    dueAt: due.toISOString(), reminderAt: reminder.toISOString(), urgentAt: urgent.toISOString(), hardAt: hard.toISOString(),
  };
}

function escalationLevelFor(task, assignment, now = new Date()) {
  if (task?.status !== 'active' || ['submitted', 'approved'].includes(assignment?.status)) return 0;
  const current = now instanceof Date ? now : new Date(now);
  if (assignment?.escalation_snoozed_until && current < new Date(assignment.escalation_snoozed_until)) return Number(assignment.escalation_level || 0);
  if (current >= new Date(task.hard_escalation_at)) return 3;
  if (current >= new Date(task.urgent_at)) return 2;
  if (current >= new Date(task.reminder_at)) return 1;
  return 0;
}

function escalationAudience(level, { assignee, creator, admins = [], directors = [], mode = 'pilot' }) {
  const people = level === 1 ? [assignee, creator]
    : level === 2 ? [assignee, creator, ...admins]
      : [assignee, creator, ...admins, ...(modeOf(mode) === 'full' ? directors : [])];
  const unique = new Map();
  for (const person of people) if (person?.id && person.active !== false && canUsePushTasks(person.role, mode)) unique.set(Number(person.id), person);
  return [...unique.values()];
}

function jobSignature(secret, timestamp, method = 'POST', path = '/api/tasks/internal/process-escalations') {
  return crypto.createHmac('sha256', secret).update(`${timestamp}.${method.toUpperCase()}.${path}`).digest('hex');
}

function verifyJobSignature({ secret, timestamp, signature, now = Date.now(), maxAgeMs = 5 * 60_000, method, path }) {
  if (!secret || !timestamp || !signature || !/^\d+$/.test(String(timestamp)) || !/^[0-9a-f]{64}$/i.test(String(signature))) return false;
  const millis = Number(timestamp) * 1000;
  if (!Number.isFinite(millis) || Math.abs(now - millis) > maxAgeMs) return false;
  const expected = jobSignature(secret, timestamp, method, path);
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(String(signature).toLowerCase()));
}

module.exports = {
  canCreatePushTasks,
  canManagePushTask,
  canReviewPushTask,
  canUsePushTasks,
  escalationAudience,
  escalationLevelFor,
  jobSignature,
  modeOf,
  normalizePushTaskInput,
  verifyJobSignature,
};
