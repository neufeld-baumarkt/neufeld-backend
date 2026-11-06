const test = require('node:test');
const assert = require('node:assert/strict');
const { saveWorkflowDraftRevision } = require('./workflowDraftPersistence');

const input = {
  branchId: 4,
  businessDate: '2026-11-06',
  workflowType: 'day_close',
  draftKey: 'active',
  stage: 'finalize',
  serializedState: '{"value":1}',
  userId: 9,
  expectedRevision: 7,
};

test('speichert einen Entwurf mit optimistischer Revision regulär', async () => {
  const calls = [];
  const queryable = {
    query: async (sql, parameters) => {
      calls.push({ sql, parameters });
      return { rows: [{ id: 12, revision: 8 }] };
    },
  };
  const result = await saveWorkflowDraftRevision(queryable, input);
  assert.equal(result.replayed, false);
  assert.equal(result.row.revision, 8);
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /revision=controlling\.cash_workflow_drafts\.revision\+1/);
  assert.equal(calls[0].parameters[7], 7);
});

test('akzeptiert einen identischen erneut zugestellten Autosave idempotent', async () => {
  const calls = [];
  const current = { id: 12, stage: 'finalize', state: { value: 1 }, revision: 8 };
  const queryable = {
    query: async (sql, parameters) => {
      calls.push({ sql, parameters });
      return calls.length === 1 ? { rows: [] } : { rows: [current] };
    },
  };
  const result = await saveWorkflowDraftRevision(queryable, input);
  assert.deepEqual(result, { row: current, replayed: true });
  assert.equal(calls.length, 2);
  assert.match(calls[1].sql, /stage=\$5 AND state=\$6::jsonb/);
  assert.deepEqual(calls[1].parameters, [4, '2026-11-06', 'day_close', 'active', 'finalize', '{"value":1}']);
});

test('meldet bei gleicher Revisionserwartung aber anderem Inhalt weiterhin Konflikt', async () => {
  const queryable = { query: async () => ({ rows: [] }) };
  const result = await saveWorkflowDraftRevision(queryable, input);
  assert.deepEqual(result, { row: null, replayed: false });
});
