async function saveWorkflowDraftRevision(queryable, {
  branchId,
  businessDate,
  workflowType,
  draftKey,
  stage,
  serializedState,
  userId,
  expectedRevision,
}) {
  const parameters = [
    branchId,
    businessDate,
    workflowType,
    draftKey,
    stage,
    serializedState,
    userId,
    expectedRevision,
  ];
  const saved = await queryable.query(`INSERT INTO controlling.cash_workflow_drafts
    (filiale_id,business_date,workflow_type,draft_key,stage,state,created_by_user_id,updated_by_user_id)
    VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$7)
    ON CONFLICT (filiale_id,business_date,workflow_type,draft_key) WHERE status='active'
    DO UPDATE SET stage=EXCLUDED.stage,state=EXCLUDED.state,
      updated_by_user_id=EXCLUDED.updated_by_user_id,updated_at=NOW(),revision=controlling.cash_workflow_drafts.revision+1
    WHERE controlling.cash_workflow_drafts.revision=$8
    RETURNING *`, parameters);
  if (saved.rows[0]) return { row: saved.rows[0], replayed: false };

  // Mobile browsers, proxies and unstable networks may deliver the same PUT
  // twice. Accept only an exactly equal persisted stage and JSONB state as an
  // idempotent replay; genuinely different content remains a hard conflict.
  const replay = await queryable.query(`SELECT * FROM controlling.cash_workflow_drafts
    WHERE filiale_id=$1 AND business_date=$2 AND workflow_type=$3 AND draft_key=$4
      AND status='active' AND stage=$5 AND state=$6::jsonb
    LIMIT 1`, parameters.slice(0, 6));
  return replay.rows[0]
    ? { row: replay.rows[0], replayed: true }
    : { row: null, replayed: false };
}

module.exports = { saveWorkflowDraftRevision };
