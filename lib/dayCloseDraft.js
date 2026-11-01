async function completeDayCloseDraft(queryable, { branchId, businessDate, userId }) {
  const result = await queryable.query(`UPDATE controlling.cash_workflow_drafts
    SET status='completed',closed_at=NOW(),updated_at=NOW(),updated_by_user_id=$3,revision=revision+1
    WHERE filiale_id=$1 AND business_date=$2 AND workflow_type='day_close' AND draft_key='active'
      AND status='active'
    RETURNING id,status,revision,closed_at`, [branchId, businessDate, userId]);
  return result.rows[0] || null;
}

module.exports = { completeDayCloseDraft };
