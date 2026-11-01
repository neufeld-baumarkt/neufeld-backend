async function deleteBusinessDayDrafts(queryable, { branchId, businessDate }) {
  const result = await queryable.query(`DELETE FROM controlling.cash_workflow_drafts
    WHERE filiale_id=$1 AND business_date=$2`, [branchId, businessDate]);
  return Number(result.rowCount || 0);
}

async function lockDraftBusinessDay(queryable, { branchId, businessDate }) {
  const result = await queryable.query(`SELECT status FROM core.cash_business_days
    WHERE branch_id=$1 AND business_date=$2
    FOR SHARE`, [branchId, businessDate]);
  return result.rows[0]?.status || null;
}

module.exports = { deleteBusinessDayDrafts, lockDraftBusinessDay };
