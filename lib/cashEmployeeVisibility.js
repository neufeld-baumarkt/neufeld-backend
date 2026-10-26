function visibleCashEmployees(employees, branchId) {
  const selectedBranchId = Number(branchId);
  return employees.filter((employee) => (
    employee.filiale_id == null || Number(employee.filiale_id) === selectedBranchId
  ));
}

module.exports = { visibleCashEmployees };
