const CASH_BRANCH_HEADER = 'x-cash-branch';

const ALLOWED_CORS_HEADERS = Object.freeze([
  'Content-Type',
  'Authorization',
  'x-filiale',
  'x-file-name',
  CASH_BRANCH_HEADER,
]);

module.exports = { ALLOWED_CORS_HEADERS, CASH_BRANCH_HEADER };
