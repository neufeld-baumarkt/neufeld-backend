const CASH_BRANCH_HEADER = 'x-cash-branch';
const CASH_BUSINESS_DATE_HEADER = 'x-cash-business-date';

const ALLOWED_CORS_HEADERS = Object.freeze([
  'Content-Type',
  'Authorization',
  'x-filiale',
  'x-file-name',
  CASH_BRANCH_HEADER,
  CASH_BUSINESS_DATE_HEADER,
]);

module.exports = { ALLOWED_CORS_HEADERS, CASH_BRANCH_HEADER, CASH_BUSINESS_DATE_HEADER };
