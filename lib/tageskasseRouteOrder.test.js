const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

test('V2.1 Tageskasse routes are mounted before the legacy safe-count fallback', () => {
  const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const v21Mount = "app.use('/api/tageskasse', tageskasseRoutes);";
  const legacyMount = "app.use('/api/tageskasse/safe-counts', require('./routes/safeCounts'));";
  const v21Index = serverSource.indexOf(v21Mount);
  const legacyIndex = serverSource.indexOf(legacyMount);

  assert.notEqual(v21Index, -1, 'V2.1 Tageskasse router is not mounted');
  assert.notEqual(legacyIndex, -1, 'legacy safe-count fallback is not mounted');
  assert.ok(v21Index < legacyIndex, 'legacy safe-count router shadows the V2.1 endpoints');
});
