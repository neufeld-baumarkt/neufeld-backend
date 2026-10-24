async function findActivePinMatches(candidates, pin, compare) {
  const matches = [];
  const now = new Date();
  for (const candidate of candidates || []) {
    if (candidate.must_change_pin === true) continue;
    if (candidate.locked_until && new Date(candidate.locked_until) > now) continue;
    if (await compare(pin, candidate.pin_hash)) matches.push(candidate);
  }
  return matches;
}

module.exports = { findActivePinMatches };
