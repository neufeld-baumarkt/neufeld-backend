function requireOpeningReplacement(activeOpening, requestedOpeningId) {
  if (!activeOpening) return null;
  const requested = Number(requestedOpeningId);
  if (!Number.isInteger(requested) || requested !== Number(activeOpening.id)) {
    throw Object.assign(new Error(`Lade ${activeOpening.drawer_number} ist noch ${activeOpening.employee_name} zugeordnet. Die Ablösung muss ausdrücklich bestätigt werden.`), {
      statusCode: 409,
      code: 'DRAWER_OCCUPIED',
      activeOpening,
    });
  }
  return Number(activeOpening.id);
}

module.exports = { requireOpeningReplacement };
