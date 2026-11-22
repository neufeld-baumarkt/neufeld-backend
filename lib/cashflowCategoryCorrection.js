function validateCategoryCorrection({ currentCategoryId, targetCategoryId, currentType, targetType }) {
  if (!Number.isInteger(Number(targetCategoryId)) || Number(targetCategoryId) < 1) {
    return 'Ungültige Zielkategorie.';
  }

  if (String(currentType || '') !== String(targetType || '')) {
    return 'Eine Buchung darf nur innerhalb desselben Buchungstyps verschoben werden.';
  }

  return '';
}

module.exports = { validateCategoryCorrection };
