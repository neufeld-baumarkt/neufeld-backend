'use strict';

function normalizeText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeSupplierCode(value) {
  return normalizeText(value).toLowerCase();
}

function parsePositiveInteger(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function isValidIsoDate(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const [year, month, day] = value.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year
    && date.getUTCMonth() === month - 1
    && date.getUTCDate() === day;
}

function moneyToCents(value, label = 'Betrag') {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    throw new Error(`${label} fehlt oder ist ungültig`);
  }
  return Math.round(numeric * 100);
}

function centsToMoney(cents) {
  return Math.round(Number(cents)) / 100;
}

function canonicalBranch(value, branchMap) {
  const normalized = normalizeText(value);
  if (!normalized) return '';
  return branchMap.get(normalized.toLocaleLowerCase('de-DE')) || '';
}

/**
 * Verteilt einen ganzzahligen Centbetrag proportional zu Stückzahlen.
 * Größte Nachkommareste erhalten die verbleibenden Cent. So bleibt die
 * Summe exakt erhalten und Rundungsfehler können nie Geld erzeugen.
 */
function allocateCents(totalCents, buckets) {
  const totalUnits = buckets.reduce((sum, bucket) => sum + bucket.units, 0);
  if (!Number.isInteger(totalCents) || totalCents < 0 || totalUnits <= 0) {
    throw new Error('Cent-Verteilung ist ungültig');
  }

  const allocations = buckets.map((bucket, index) => {
    const numerator = totalCents * bucket.units;
    return {
      ...bucket,
      index,
      cents: Math.floor(numerator / totalUnits),
      remainder: numerator % totalUnits,
    };
  });

  let remaining = totalCents - allocations.reduce((sum, item) => sum + item.cents, 0);
  const priority = [...allocations].sort((a, b) => b.remainder - a.remainder || a.index - b.index);
  for (let index = 0; index < remaining; index += 1) {
    priority[index % priority.length].cents += 1;
  }

  return allocations.sort((a, b) => a.index - b.index);
}

function calculateOrderPlan({
  articles,
  requestedPositions,
  splitDetails,
  sourceFiliale,
  validBranches,
  minimumOrderVe = 2,
}) {
  const articleMap = new Map((articles || []).map((article) => [String(article.id), article]));
  const branchMap = new Map((validBranches || []).map((branch) => {
    const canonical = normalizeText(branch);
    return [canonical.toLocaleLowerCase('de-DE'), canonical];
  }));
  const source = canonicalBranch(sourceFiliale, branchMap);

  if (!source) throw new Error('Die bestellende Filiale ist nicht aktiv oder nicht zulässig');
  if (!Array.isArray(requestedPositions) || requestedPositions.length === 0) {
    throw new Error('Mindestens eine Bestellposition ist erforderlich');
  }

  const positions = [];
  const seenArticleIds = new Set();
  let totalVe = 0;

  requestedPositions.forEach((requested, index) => {
    const articleId = normalizeText(requested?.articleId);
    const quantityVe = parsePositiveInteger(requested?.menge_kartons);
    if (!articleId || !articleMap.has(articleId)) {
      throw new Error(`Bestellposition ${index + 1}: Artikel ist ungültig oder nicht bestellbar`);
    }
    if (seenArticleIds.has(articleId)) {
      throw new Error(`Artikel ${articleId} ist mehrfach in der Bestellung enthalten`);
    }
    if (quantityVe === null) {
      throw new Error(`Bestellposition ${index + 1}: Es sind nur ganze positive VE zulässig`);
    }

    const article = articleMap.get(articleId);
    const veSize = parsePositiveInteger(article.ve_stueck);
    if (veSize === null) throw new Error(`Artikel ${articleId}: VE-Größe ist ungültig`);
    const pricePerVeCents = moneyToCents(article.ek_pro_karton, `Artikel ${articleId}: VE-EK`);

    seenArticleIds.add(articleId);
    totalVe += quantityVe;
    positions.push({
      article,
      articleId,
      quantityVe,
      veSize,
      totalPieces: quantityVe * veSize,
      pricePerVeCents,
      totalCents: quantityVe * pricePerVeCents,
    });
  });

  const minimumVe = parsePositiveInteger(minimumOrderVe) || 2;
  if (totalVe < minimumVe) {
    throw new Error(`Die Bestellung muss insgesamt mindestens ${minimumVe} VE enthalten`);
  }

  const rawSplitDetails = splitDetails && typeof splitDetails === 'object' && !Array.isArray(splitDetails)
    ? splitDetails
    : {};
  for (const articleId of Object.keys(rawSplitDetails)) {
    if (!seenArticleIds.has(articleId)) {
      throw new Error(`Split enthält einen nicht bestellten Artikel: ${articleId}`);
    }
  }

  const targetTotals = new Map();
  const articleSplits = {};
  let sourceTotalCents = 0;

  for (const position of positions) {
    const rawRows = Array.isArray(rawSplitDetails[position.articleId]?.zeilen)
      ? rawSplitDetails[position.articleId].zeilen
      : [];
    const seenTargets = new Set();
    const rows = [];
    let distributedPieces = 0;

    rawRows.forEach((rawRow, index) => {
      const target = canonicalBranch(rawRow?.target_filiale, branchMap);
      if (!target) throw new Error(`Split ${position.articleId}, Zeile ${index + 1}: Ziel-Filiale ist ungültig`);
      if (target === source) throw new Error(`Split ${position.articleId}: Quell- und Ziel-Filiale müssen verschieden sein`);
      if (seenTargets.has(target)) throw new Error(`Split ${position.articleId}: Filiale ${target} ist doppelt enthalten`);

      const unit = rawRow?.einheit === 'karton' ? 'karton' : rawRow?.einheit === 'stueck' ? 'stueck' : '';
      const quantity = parsePositiveInteger(rawRow?.menge);
      if (!unit || quantity === null) {
        throw new Error(`Split ${position.articleId}, Zeile ${index + 1}: Einheit oder Menge ist ungültig`);
      }

      const pieces = unit === 'karton' ? quantity * position.veSize : quantity;
      distributedPieces += pieces;
      if (distributedPieces > position.totalPieces) {
        throw new Error(`Split ${position.articleId}: Verteilung überschreitet die bestellte Stückzahl`);
      }

      seenTargets.add(target);
      rows.push({ target_filiale: target, einheit: unit, menge: quantity, menge_stueck: pieces });
    });

    const sourcePieces = position.totalPieces - distributedPieces;
    const buckets = [
      ...rows.map((row) => ({ key: row.target_filiale, units: row.menge_stueck })),
      ...(sourcePieces > 0 ? [{ key: source, units: sourcePieces }] : []),
    ];
    const allocations = allocateCents(position.totalCents, buckets.length > 0
      ? buckets
      : [{ key: source, units: position.totalPieces }]);

    rows.forEach((row, index) => {
      const cents = allocations[index].cents;
      row.betrag_netto = centsToMoney(cents);
      targetTotals.set(row.target_filiale, (targetTotals.get(row.target_filiale) || 0) + cents);
    });
    const sourceAllocation = allocations.find((item) => item.key === source);
    const sourceCents = sourceAllocation?.cents || 0;
    sourceTotalCents += sourceCents;

    if (rows.length > 0) {
      articleSplits[position.articleId] = {
        source_filiale: source,
        bestellte_ve: position.quantityVe,
        ve_groesse: position.veSize,
        gesamt_stueck: position.totalPieces,
        ve_preis_netto: centsToMoney(position.pricePerVeCents),
        zeilen: rows,
        rest: {
          filiale: source,
          menge_stueck: sourcePieces,
          betrag_netto: centsToMoney(sourceCents),
        },
      };
    }
  }

  const totalCents = positions.reduce((sum, position) => sum + position.totalCents, 0);
  const budgetSplits = Array.from(targetTotals.entries())
    .filter(([, cents]) => cents > 0)
    .map(([filiale, cents]) => ({ filiale, betrag: centsToMoney(cents) }));

  if (sourceTotalCents + budgetSplits.reduce((sum, item) => sum + Math.round(item.betrag * 100), 0) !== totalCents) {
    throw new Error('Interne Splitberechnung ist nicht centgenau ausgeglichen');
  }

  return {
    sourceFiliale: source,
    totalVe,
    totalCents,
    totalNet: centsToMoney(totalCents),
    sourceNet: centsToMoney(sourceTotalCents),
    positions,
    budgetSplits,
    splitSnapshot: budgetSplits.length > 0
      ? {
          mode: 'split',
          source_filiale: source,
          gesamtbetrag: centsToMoney(totalCents),
          artikel: articleSplits,
          targets: budgetSplits,
          rest: { filiale: source, betrag: centsToMoney(sourceTotalCents) },
        }
      : {
          mode: 'single',
          filiale: source,
          betrag: centsToMoney(totalCents),
        },
  };
}

module.exports = {
  allocateCents,
  calculateOrderPlan,
  centsToMoney,
  isValidIsoDate,
  normalizeSupplierCode,
  parsePositiveInteger,
};
