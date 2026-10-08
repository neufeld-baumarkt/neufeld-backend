'use strict';

function normalizeText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeMellerudArticleIdentity(article) {
  const supplierArticleNo = normalizeText(article?.supplier_article_no);
  const kundenArtNr = normalizeText(article?.kunden_art_nr);

  if (!supplierArticleNo) {
    throw new Error('Mellerud-Art.-Nr. fehlt');
  }
  if (!kundenArtNr) {
    throw new Error('Neufeld-Art.-Nr. fehlt');
  }
  if (supplierArticleNo.length > 100 || kundenArtNr.length > 100) {
    throw new Error('Artikelnummern dürfen höchstens 100 Zeichen enthalten');
  }

  return {
    supplier_article_no: supplierArticleNo,
    kunden_art_nr: kundenArtNr,
  };
}

function normalizeMellerudArticleMaster(article) {
  const identity = normalizeMellerudArticleIdentity(article);
  const ean = normalizeText(article?.ean);
  const name = normalizeText(article?.name);
  const veStueck = parsePositiveInteger(article?.ve_stueck);
  const ekEinzel = Number(article?.ek_einzel);
  const ekProKarton = Number(article?.ek_pro_karton);
  const rawSortIndex = article?.sort_index;
  const sortIndex = rawSortIndex === undefined || rawSortIndex === null || rawSortIndex === ''
    ? null
    : Number(rawSortIndex);

  if (!ean) throw new Error('EAN fehlt');
  if (!/^\d{8,14}$/.test(ean)) throw new Error('EAN muss aus 8 bis 14 Ziffern bestehen');
  if (!name) throw new Error('Artikelbezeichnung fehlt');
  if (name.length > 250) throw new Error('Artikelbezeichnung darf höchstens 250 Zeichen enthalten');
  if (veStueck === null) throw new Error('VE-Größe muss eine positive ganze Zahl sein');
  if (!Number.isFinite(ekEinzel) || ekEinzel <= 0) throw new Error('EK pro Stück fehlt oder ist ungültig');
  if (!Number.isFinite(ekProKarton) || ekProKarton <= 0) throw new Error('EK pro VE fehlt oder ist ungültig');
  if (Math.round(ekEinzel * 10000) / 10000 !== ekEinzel || Math.round(ekProKarton * 100) / 100 !== ekProKarton) {
    throw new Error('EK pro Stück erlaubt höchstens 4 und EK pro VE höchstens 2 Nachkommastellen');
  }
  if (sortIndex !== null && (!Number.isInteger(sortIndex) || sortIndex < 0)) {
    throw new Error('Sortierung muss eine nicht-negative ganze Zahl sein');
  }

  return {
    ...identity,
    ean,
    name,
    ve_stueck: veStueck,
    ek_einzel: ekEinzel,
    ek_pro_karton: ekProKarton,
    sort_index: sortIndex,
  };
}

function normalizeSupplierCode(value) {
  return normalizeText(value).toLowerCase();
}

function supportsMellerudWorkflow(supplier) {
  if (normalizeSupplierCode(supplier?.code) !== 'mellerud') return false;
  return new Set(['mellerud', 'mell_classic']).has(normalizeSupplierCode(supplier?.formular_typ));
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
    normalizeMellerudArticleIdentity(article);
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
  normalizeMellerudArticleIdentity,
  normalizeMellerudArticleMaster,
  normalizeSupplierCode,
  parsePositiveInteger,
  supportsMellerudWorkflow,
};
