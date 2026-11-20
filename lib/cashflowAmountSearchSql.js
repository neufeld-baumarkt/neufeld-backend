const { buildAmountSuggestions } = require('./cashflowAmountSearch');

// ECMAScript String.trim whitespace, also used by buildAmountSuggestions.
const trimCharacters = '\u0009\u000a\u000b\u000c\u000d \u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000\ufeff';

// Aggregate narrow rows, then fetch full details only for actual matches.
// Do not limit/filter individual amounts before summing an entire eligible Avis.
const amountSearchQuery = `
WITH eligible AS MATERIALIZED (
  SELECT b.id,
         ROUND(COALESCE(b.zahlbetrag, b.betrag)::numeric * 100) AS cents,
         NULLIF(BTRIM(b.avis_nummer, '${trimCharacters}'), '') AS avis_key
  FROM cashflow.buchungen b
  JOIN cashflow.kategorien k ON k.id = b.kategorie_id
  WHERE k.aktiv = true AND k.typ = 'Ausgabe'
    AND COALESCE(b.eintrag_typ, 'betrag') = 'betrag'
    AND COALESCE(b.zahlbetrag, b.betrag) > 0
), usable AS (
  SELECT * FROM eligible WHERE cents > 0 AND cents <= 9007199254740991
), matching_avis AS MATERIALIZED (
  SELECT avis_key FROM usable WHERE avis_key IS NOT NULL
  GROUP BY avis_key HAVING SUM(cents) = $1::numeric
), matches AS MATERIALIZED (
  SELECT u.id,
         COALESCE(u.avis_key IN (SELECT avis_key FROM matching_avis), false) AS amount_avis_match
  FROM usable u
  WHERE u.cents = $1::numeric
     OR u.avis_key IN (SELECT avis_key FROM matching_avis)
)
SELECT
        ma.amount_avis_match,
        b.id,
        b.jahr,
        b.kw,
        b.datum,
        b.tag,
        b.kategorie_id,
        k.name AS kategorie,
        k.typ,
        b.betrag,
        b.filiale,
        b.status,
        b.notiz,
        b.eintrag_typ,
        b.rechnungsnummer,
        b.rechnungsdatum,
        b.avis_nummer,
        b.zahlungsart,
        b.buchungsgruppe_id,
        b.rechnungsbetrag_brutto,
        b.skonto_prozent,
        b.skonto_genutzt,
        b.zahlungsdatum,
        b.zahlbetrag,
        b.erstellt_von,
        b.erstellt_am,
        b.geaendert_am
      FROM matches ma
      JOIN cashflow.buchungen b ON b.id = ma.id
      JOIN cashflow.kategorien k ON k.id = b.kategorie_id
      ORDER BY b.avis_nummer NULLS LAST, b.rechnungsdatum NULLS LAST,
               b.rechnungsnummer NULLS LAST, b.id
`;

function buildSqlAmountSuggestions(rows, targetCents) {
  const clean = ({ amount_avis_match, ...row }) => row;
  // A matching invoice alone must never create a partial Avis suggestion.
  const avisRows = rows.filter((row) => row.amount_avis_match).map(clean);
  return {
    avis: buildAmountSuggestions(avisRows, targetCents).avis,
    rechnungen: buildAmountSuggestions(rows.map(clean), targetCents).rechnungen,
  };
}

module.exports = { amountSearchQuery, buildSqlAmountSuggestions };
