# Betragssuche: SQL-Eingrenzung

Stand: 20.11.2026. Basis Backend main 7c8f81855dcd1a10bac43e37eb35f1e3c1a130f9;
Frontend-Aufruf in src/components/cashflow/CashflowWeekDetailModal.jsx geprüft.
Die ursprünglichen PRs Backend #34 und Frontend #25 sind zusammengeführt.

## Befund

Der bisherige Betragspfad lädt alle positiven Ausgaben aktiver Ausgabenkategorien
mit Eintragstyp Betrag. Vollständige Detailzeilen werden in PostgreSQL sortiert,
übertragen, im Node-Prozess gehalten und erst dort nach Centbetrag und Avis-Summe
gefiltert. Laufzeit und Speicherbedarf wachsen deshalb auch ohne Treffer mit dem
berechtigten Gesamtbestand. Eine produktive Verlangsamung wurde nicht gemessen.

## Umgesetzter Vorschlag

Eine separate parametrisierte SQL-Abfrage ermittelt zunächst nur ID, Centbetrag
und normalisierte Avis-Nummer. GROUP BY / HAVING SUM bestimmt vollständige passende
Avis. Anschließend werden nur Detailzeilen passender Einzelbeträge oder dieser
Avis geladen und in bisheriger Reihenfolge ausgegeben. Ein internes Treffermerkmal
verhindert, dass ein isolierter Einzelrechnungstreffer ein falsches Teilavis bildet.
Die Avis-Trefferliste wird ausdrücklich materialisiert; ein Ausführungsplantest
sichert die einmalige Aggregation ab. Die Zugehörigkeit wird per IN-Teilabfrage
statt über einen wiederholt aggregierenden Join geprüft.
Das Merkmal erscheint nicht in der API-Antwort. Die Nummernsuche bleibt unverändert.

Erhalten bleiben Zahlbetrag vor Planbetrag (auch bei Zahlbetrag 0), positive Beträge,
aktive Ausgabenkategorien, null als Eintragstyp Betrag, Centvergleich, ECMAScript-
Whitespace-Bereinigung der Avis-Nummer, globale Avis-Gruppierung über Jahre und
Filialen, Ein-Rechnungs-Avis, Statusermittlung und gleichzeitige Einzel-/Avis-Treffer.
Es gibt absichtlich kein LIMIT vor der Aggregation oder auf Treffern: Damit würden
bisherige Ergebnisse entfallen oder Avis unvollständig werden.

## Wirkung und Grenzen

Der schmale Aggregationsdurchlauf bleibt O(N); die Datenbank muss weiterhin alle
berechtigten Avis-Positionen berücksichtigen. Das ist keine konstante Suchzeit und
keine nachgewiesene Reduktion der PostgreSQL-CPU-Zeit. Reduziert werden übertragene
Detaildaten, Detail-Sortierung und Verarbeitung im Backend auf die Trefferzahl M.
Bei sehr vielen echten Treffern bleibt auch die Antwort groß. Der materialisierte
CTE benötigt Speicher und kann auf temporäre Dateien ausweichen. PostgreSQL 12+
ist für AS MATERIALIZED erforderlich.

Ein alleiniger Betragsindex löst die vollständige Avis-Summierung nicht und wird
durch den bewusst gemeinsamen CTE auch nicht als direkter Betragssuchpfad genutzt.
Deshalb enthält dieser Vorschlag keine ungemessene Indexmigration.

Als zweite Stufe sind zwei getrennte SQL-Pfade sinnvoll: Einzelbeträge über einen
partiellen Ausdrucksindex auf ROUND(COALESCE(zahlbetrag,betrag)::numeric * 100),
Avis weiterhin per Aggregation. Ein schmaler partieller Index auf Kategorie, Avis
und effektivem Zahlbetrag kann den Aggregationsscan unterstützen; Nutzen und
Schreibkosten müssen anhand einer Datenkopie mit EXPLAIN (ANALYZE, BUFFERS) geprüft
werden. Kategorieaktivität ist dabei weiterhin per Join zu prüfen. Ein Suchfilter
auf Datum/Jahr oder Beträge vor der Avis-Summierung wäre semantisch falsch.

Falls auch die Aggregation zu teuer wird, ist eine transaktional gepflegte
Avis-Summentabelle die nächste Stufe. Sie muss Änderungen an Beträgen, Zahlungen,
Avis-Zuordnungen, Eintragstyp und Kategorieaktivität korrekt nachführen. Wegen
dieser zusätzlichen Konsistenzpflicht ist sie hier nicht eingeführt.

## Verifikation

`npm ci` und `node --test` führen auch einen isolierten PostgreSQL-Test über PGlite
(Entwicklungsabhängigkeit) aus. Dieser importiert weder db.js noch produktive
Zugangsdaten. Synthetisches Schema: betrag und zahlbetrag numeric(12,2); für
zahlbetrag ist dies durch Migration 027 belegt. Die ursprüngliche DDL von betrag
liegt im Repository nicht vor. Vor einem Rollout den tatsächlichen Spaltentyp
prüfen: Fließkomma oder zusätzliche Dezimalstellen erfordern eigene Grenztests
wegen unterschiedlicher JavaScript-/SQL-Rundung.

Die neue SQL-Abfrage wird für sechs Suchbeträge direkt mit dem bisherigen
Vollabfrage-plus-JavaScript-Verfahren verglichen, einschließlich vollständiger
Antwortobjekte und Reihenfolge. 10.000 zusätzliche irrelevante Buchungen sichern
die Eingrenzung ab. Beim Suchbetrag 100 Euro werden nur 8 Detailzeilen statt
10.011 berechtigter Zeilen geladen. EXPLAIN ANALYZE bestätigt die acht Ausgabezeilen.
Die dort ausgegebene synthetische Laufzeit ist kein Produktionsbenchmark.
Mit `CASHFLOW_TEST_EXPLAIN=1 node --test lib/cashflowAmountSearchSql.test.js`
lässt sich zusätzlich der vollständige Plan ausgeben.

Weitere Fälle: Skonto/Zahlbetrag, Null-/Nullbetrag, Gutschriften, inaktive Kategorien,
Einnahmen, Feiertage, Teilavis-Falschpositiv, Ein-Rechnungs-Avis, fehlende und leere
Avis-Nummern, Tab/NBSP, jahres-/filialübergreifende Avis, Cent-Summen und kein Treffer.

Produktive Daten, Schema, Indizes und Deployments wurden nicht verändert. Vor einem
Rollout auf einer isolierten Datenkopie PostgreSQL-Version, Spaltentypen, vorhandene
Indizes, reale Verteilung, parallele Suchlast, p95-Laufzeit und temporäre I/O prüfen.

Ausgeführte Prüfungen: Backend `node --test`: 267 Tests, 266 bestanden,
1 übersprungen, keine Fehler. Frontend
`node --test src/lib/cashflowAmountSearchUi.test.mjs`: 1 bestanden.
SQL-Regression mit zusätzlichem EXPLAIN ANALYZE separat erfolgreich ausgeführt.
