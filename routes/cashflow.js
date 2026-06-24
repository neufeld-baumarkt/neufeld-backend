// routes/cashflow.js – Cashflow Endpoints
// Zweck:
// - Jahresübersicht für Cashflow-Dashboard bereitstellen
// - Kategorieauswertung für Cashflow-Dashboard bereitstellen
// - KPI-Auswertung für Cashflow-Dashboard bereitstellen
// - Buchungsübersicht bereitstellen
// - Fast-Booking-Buchungen speichern
// - Unternehmensbuchungen automatisch auf Filialen verteilen
// - Bestehende Buchungen aktualisieren
// - Bestehende Buchungen löschen
// - Optionaler bisKw-Filter für Zeitraumvergleiche
// - Zugriff nur für Admin, Supervisor und Geschäftsführer
// - Saldo wird serverseitig über cashflow.kategorien.typ berechnet

const express = require('express');
const router = express.Router();

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

const ALLOWED_ROLES = new Set(['Admin', 'Supervisor', 'Geschäftsführer']);
const ALLOWED_TAGS = new Set(['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']);

const STORED_FILIALEN = [
  'Unternehmen',
  'Verwaltung',
  'Ahaus',
  'Münster',
  'Telgte',
  'Vreden',
];

const SPLIT_FILIALEN = ['Ahaus', 'Münster', 'Telgte', 'Vreden'];

const ALLOWED_FAST_BOOKING_FILIALEN = new Set([
  ...STORED_FILIALEN,
  'Unternehmen',
]);

const ALLOWED_STORED_FILIALEN = new Set(STORED_FILIALEN);
const ALLOWED_EINTRAG_TYPEN = new Set(['betrag', 'feiertag']);
const ALLOWED_STATUS = new Set(['angekuendigt', 'gebucht']);

function requireCashflowAccess(req, res, next) {
  const role = req.user?.role;

  if (!ALLOWED_ROLES.has(role)) {
    return res.status(403).json({
      message:
        'Zugriff verweigert. Erforderliche Rolle: Admin, Supervisor oder Geschäftsführer.',
    });
  }

  next();
}

function isValidUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(value || '').trim()
  );
}

function splitAmountToFilialen(betrag) {
  const cents = Math.round(Number(betrag) * 100);
  const base = Math.floor(cents / SPLIT_FILIALEN.length);
  const remainder = cents % SPLIT_FILIALEN.length;

  return SPLIT_FILIALEN.map((filiale, index) => ({
    filiale,
    betrag: (base + (index < remainder ? 1 : 0)) / 100,
  }));
}

function parseJahrParam(req, res) {
  const jahr = Number(req.query?.jahr);

  if (!Number.isInteger(jahr) || jahr < 2000 || jahr > 2100) {
    res.status(400).json({
      message: 'Ungültiges Jahr. Erwartet wird z. B. ?jahr=2024',
    });
    return null;
  }

  return jahr;
}

function parseBisKwParam(req, res) {
  if (
    req.query?.bisKw === undefined ||
    req.query?.bisKw === null ||
    req.query?.bisKw === ''
  ) {
    return null;
  }

  const bisKw = Number(req.query.bisKw);

  if (!Number.isInteger(bisKw) || bisKw < 1 || bisKw > 53) {
    res.status(400).json({
      message: 'Ungültige bisKw. Erwartet wird z. B. &bisKw=18',
    });
    return false;
  }

  return bisKw;
}

function buildBisKwFilter(bisKw, params) {
  if (bisKw === null) return '';

  params.push(bisKw);
  return `AND b.kw <= $${params.length}`;
}

function parseFastBookingPayload(req, res) {
  const jahr = Number(req.body?.jahr);
  const kw = Number(req.body?.kw);
  const tag = String(req.body?.tag || '').trim();
  const kategorieId = Number(req.body?.kategorie_id ?? req.body?.kategorieId);
  const filiale = String(req.body?.filiale || '').trim();
  const eintragTyp = String(req.body?.eintrag_typ || 'betrag').trim();
  const notizRaw = req.body?.notiz;
  const notiz =
    notizRaw === undefined || notizRaw === null || String(notizRaw).trim() === ''
      ? null
      : String(notizRaw).trim();

  if (!Number.isInteger(jahr) || jahr < 2000 || jahr > 2100) {
    res.status(400).json({ message: 'Ungültiges Jahr.' });
    return null;
  }

  if (!Number.isInteger(kw) || kw < 1 || kw > 53) {
    res.status(400).json({ message: 'Ungültige KW.' });
    return null;
  }

  if (!ALLOWED_TAGS.has(tag)) {
    res.status(400).json({
      message: 'Ungültiger Tag. Erlaubt sind Mo, Di, Mi, Do, Fr, Sa, So.',
    });
    return null;
  }

  if (!Number.isInteger(kategorieId) || kategorieId < 1) {
    res.status(400).json({ message: 'Ungültige Kategorie.' });
    return null;
  }

  if (!ALLOWED_FAST_BOOKING_FILIALEN.has(filiale)) {
    res.status(400).json({
      message:
        'Ungültige Filiale. Erlaubt sind Verwaltung, Unternehmen, Ahaus, Münster, Telgte und Vreden.',
    });
    return null;
  }

  if (!ALLOWED_EINTRAG_TYPEN.has(eintragTyp)) {
    res.status(400).json({
      message: 'Ungültiger Eintragstyp. Erlaubt sind betrag und feiertag.',
    });
    return null;
  }

  let betrag = 0;

  if (eintragTyp === 'betrag') {
    betrag = Number(req.body?.betrag);

    if (!Number.isFinite(betrag) || betrag <= 0) {
      res.status(400).json({
        message: 'Ungültiger Betrag. Erwartet wird eine Zahl größer 0.',
      });
      return null;
    }
  }

  if (eintragTyp === 'feiertag') {
    betrag = 0;
  }

  return {
    jahr,
    kw,
    tag,
    kategorieId,
    filiale,
    betrag,
    eintragTyp,
    notiz,
  };
}

function parseUpdateBuchungPayload(req, res) {
  const status =
    req.body?.status === undefined || req.body?.status === null
      ? undefined
      : String(req.body.status).trim();

  const notiz =
    req.body?.notiz === undefined ||
    req.body?.notiz === null ||
    String(req.body.notiz).trim() === ''
      ? null
      : String(req.body.notiz).trim();

  const filiale =
    req.body?.filiale === undefined || req.body?.filiale === null
      ? undefined
      : String(req.body.filiale).trim();

  const eintragTyp =
    req.body?.eintrag_typ === undefined || req.body?.eintrag_typ === null
      ? undefined
      : String(req.body.eintrag_typ).trim();

  const jahrRaw = req.body?.jahr;
  const jahrProvided = jahrRaw !== undefined && jahrRaw !== null && jahrRaw !== '';
  const jahr = jahrProvided ? Number(jahrRaw) : undefined;

  const kwRaw = req.body?.kw;
  const kwProvided = kwRaw !== undefined && kwRaw !== null && kwRaw !== '';
  const kw = kwProvided ? Number(kwRaw) : undefined;

  const tag =
    req.body?.tag === undefined || req.body?.tag === null
      ? undefined
      : String(req.body.tag).trim();

  const betragRaw = req.body?.betrag;
  const betragProvided =
    betragRaw !== undefined && betragRaw !== null && betragRaw !== '';
  const betrag = betragProvided ? Number(betragRaw) : undefined;

  if (status !== undefined && !ALLOWED_STATUS.has(status)) {
    res.status(400).json({
      message: 'Ungültiger Status. Erlaubt sind angekuendigt und gebucht.',
    });
    return null;
  }

  if (filiale !== undefined && !ALLOWED_STORED_FILIALEN.has(filiale)) {
    res.status(400).json({
      message:
        'Ungültige Filiale. Erlaubt sind Unternehmen, Verwaltung, Ahaus, Münster, Telgte und Vreden.',
    });
    return null;
  }

  if (eintragTyp !== undefined && !ALLOWED_EINTRAG_TYPEN.has(eintragTyp)) {
    res.status(400).json({
      message: 'Ungültiger Eintragstyp. Erlaubt sind betrag und feiertag.',
    });
    return null;
  }

  if (jahrProvided && (!Number.isInteger(jahr) || jahr < 2000 || jahr > 2100)) {
    res.status(400).json({ message: 'Ungültiges Jahr.' });
    return null;
  }

  if (kwProvided && (!Number.isInteger(kw) || kw < 1 || kw > 53)) {
    res.status(400).json({ message: 'Ungültige KW.' });
    return null;
  }

  if (tag !== undefined && !ALLOWED_TAGS.has(tag)) {
    res.status(400).json({
      message: 'Ungültiger Tag. Erlaubt sind Mo, Di, Mi, Do, Fr, Sa, So.',
    });
    return null;
  }

  if (betragProvided && (!Number.isFinite(betrag) || betrag < 0)) {
    res.status(400).json({
      message: 'Ungültiger Betrag. Erwartet wird eine Zahl größer oder gleich 0.',
    });
    return null;
  }

  if (eintragTyp === 'betrag' && betragProvided && betrag <= 0) {
    res.status(400).json({
      message: 'Bei Eintragstyp betrag muss der Betrag größer 0 sein.',
    });
    return null;
  }

  if (
    status === undefined &&
    req.body?.notiz === undefined &&
    filiale === undefined &&
    eintragTyp === undefined &&
    !jahrProvided &&
    !kwProvided &&
    tag === undefined &&
    !betragProvided
  ) {
    res.status(400).json({
      message: 'Keine gültigen Änderungsdaten übergeben.',
    });
    return null;
  }

  return {
    status,
    notiz,
    filiale,
    eintragTyp,
    jahr,
    kw,
    tag,
    betrag,
    updateStatus: status !== undefined,
    updateNotiz: req.body?.notiz !== undefined,
    updateFiliale: filiale !== undefined,
    updateEintragTyp: eintragTyp !== undefined,
    updateJahr: jahrProvided,
    updateKw: kwProvided,
    updateTag: tag !== undefined,
    updateBetrag: betragProvided,
  };
}

// POST /api/cashflow/buchungen
router.post('/buchungen', verifyToken(), requireCashflowAccess, async (req, res) => {
  const payload = parseFastBookingPayload(req, res);
  if (payload === null) return;

  try {
    const kategorieCheck = await pool.query(
      `
      SELECT
        id,
        name,
        typ
      FROM cashflow.kategorien
      WHERE id = $1
        AND aktiv = true
      `,
      [payload.kategorieId]
    );

    if (kategorieCheck.rowCount === 0) {
      return res.status(400).json({
        message: 'Kategorie existiert nicht oder ist nicht aktiv.',
      });
    }

    const erstelltVon = req.user?.name || req.user?.id || 'system';

    const isEinnahme =
      String(kategorieCheck.rows[0].typ || '').trim() === 'Einnahme';

    const targets =
      payload.filiale === 'Unternehmen' && !isEinnahme
        ? splitAmountToFilialen(payload.betrag)
        : [{ filiale: payload.filiale, betrag: payload.betrag }];

    const insertedRows = [];

    await pool.query('BEGIN');

    try {
      for (const target of targets) {
        const result = await pool.query(
          `
          INSERT INTO cashflow.buchungen (
            jahr,
            kw,
            datum,
            tag,
            kategorie_id,
            betrag,
            quelle,
            quelle_zeile,
            erstellt_von,
            filiale,
            status,
            notiz,
            eintrag_typ
          )
          VALUES (
            $1,
            $2,
            NULL,
            $3,
            $4,
            $5,
            'fast_booking',
            NULL,
            $6,
            $7,
            'angekuendigt',
            $8,
            $9
          )
          RETURNING
            id,
            jahr,
            kw,
            datum,
            tag,
            kategorie_id,
            betrag,
            quelle,
            quelle_zeile,
            erstellt_von,
            erstellt_am,
            geaendert_am,
            filiale,
            status,
            notiz,
            eintrag_typ
          `,
          [
            payload.jahr,
            payload.kw,
            payload.tag,
            payload.kategorieId,
            payload.eintragTyp === 'feiertag' ? 0 : target.betrag,
            erstelltVon,
            target.filiale,
            payload.filiale === 'Unternehmen' && !isEinnahme
              ? payload.notiz || 'Unternehmensverteilung'
              : payload.notiz,
            payload.eintragTyp,
          ]
        );

        insertedRows.push({
          ...result.rows[0],
          kategorie: kategorieCheck.rows[0].name,
          typ: kategorieCheck.rows[0].typ,
        });
      }

      await pool.query('COMMIT');
    } catch (err) {
      await pool.query('ROLLBACK');
      throw err;
    }

    return res.status(201).json({
      message:
        payload.filiale === 'Unternehmen' && !isEinnahme
          ? 'Cashflow-Unternehmensbuchung verteilt gespeichert.'
          : 'Cashflow-Buchung gespeichert.',
      buchung: insertedRows[0],
      buchungen: insertedRows,
    });
  } catch (err) {
    console.error('Fehler POST /api/cashflow/buchungen:', err);

    return res.status(500).json({
      message: 'Serverfehler beim Speichern der Cashflow-Buchung.',
    });
  }
});

// PATCH /api/cashflow/buchungen/:id
router.patch('/buchungen/:id', verifyToken(), requireCashflowAccess, async (req, res) => {
  const id = String(req.params?.id || '').trim();

  if (!isValidUuid(id)) {
    return res.status(400).json({
      message: 'Ungültige Buchungs-ID.',
    });
  }

  const payload = parseUpdateBuchungPayload(req, res);
  if (payload === null) return;

  try {
    const result = await pool.query(
      `
      UPDATE cashflow.buchungen b
      SET
        status = CASE WHEN $2::boolean THEN $3 ELSE b.status END,
        notiz = CASE WHEN $4::boolean THEN $5 ELSE b.notiz END,
        filiale = CASE WHEN $6::boolean THEN $7 ELSE b.filiale END,
        eintrag_typ = CASE WHEN $8::boolean THEN $9 ELSE b.eintrag_typ END,
        jahr = CASE WHEN $10::boolean THEN $11 ELSE b.jahr END,
        kw = CASE WHEN $12::boolean THEN $13 ELSE b.kw END,
        tag = CASE WHEN $14::boolean THEN $15 ELSE b.tag END,
        betrag = CASE
          WHEN $8::boolean AND $9 = 'feiertag' THEN 0
          WHEN $16::boolean THEN $17
          ELSE b.betrag
        END,
        geaendert_am = NOW()
      FROM cashflow.kategorien k
      WHERE b.id = $1
        AND k.id = b.kategorie_id
        AND k.aktiv = true
      RETURNING
        b.id,
        b.jahr,
        b.kw,
        b.datum,
        b.tag,
        b.kategorie_id,
        k.name AS kategorie,
        k.typ,
        b.betrag,
        b.quelle,
        b.quelle_zeile,
        b.erstellt_von,
        b.erstellt_am,
        b.geaendert_am,
        b.filiale,
        b.status,
        b.notiz,
        b.eintrag_typ
      `,
      [
        id,
        payload.updateStatus,
        payload.status || null,
        payload.updateNotiz,
        payload.notiz,
        payload.updateFiliale,
        payload.filiale || null,
        payload.updateEintragTyp,
        payload.eintragTyp || null,
        payload.updateJahr,
        payload.jahr ?? null,
        payload.updateKw,
        payload.kw ?? null,
        payload.updateTag,
        payload.tag || null,
        payload.updateBetrag,
        payload.betrag ?? null,
      ]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        message: 'Cashflow-Buchung nicht gefunden.',
      });
    }

    return res.json({
      message: 'Cashflow-Buchung aktualisiert.',
      buchung: result.rows[0],
    });
  } catch (err) {
    console.error('Fehler PATCH /api/cashflow/buchungen/:id:', err);

    return res.status(500).json({
      message: 'Serverfehler beim Aktualisieren der Cashflow-Buchung.',
    });
  }
});

// DELETE /api/cashflow/buchungen/:id
router.delete('/buchungen/:id', verifyToken(), requireCashflowAccess, async (req, res) => {
  const id = String(req.params?.id || '').trim();

  if (!isValidUuid(id)) {
    return res.status(400).json({
      message: 'Ungültige Buchungs-ID.',
    });
  }

  try {
    const result = await pool.query(
      `
      DELETE FROM cashflow.buchungen b
      USING cashflow.kategorien k
      WHERE b.id = $1
        AND k.id = b.kategorie_id
        AND k.aktiv = true
      RETURNING
        b.id,
        b.jahr,
        b.kw,
        b.datum,
        b.tag,
        b.kategorie_id,
        k.name AS kategorie,
        k.typ,
        b.betrag,
        b.quelle,
        b.quelle_zeile,
        b.erstellt_von,
        b.erstellt_am,
        b.geaendert_am,
        b.filiale,
        b.status,
        b.notiz,
        b.eintrag_typ
      `,
      [id]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        message: 'Cashflow-Buchung nicht gefunden.',
      });
    }

    return res.json({
      message: 'Cashflow-Buchung gelöscht.',
      buchung: result.rows[0],
    });
  } catch (err) {
    console.error('Fehler DELETE /api/cashflow/buchungen/:id:', err);

    return res.status(500).json({
      message: 'Serverfehler beim Löschen der Cashflow-Buchung.',
    });
  }
});

// GET /api/cashflow/jahresuebersicht?jahr=2024
router.get('/jahresuebersicht', verifyToken(), requireCashflowAccess, async (req, res) => {
  const jahr = parseJahrParam(req, res);
  if (jahr === null) return;

  const bisKw = parseBisKwParam(req, res);
  if (bisKw === false) return;

  try {
    const params = [jahr];
    const bisKwFilter = buildBisKwFilter(bisKw, params);

    const result = await pool.query(
      `
      SELECT
        b.kw,
        COALESCE(SUM(CASE WHEN k.typ = 'Einnahme' THEN b.betrag ELSE 0 END), 0)::numeric(12,2) AS einnahmen,
        COALESCE(SUM(CASE WHEN k.typ = 'Ausgabe' THEN b.betrag ELSE 0 END), 0)::numeric(12,2) AS ausgaben,
        COALESCE(SUM(
          CASE
            WHEN k.typ = 'Einnahme' THEN b.betrag
            WHEN k.typ = 'Ausgabe' THEN -b.betrag
            ELSE 0
          END
        ), 0)::numeric(12,2) AS saldo
      FROM cashflow.buchungen b
      JOIN cashflow.kategorien k
        ON k.id = b.kategorie_id
      WHERE b.jahr = $1
        AND k.aktiv = true
        ${bisKwFilter}
      GROUP BY b.kw
      ORDER BY b.kw
      `,
      params
    );

    const maxKw = bisKw || 53;

    const alleWochen = Array.from({ length: maxKw }, (_, index) => ({
      jahr,
      kw: index + 1,
      einnahmen: '0.00',
      ausgaben: '0.00',
      saldo: '0.00',
    }));

    const weekMap = new Map(
      result.rows.map((week) => [
        Number(week.kw),
        {
          jahr,
          ...week,
        },
      ])
    );

    const mergedWeeks = alleWochen.map((week) => weekMap.get(week.kw) || week);

    return res.json({
      jahr,
      bisKw,
      weeks: mergedWeeks,
    });
  } catch (err) {
    console.error('Fehler GET /api/cashflow/jahresuebersicht:', err);
    return res.status(500).json({
      message: 'Serverfehler bei Cashflow-Jahresübersicht.',
    });
  }
});

// GET /api/cashflow/buchungen?jahr=2024
router.get('/buchungen', verifyToken(), requireCashflowAccess, async (req, res) => {
  const jahr = parseJahrParam(req, res);
  if (jahr === null) return;

  const bisKw = parseBisKwParam(req, res);
  if (bisKw === false) return;

  try {
    const params = [jahr];
    const bisKwFilter = buildBisKwFilter(bisKw, params);

    const result = await pool.query(
      `
      SELECT
        b.id,
        b.jahr,
        b.kw,
        b.datum,
        b.tag,
        b.kategorie_id,
        k.name AS kategorie,
        k.typ,
        b.betrag,
        b.quelle,
        b.quelle_zeile,
        b.erstellt_von,
        b.erstellt_am,
        b.geaendert_am,
        b.filiale,
        b.status,
        b.notiz,
        b.eintrag_typ
      FROM cashflow.buchungen b
      JOIN cashflow.kategorien k
        ON k.id = b.kategorie_id
      WHERE b.jahr = $1
        AND k.aktiv = true
        ${bisKwFilter}
      ORDER BY
        b.kw DESC,
        b.datum DESC,
        k.sortierung ASC,
        b.id ASC
      `,
      params
    );

    return res.json({
      jahr,
      bisKw,
      anzahl: result.rows.length,
      buchungen: result.rows,
    });
  } catch (err) {
    console.error('Fehler GET /api/cashflow/buchungen:', err);

    return res.status(500).json({
      message: 'Serverfehler beim Laden der Cashflow-Buchungen.',
    });
  }
});

// GET /api/cashflow/kategorien?jahr=2024
router.get('/kategorien', verifyToken(), requireCashflowAccess, async (req, res) => {
  const jahr = parseJahrParam(req, res);
  if (jahr === null) return;

  const bisKw = parseBisKwParam(req, res);
  if (bisKw === false) return;

  try {
    const params = [jahr];
    const bisKwFilter = buildBisKwFilter(bisKw, params);

    const result = await pool.query(
      `
      WITH kategorien_summen AS (
        SELECT
          k.id,
          k.name AS kategorie,
          k.typ,
          k.sortierung,
          COALESCE(SUM(b.betrag), 0)::numeric(12,2) AS gesamt
        FROM cashflow.buchungen b
        JOIN cashflow.kategorien k
          ON k.id = b.kategorie_id
        WHERE b.jahr = $1
          AND k.aktiv = true
          ${bisKwFilter}
        GROUP BY
          k.id,
          k.name,
          k.typ,
          k.sortierung
      ),
      typ_summen AS (
        SELECT
          typ,
          COALESCE(SUM(gesamt), 0)::numeric(12,2) AS typ_gesamt
        FROM kategorien_summen
        GROUP BY typ
      )
      SELECT
        ks.id,
        ks.kategorie,
        ks.typ,
        ks.gesamt,
        CASE
          WHEN ts.typ_gesamt = 0 THEN 0::numeric(8,2)
          ELSE ROUND((ks.gesamt / ts.typ_gesamt) * 100, 2)::numeric(8,2)
        END AS anteil_prozent
      FROM kategorien_summen ks
      JOIN typ_summen ts
        ON ts.typ = ks.typ
      ORDER BY
        ks.sortierung,
        ks.id
      `,
      params
    );

    return res.json({
      jahr,
      bisKw,
      kategorien: result.rows,
    });
  } catch (err) {
    console.error('Fehler GET /api/cashflow/kategorien:', err);
    return res.status(500).json({
      message: 'Serverfehler bei Cashflow-Kategorienauswertung.',
    });
  }
});

// GET /api/cashflow/kpis?jahr=2024
router.get('/kpis', verifyToken(), requireCashflowAccess, async (req, res) => {
  const jahr = parseJahrParam(req, res);
  if (jahr === null) return;

  const bisKw = parseBisKwParam(req, res);
  if (bisKw === false) return;

  try {
    const params = [jahr];
    const bisKwFilter = buildBisKwFilter(bisKw, params);

    const result = await pool.query(
      `
      WITH wochen AS (
        SELECT
          b.kw,
          COALESCE(SUM(CASE WHEN k.typ = 'Einnahme' THEN b.betrag ELSE 0 END), 0)::numeric(12,2) AS einnahmen,
          COALESCE(SUM(CASE WHEN k.typ = 'Ausgabe' THEN b.betrag ELSE 0 END), 0)::numeric(12,2) AS ausgaben,
          COALESCE(SUM(
            CASE
              WHEN k.typ = 'Einnahme' THEN b.betrag
              WHEN k.typ = 'Ausgabe' THEN -b.betrag
              ELSE 0
            END
          ), 0)::numeric(12,2) AS saldo
        FROM cashflow.buchungen b
        JOIN cashflow.kategorien k
          ON k.id = b.kategorie_id
        WHERE b.jahr = $1
          AND k.aktiv = true
          ${bisKwFilter}
        GROUP BY b.kw
      ),
      aggregate AS (
        SELECT
          COALESCE(SUM(einnahmen), 0)::numeric(12,2) AS einnahmen,
          COALESCE(SUM(ausgaben), 0)::numeric(12,2) AS ausgaben,
          COALESCE(SUM(saldo), 0)::numeric(12,2) AS saldo,
          COALESCE(AVG(saldo), 0)::numeric(12,2) AS durchschnitt_saldo,
          COUNT(*)::int AS anzahl_wochen
        FROM wochen
      ),
      beste AS (
        SELECT
          kw AS beste_kw,
          saldo AS bester_saldo
        FROM wochen
        ORDER BY saldo DESC, kw ASC
        LIMIT 1
      ),
      schlechteste AS (
        SELECT
          kw AS schlechteste_kw,
          saldo AS schlechtester_saldo
        FROM wochen
        ORDER BY saldo ASC, kw ASC
        LIMIT 1
      )
      SELECT
        a.einnahmen,
        a.ausgaben,
        a.saldo,
        b.beste_kw,
        b.bester_saldo,
        s.schlechteste_kw,
        s.schlechtester_saldo,
        a.durchschnitt_saldo,
        a.anzahl_wochen
      FROM aggregate a
      LEFT JOIN beste b ON true
      LEFT JOIN schlechteste s ON true
      `,
      params
    );

    const row = result.rows?.[0] || {};

    return res.json({
      jahr,
      bisKw,
      kpis: {
        einnahmen: row.einnahmen || '0.00',
        ausgaben: row.ausgaben || '0.00',
        saldo: row.saldo || '0.00',
        beste_kw: row.beste_kw || null,
        bester_saldo: row.bester_saldo || null,
        schlechteste_kw: row.schlechteste_kw || null,
        schlechtester_saldo: row.schlechtester_saldo || null,
        durchschnitt_saldo: row.durchschnitt_saldo || '0.00',
        anzahl_wochen: row.anzahl_wochen || 0,
      },
    });
  } catch (err) {
    console.error('Fehler GET /api/cashflow/kpis:', err);
    return res.status(500).json({
      message: 'Serverfehler bei Cashflow-KPI-Auswertung.',
    });
  }
});

module.exports = router;