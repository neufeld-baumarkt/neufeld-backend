const express = require('express');

const router = express.Router();

const verifyToken = require('../middleware/verifyToken');
const db = require('../db');

/**
 * Hilfsfunktion:
 * - akzeptiert nur YYYY-MM-DD
 * - verhindert stilles "irgendwie parsebar"
 */
function isValidIsoDate(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function normalizeFiliale(value) {
  if (typeof value !== 'string') return null;
  const t = value.trim();
  return t ? t : null;
}

function parseNumericSafe(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseOptionalInt(value) {
  if (value === undefined || value === null || value === '') return null;
  const n = Number(value);
  return Number.isInteger(n) ? n : NaN;
}

function canReadAllOrders(role) {
  return ['Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1'].includes(role);
}

async function resolveBudgetYearWeek(client, dateStr) {
  const result = await client.query(
    `
      SELECT
        EXTRACT(ISOYEAR FROM $1::date)::int AS jahr,
        EXTRACT(WEEK FROM $1::date)::int AS kw
    `,
    [dateStr]
  );

  return {
    jahr: result.rows[0].jahr,
    kw: result.rows[0].kw,
  };
}

async function ensureWeekBudget(client, filiale, jahr, kw) {
  const upsert = await client.query(
    `
      INSERT INTO budget.week_budgets (
        filiale,
        jahr,
        kw,
        prozentsatz_snapshot,
        freigegeben,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        COALESCE((SELECT prozentsatz FROM budget.week_rules WHERE jahr = $2 AND kw = $3 LIMIT 1), 0),
        false,
        NOW(),
        NOW()
      )
      ON CONFLICT (filiale, jahr, kw)
      DO UPDATE SET
        prozentsatz_snapshot = CASE
          WHEN budget.week_budgets.prozentsatz_snapshot = 0 THEN EXCLUDED.prozentsatz_snapshot
          ELSE budget.week_budgets.prozentsatz_snapshot
        END,
        updated_at = NOW()
      RETURNING id
    `,
    [filiale, jahr, kw]
  );

  return upsert.rows[0].id;
}

/**
 * GET /api/bestellungen/form
 * Zweck:
 * - liefert die bisherige Basis-/Form-Struktur
 * - getrennt vom echten Listen-Read
 */
router.get('/form', verifyToken(), async (req, res) => {
  try {
    const { id, name, role, filiale } = req.user || {};

    const result = await db.query(
      'SELECT COUNT(*) AS count FROM "order".order_suppliers'
    );

    const supplierCount = result.rows?.[0]?.count ?? null;

    return res.json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Bestellformular-Basis erfolgreich geladen',
      stage: 'phase-2-read-form',
      user: {
        id: id ?? null,
        name: name ?? null,
        role: role ?? null,
        filiale: filiale ?? null,
      },
      permissions: {
        authenticated: true,
        canRead: true,
        canWrite: true,
      },
      filters: {
        jahr: null,
        kw: null,
        filiale: filiale ?? null,
      },
      form: {
        kopf: {
          bestellung_id: null,
          filiale: filiale ?? null,
          jahr: null,
          kw: null,
          lieferant: null,
          bestelldatum: null,
          bemerkung: null,
          status: 'saved',
        },
        positionen: [],
      },
      meta: {
        dbConnected: true,
        supplierCount: supplierCount,
        source: 'db-read-form',
        nextStep: 'write-order-save',
      },
    });
  } catch (err) {
    console.error('GET /api/bestellungen/form Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * GET /api/bestellungen
 * Zweck:
 * - echter Listen-Read für bestehende Bestellungen
 * - filterbar über jahr / kw / filiale
 * - Filiale sieht nur eigene Bestellungen
 * - Superuser sieht alle Bestellungen
 */
router.get('/', verifyToken(), async (req, res) => {
  try {
    const { id, name, role, filiale: userFiliale } = req.user || {};
    const canReadAll = canReadAllOrders(role);

    const jahr = parseOptionalInt(req.query?.jahr);
    const kw = parseOptionalInt(req.query?.kw);
    const requestedFiliale = normalizeFiliale(req.query?.filiale);

    if (Number.isNaN(jahr)) {
      return res.status(400).json({ message: 'jahr ist ungültig' });
    }

    if (Number.isNaN(kw)) {
      return res.status(400).json({ message: 'kw ist ungültig' });
    }

    if (jahr !== null && (jahr < 2000 || jahr > 2100)) {
      return res.status(400).json({ message: 'jahr ist außerhalb des erlaubten Bereichs' });
    }

    if (kw !== null && (kw < 1 || kw > 53)) {
      return res.status(400).json({ message: 'kw ist außerhalb des erlaubten Bereichs' });
    }

    if (!canReadAll && requestedFiliale && requestedFiliale !== userFiliale) {
      return res.status(403).json({ message: 'Kein Zugriff auf fremde Filial-Bestellungen' });
    }

    const effectiveFiliale = canReadAll
      ? requestedFiliale
      : (userFiliale ?? null);

    const params = [];
    const whereParts = [];

    if (jahr !== null) {
      params.push(jahr);
      whereParts.push(`EXTRACT(ISOYEAR FROM o.bestelldatum)::int = $${params.length}`);
    }

    if (kw !== null) {
      params.push(kw);
      whereParts.push(`EXTRACT(WEEK FROM o.bestelldatum)::int = $${params.length}`);
    }

    if (effectiveFiliale && effectiveFiliale !== 'Alle') {
      params.push(effectiveFiliale);
      whereParts.push(`o.filiale = $${params.length}`);
    }

    const whereSql = whereParts.length > 0
      ? `WHERE ${whereParts.join(' AND ')}`
      : '';

    const result = await db.query(
      `
      SELECT
        o.id,
        o.filiale,
        o.ordered_by_name,
        o.bestelldatum,
        EXTRACT(ISOYEAR FROM o.bestelldatum)::int AS jahr,
        EXTRACT(WEEK FROM o.bestelldatum)::int AS kw,
        o.status,
        o.gesamtsumme_netto,
        o.budget_booking_id,
        o.supplier_formular_typ_snapshot,
        o.created_at,
        o.updated_at,
        s.id AS supplier_id,
        s.name AS supplier_name,
        s.code AS supplier_code,
        s.formular_typ AS supplier_formular_typ_live,
        COALESCE(p.position_count, 0)::int AS position_count
      FROM "order".order_orders o
      INNER JOIN "order".order_suppliers s
        ON s.id = o.supplier_id
      LEFT JOIN (
        SELECT
          order_id,
          COUNT(*) AS position_count
        FROM "order".order_order_positions
        GROUP BY order_id
      ) p
        ON p.order_id = o.id
      ${whereSql}
      ORDER BY o.bestelldatum DESC, o.created_at DESC
      `,
      params
    );

    return res.json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Bestellungen erfolgreich geladen',
      stage: 'phase-3-read-orders',
      user: {
        id: id ?? null,
        name: name ?? null,
        role: role ?? null,
        filiale: userFiliale ?? null,
      },
      permissions: {
        authenticated: true,
        canRead: true,
        canWrite: true,
        canReadAll,
      },
      filters: {
        jahr,
        kw,
        filiale: effectiveFiliale ?? null,
      },
      count: result.rows.length,
      items: result.rows.map((row) => ({
        id: row.id,
        filiale: row.filiale,
        ordered_by_name: row.ordered_by_name,
        bestelldatum: row.bestelldatum,
        jahr: row.jahr,
        kw: row.kw,
        status: row.status,
        gesamtsumme_netto: row.gesamtsumme_netto,
        budget_booking_id: row.budget_booking_id,
        supplier: {
          id: row.supplier_id,
          name: row.supplier_name,
          code: row.supplier_code,
          formular_typ: row.supplier_formular_typ_live || row.supplier_formular_typ_snapshot || null,
        },
        position_count: row.position_count,
        created_at: row.created_at,
        updated_at: row.updated_at,
      })),
      meta: {
        source: 'db-read-order-orders',
        listMode: true,
      },
    });
  } catch (err) {
    console.error('GET /api/bestellungen Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * GET /api/bestellungen/lieferanten
 * Zweck:
 * - Liefert aktive Lieferanten für das Bestellmodul
 * - Sortiert nach Name
 * - Noch bewusst ohne Artikel/EK/Bestelllogik
 */
router.get('/lieferanten', verifyToken(), async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        id,
        name,
        code,
        formular_typ,
        aktiv
      FROM "order".order_suppliers
      WHERE aktiv = true
      ORDER BY name ASC
    `);

    return res.json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Lieferanten erfolgreich geladen',
      stage: 'phase-2-lieferanten-read',
      count: result.rows.length,
      items: result.rows,
      meta: {
        source: 'db-read-order-suppliers',
      },
    });
  } catch (err) {
    console.error('GET /api/bestellungen/lieferanten Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * GET /api/bestellungen/filialprofil?supplier=<code>
 * Zweck:
 * - liefert aktive Filialprofile eines aktiven Lieferanten
 * - Frontend gibt supplier-code mit
 * - Backend mappt code -> supplier_id -> Filialprofile
 */
router.get('/filialprofil', verifyToken(), async (req, res) => {
  try {
    const { supplier } = req.query || {};

    if (!supplier) {
      return res.status(400).json({ message: 'supplier fehlt' });
    }

    const supplierResult = await db.query(
      `
      SELECT
        id,
        name,
        code,
        formular_typ
      FROM "order".order_suppliers
      WHERE code = $1
        AND aktiv = true
      LIMIT 1
      `,
      [supplier]
    );

    if (supplierResult.rows.length === 0) {
      return res.status(404).json({ message: 'Lieferant nicht gefunden' });
    }

    const supplierData = supplierResult.rows[0];

    const result = await db.query(
      `
      SELECT
        filiale,
        firma,
        strasse,
        ort,
        kunden_nr,
        auftrags_nr,
        gespraechspartner
      FROM "order".order_supplier_branch_profiles
      WHERE supplier_id = $1
        AND aktiv = true
      ORDER BY filiale ASC
      `,
      [supplierData.id]
    );

    return res.json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Filialprofile erfolgreich geladen',
      stage: 'phase-3-filialprofil-read',
      supplier: supplierData,
      count: result.rows.length,
      items: result.rows,
      meta: {
        source: 'db-read-order-supplier-branch-profiles',
      },
    });
  } catch (err) {
    console.error('GET /api/bestellungen/filialprofil Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * GET /api/bestellungen/artikel?supplier=<code>
 * Zweck:
 * - Liefert aktive Artikel eines aktiven Lieferanten
 * - Frontend gibt supplier-code mit
 * - Backend mappt code -> supplier_id -> Artikel
 */
router.get('/artikel', verifyToken(), async (req, res) => {
  try {
    const { supplier } = req.query || {};

    if (!supplier) {
      return res.status(400).json({ message: 'supplier fehlt' });
    }

    const supplierResult = await db.query(
      `
      SELECT
        id,
        name,
        code,
        formular_typ
      FROM "order".order_suppliers
      WHERE code = $1
        AND aktiv = true
      LIMIT 1
      `,
      [supplier]
    );

    if (supplierResult.rows.length === 0) {
      return res.status(404).json({ message: 'Lieferant nicht gefunden' });
    }

    const supplierData = supplierResult.rows[0];

    const articlesResult = await db.query(
      `
      SELECT
        id,
        supplier_article_no,
        ean,
        name,
        ve_stueck,
        sort_index
      FROM "order".order_supplier_articles
      WHERE supplier_id = $1
        AND aktiv = true
      ORDER BY sort_index ASC
      `,
      [supplierData.id]
    );

    return res.json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Artikel erfolgreich geladen',
      stage: 'phase-2-artikel-read',
      supplier: supplierData,
      count: articlesResult.rows.length,
      items: articlesResult.rows,
      meta: {
        source: 'db-read-order-supplier-articles',
      },
    });
  } catch (err) {
    console.error('GET /api/bestellungen/artikel Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * GET /api/bestellungen/artikel-mit-ek?supplier=<code>&datum=YYYY-MM-DD
 * Zweck:
 * - Liefert aktive Artikel eines aktiven Lieferanten
 * - ergänzt um den zum Stichtag gültigen EK-Satz
 */
router.get('/artikel-mit-ek', verifyToken(), async (req, res) => {
  try {
    const { supplier, datum } = req.query || {};

    if (!supplier) {
      return res.status(400).json({ message: 'supplier fehlt' });
    }

    const stichtag = datum || new Date().toISOString().split('T')[0];

    const supplierResult = await db.query(
      `
      SELECT
        id,
        name,
        code,
        formular_typ
      FROM "order".order_suppliers
      WHERE code = $1
        AND aktiv = true
      LIMIT 1
      `,
      [supplier]
    );

    if (supplierResult.rows.length === 0) {
      return res.status(404).json({ message: 'Lieferant nicht gefunden' });
    }

    const supplierData = supplierResult.rows[0];

    const result = await db.query(
      `
      SELECT
        a.id,
        a.supplier_article_no,
        a.ean,
        a.name,
        a.ve_stueck,
        a.sort_index,
        p.ek_einzel,
        p.ek_pro_karton,
        p.gueltig_ab,
        p.gueltig_bis
      FROM "order".order_supplier_articles a
      LEFT JOIN LATERAL (
        SELECT
          p.ek_einzel,
          p.ek_pro_karton,
          p.gueltig_ab,
          p.gueltig_bis
        FROM "order".order_supplier_article_prices p
        WHERE p.article_id = a.id
          AND p.gueltig_ab <= $2::date
          AND (p.gueltig_bis IS NULL OR p.gueltig_bis >= $2::date)
        ORDER BY p.gueltig_ab DESC
        LIMIT 1
      ) p ON true
      WHERE a.supplier_id = $1
        AND a.aktiv = true
      ORDER BY a.sort_index ASC
      `,
      [supplierData.id, stichtag]
    );

    return res.json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Artikel inkl. EK erfolgreich geladen',
      stage: 'phase-2-artikel-ek-read',
      supplier: supplierData,
      datum: stichtag,
      count: result.rows.length,
      items: result.rows,
      meta: {
        source: 'db-read-articles-with-ek',
      },
    });
  } catch (err) {
    console.error('GET /api/bestellungen/artikel-mit-ek Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * POST /api/bestellungen
 * Zweck:
 * - Speichert eine Bestellung inkl. Positions-Snapshots
 * - Lädt Lieferant, Artikel und gültigen EK aus der DB
 * - berechnet Summen DB-seitig innerhalb einer Transaktion
 * - erzeugt zusätzlich die Budgetbuchung (normal oder split)
 *
 * WICHTIGER FACHSTAND:
 * - Order und Budget sind zwei getrennte Datensätze
 * - Order bleibt Nachweis / read-only
 * - Budget bleibt eigenständig bearbeitbar
 * - deshalb wird KEINE harte Rückverknüpfung order.order_orders.budget_booking_id gesetzt
 *
 * Erwarteter Payload:
 * {
 *   "order": {
 *     "supplier": "mellerud",
 *     "filiale": "Vreden",
 *     "bestelldatum": "2026-04-03",
 *     "status": "saved",
 *     "positionen": [
 *       { "articleId": "uuid", "menge_kartons": 2 }
 *     ]
 *   },
 *   "budget": {
 *     "typ": "bestellung",
 *     "splits": [
 *       { "filiale": "Ahaus", "betrag": 30.00 }
 *     ]
 *   }
 * }
 */
router.post('/', verifyToken(), async (req, res) => {
  const client = await db.connect();

  try {
    const { order, budget } = req.body || {};

    if (!order || typeof order !== 'object') {
      return res.status(400).json({ message: 'order fehlt oder ist ungültig' });
    }

    if (!budget || typeof budget !== 'object') {
      return res.status(400).json({ message: 'budget fehlt oder ist ungültig' });
    }

    const {
      supplier,
      filiale: bodyFiliale,
      bestelldatum,
      status,
      positionen,
    } = order;

    const userName = req.user?.name ?? null;
    const userFiliale = req.user?.filiale ?? null;

    if (!supplier || typeof supplier !== 'string') {
      return res.status(400).json({ message: 'order.supplier fehlt oder ist ungültig' });
    }

    if (!isValidIsoDate(bestelldatum)) {
      return res.status(400).json({ message: 'order.bestelldatum fehlt oder ist ungültig (YYYY-MM-DD)' });
    }

    if (!Array.isArray(positionen) || positionen.length === 0) {
      return res.status(400).json({ message: 'order.positionen fehlen oder sind leer' });
    }

    const allowedInitialStatuses = new Set(['draft', 'saved']);
    const orderStatus = status || 'saved';

    if (!allowedInitialStatuses.has(orderStatus)) {
      return res.status(400).json({ message: 'order.status ist ungültig (erlaubt: draft, saved)' });
    }

    const effectiveFiliale =
      userFiliale && userFiliale !== 'Alle'
        ? userFiliale
        : bodyFiliale;

    if (!effectiveFiliale || typeof effectiveFiliale !== 'string') {
      return res.status(400).json({ message: 'order.filiale fehlt oder ist ungültig' });
    }

    if (!userName) {
      return res.status(400).json({ message: 'Benutzername im Token fehlt' });
    }

    if (budget.typ !== 'bestellung') {
      return res.status(400).json({ message: 'budget.typ ist ungültig (erlaubt: bestellung)' });
    }

    const normalizedPositions = positionen.map((item, index) => ({
      index,
      articleId: item?.articleId,
      menge_kartons: item?.menge_kartons,
    }));

    for (const item of normalizedPositions) {
      if (!item.articleId || typeof item.articleId !== 'string') {
        return res.status(400).json({
          message: `order.positionen[${item.index}].articleId fehlt oder ist ungültig`,
        });
      }

      if (!Number.isInteger(item.menge_kartons) || item.menge_kartons <= 0) {
        return res.status(400).json({
          message: `order.positionen[${item.index}].menge_kartons muss eine positive Ganzzahl sein`,
        });
      }
    }

    const rawSplits = Array.isArray(budget.splits) ? budget.splits : [];
    const normalizedSplits = rawSplits.map((item, index) => ({
      index,
      filiale: normalizeFiliale(item?.filiale),
      betrag: parseNumericSafe(item?.betrag),
    }));

    const seenSplitFilialen = new Set();

    for (const item of normalizedSplits) {
      if (!item.filiale) {
        return res.status(400).json({
          message: `budget.splits[${item.index}].filiale fehlt oder ist ungültig`,
        });
      }

      if (item.filiale.toLowerCase() === 'alle') {
        return res.status(400).json({
          message: `budget.splits[${item.index}].filiale darf nicht "Alle" sein`,
        });
      }

      if (item.filiale === effectiveFiliale) {
        return res.status(400).json({
          message: `budget.splits[${item.index}].filiale darf nicht identisch zur Parent-Filiale sein`,
        });
      }

      if (item.betrag === null || item.betrag <= 0) {
        return res.status(400).json({
          message: `budget.splits[${item.index}].betrag muss > 0 sein`,
        });
      }

      if (seenSplitFilialen.has(item.filiale)) {
        return res.status(400).json({
          message: `budget.splits enthält doppelte Filiale: ${item.filiale}`,
        });
      }

      seenSplitFilialen.add(item.filiale);
    }

    await client.query('BEGIN');

    const supplierResult = await client.query(
      `
      SELECT
        id,
        name,
        code,
        formular_typ
      FROM "order".order_suppliers
      WHERE code = $1
        AND aktiv = true
      LIMIT 1
      `,
      [supplier]
    );

    if (supplierResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Lieferant nicht gefunden oder inaktiv' });
    }

    const supplierData = supplierResult.rows[0];

    const profileResult = await client.query(
      `
      SELECT
        filiale,
        firma,
        strasse,
        ort,
        kunden_nr,
        auftrags_nr,
        gespraechspartner
      FROM "order".order_supplier_branch_profiles
      WHERE supplier_id = $1
        AND filiale = $2
        AND aktiv = true
      LIMIT 1
      `,
      [supplierData.id, effectiveFiliale]
    );

    if (profileResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({
        message: `Kein aktives Filialprofil für Lieferant ${supplierData.code} und Filiale ${effectiveFiliale} gefunden`,
      });
    }

    const profileData = profileResult.rows[0];

    const orderInsertResult = await client.query(
      `
      INSERT INTO "order".order_orders (
        supplier_id,
        filiale,
        ordered_by_name,
        bestelldatum,
        status,
        gesamtsumme_netto,
        budget_booking_id,
        supplier_formular_typ_snapshot,
        firma_snapshot,
        kunden_nr_snapshot,
        strasse_snapshot,
        ort_snapshot,
        auftrags_nr_snapshot,
        gespraechspartner_snapshot
      )
      VALUES (
        $1, $2, $3, $4::date, $5, 0, NULL, $6, $7, $8, $9, $10, $11, $12
      )
      RETURNING
        id,
        supplier_id,
        filiale,
        ordered_by_name,
        bestelldatum,
        status,
        gesamtsumme_netto,
        budget_booking_id,
        supplier_formular_typ_snapshot,
        created_at,
        updated_at
      `,
      [
        supplierData.id,
        effectiveFiliale,
        userName,
        bestelldatum,
        orderStatus,
        supplierData.formular_typ,
        profileData.firma,
        profileData.kunden_nr,
        profileData.strasse,
        profileData.ort,
        profileData.auftrags_nr,
        profileData.gespraechspartner,
      ]
    );

    const orderRow = orderInsertResult.rows[0];
    const createdPositions = [];

    for (const item of normalizedPositions) {
      const articleResult = await client.query(
        `
        SELECT
          a.id,
          a.supplier_article_no,
          a.ean,
          a.name,
          a.ve_stueck,
          a.sort_index,
          p.ek_pro_karton,
          p.gueltig_ab,
          p.gueltig_bis
        FROM "order".order_supplier_articles a
        LEFT JOIN LATERAL (
          SELECT
            p.ek_pro_karton,
            p.gueltig_ab,
            p.gueltig_bis
          FROM "order".order_supplier_article_prices p
          WHERE p.article_id = a.id
            AND p.gueltig_ab <= $2::date
            AND (p.gueltig_bis IS NULL OR p.gueltig_bis >= $2::date)
          ORDER BY p.gueltig_ab DESC
          LIMIT 1
        ) p ON true
        WHERE a.id = $1
          AND a.supplier_id = $3
          AND a.aktiv = true
        LIMIT 1
        `,
        [item.articleId, bestelldatum, supplierData.id]
      );

      if (articleResult.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          message: `Artikel ${item.articleId} nicht gefunden, inaktiv oder gehört nicht zum Lieferanten`,
        });
      }

      const article = articleResult.rows[0];

      if (article.ek_pro_karton === null || article.ek_pro_karton === undefined) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          message: `Für Artikel ${item.articleId} existiert kein gültiger EK zum Bestelldatum`,
        });
      }

      const positionInsertResult = await client.query(
        `
        INSERT INTO "order".order_order_positions (
          order_id,
          article_id,
          supplier_article_no_snapshot,
          ean_snapshot,
          name_snapshot,
          ve_stueck_snapshot,
          ek_pro_karton_snapshot,
          menge_kartons,
          positionssumme_netto,
          sort_index_snapshot
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7,
          $8,
          ($7::numeric * $8::integer),
          $9
        )
        RETURNING
          id,
          order_id,
          article_id,
          supplier_article_no_snapshot,
          ean_snapshot,
          name_snapshot,
          ve_stueck_snapshot,
          ek_pro_karton_snapshot,
          menge_kartons,
          positionssumme_netto,
          sort_index_snapshot,
          created_at
        `,
        [
          orderRow.id,
          article.id,
          article.supplier_article_no,
          article.ean,
          article.name,
          article.ve_stueck,
          article.ek_pro_karton,
          item.menge_kartons,
          article.sort_index,
        ]
      );

      createdPositions.push(positionInsertResult.rows[0]);
    }

    const sumUpdateResult = await client.query(
      `
      UPDATE "order".order_orders o
      SET
        gesamtsumme_netto = COALESCE(s.summe, 0),
        updated_at = now()
      FROM (
        SELECT
          order_id,
          SUM(positionssumme_netto) AS summe
        FROM "order".order_order_positions
        WHERE order_id = $1
        GROUP BY order_id
      ) s
      WHERE o.id = $1
      RETURNING
        o.id,
        o.supplier_id,
        o.filiale,
        o.ordered_by_name,
        o.bestelldatum,
        o.status,
        o.gesamtsumme_netto,
        o.budget_booking_id,
        o.supplier_formular_typ_snapshot,
        o.created_at,
        o.updated_at
      `,
      [orderRow.id]
    );

    const finalOrder = sumUpdateResult.rows[0] || orderRow;

    const { jahr, kw } = await resolveBudgetYearWeek(client, bestelldatum);

    const parentWeekBudgetId = await ensureWeekBudget(client, effectiveFiliale, jahr, kw);

    let budgetBookingId = null;

    if (normalizedSplits.length > 0) {
      const childSum = normalizedSplits.reduce((sum, item) => sum + item.betrag, 0);

      if (childSum > Number(finalOrder.gesamtsumme_netto)) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          message: 'Summe der Split-Beträge ist größer als die Bestellsumme',
        });
      }

      const resolvedSplits = [];
      for (const item of normalizedSplits) {
        const targetWeekBudgetId = await ensureWeekBudget(client, item.filiale, jahr, kw);
        resolvedSplits.push({
          target_week_budget_id: targetWeekBudgetId,
          betrag: item.betrag,
        });
      }

      const splitPayload = {
        source_week_budget_id: parentWeekBudgetId,
        gesamtbetrag: Number(finalOrder.gesamtsumme_netto),
        typ: 'bestellung',
        source: 'BESTELLUNG',
        status: 'offen',
        datum: bestelldatum,
        lieferant: supplierData.name,
        aktion_nr: null,
        beschreibung: null,
        splits: resolvedSplits,
      };

      const splitResult = await client.query(
        `SELECT budget.create_split_booking($1::jsonb, $2::text) AS result`,
        [JSON.stringify(splitPayload), userName]
      );

      budgetBookingId = splitResult.rows?.[0]?.result?.parent_id || null;

      if (!budgetBookingId) {
        throw new Error('Split-Booking konnte nicht erzeugt werden');
      }
    } else {
      const bookingInsertResult = await client.query(
        `
        INSERT INTO budget.bookings (
          week_budget_id,
          datum,
          typ,
          betrag,
          lieferant,
          status,
          created_by,
          created_at,
          source
        )
        VALUES (
          $1, $2::date, $3, $4, $5, $6, $7, NOW(), $8
        )
        RETURNING id
        `,
        [
          parentWeekBudgetId,
          bestelldatum,
          'bestellung',
          finalOrder.gesamtsumme_netto,
          supplierData.name,
          'offen',
          userName,
          'BESTELLUNG',
        ]
      );

      budgetBookingId = bookingInsertResult.rows[0].id;
    }

    await client.query('COMMIT');

    return res.status(201).json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Bestellung erfolgreich gespeichert',
      stage: 'phase-2-write-order-save',
      order: finalOrder,
      supplier: {
        id: supplierData.id,
        name: supplierData.name,
        code: supplierData.code,
        formular_typ: supplierData.formular_typ,
      },
      count: createdPositions.length,
      items: createdPositions,
      meta: {
        source: 'db-write-order-budget-transaction',
        budgetBookingCreated: true,
        budgetBookingId: budgetBookingId,
        budgetBookingKind: normalizedSplits.length > 0 ? 'split-parent' : 'booking',
        orderBudgetLinked: false,
      },
    });
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      console.error('POST /api/bestellungen Rollback-Fehler:', rollbackErr);
    }

    console.error('POST /api/bestellungen Fehler:', err);
    console.error('message:', err?.message);
    console.error('code:', err?.code);
    console.error('detail:', err?.detail);
    console.error('constraint:', err?.constraint);
    console.error('table:', err?.table);
    console.error('column:', err?.column);

    if (err?.code === '23505') {
      return res.status(409).json({
        message: 'Doppelte Position oder eindeutige DB-Regel verletzt',
      });
    }

    return res.status(500).json({ message: 'Serverfehler' });
  } finally {
    client.release();
  }
});

module.exports = router;