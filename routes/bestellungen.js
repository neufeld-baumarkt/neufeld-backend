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

/**
 * GET /api/bestellungen
 * Zweck:
 * - Erster echter Read-Endpunkt für das Bestellmodul
 * - Liefert eine stabile Grundstruktur für das Frontend
 * - Jetzt MIT einfachem DB-Read
 */
router.get('/', verifyToken(), async (req, res) => {
  try {
    const { id, name, role, filiale } = req.user || {};

    const result = await db.query(
      'SELECT COUNT(*) AS count FROM "order".order_suppliers'
    );

    const supplierCount = result.rows?.[0]?.count ?? null;

    return res.json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Bestellungen API erreichbar',
      stage: 'phase-2-read-db',
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
        source: 'db-read',
        nextStep: 'write-order-save',
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
 *
 * Erwarteter Payload:
 * {
 *   "supplier": "mellerud",
 *   "filiale": "Vreden",
 *   "bestelldatum": "2026-04-03",
 *   "status": "saved",
 *   "positionen": [
 *     { "articleId": "uuid", "menge_kartons": 2 }
 *   ]
 * }
 */
router.post('/', verifyToken(), async (req, res) => {
  const client = await db.connect();

  try {
    const {
      supplier,
      filiale: bodyFiliale,
      bestelldatum,
      status,
      positionen,
    } = req.body || {};

    const userName = req.user?.name ?? null;
    const userFiliale = req.user?.filiale ?? null;

    if (!supplier || typeof supplier !== 'string') {
      return res.status(400).json({ message: 'supplier fehlt oder ist ungültig' });
    }

    if (!isValidIsoDate(bestelldatum)) {
      return res.status(400).json({ message: 'bestelldatum fehlt oder ist ungültig (YYYY-MM-DD)' });
    }

    if (!Array.isArray(positionen) || positionen.length === 0) {
      return res.status(400).json({ message: 'positionen fehlen oder sind leer' });
    }

    const allowedInitialStatuses = new Set(['draft', 'saved']);
    const orderStatus = status || 'saved';

    if (!allowedInitialStatuses.has(orderStatus)) {
      return res.status(400).json({ message: 'status ist ungültig (erlaubt: draft, saved)' });
    }

    const effectiveFiliale =
      userFiliale && userFiliale !== 'Alle'
        ? userFiliale
        : bodyFiliale;

    if (!effectiveFiliale || typeof effectiveFiliale !== 'string') {
      return res.status(400).json({ message: 'filiale fehlt oder ist ungültig' });
    }

    if (!userName) {
      return res.status(400).json({ message: 'Benutzername im Token fehlt' });
    }

    const normalizedPositions = positionen.map((item, index) => ({
      index,
      articleId: item?.articleId,
      menge_kartons: item?.menge_kartons,
    }));

    for (const item of normalizedPositions) {
      if (!item.articleId || typeof item.articleId !== 'string') {
        return res.status(400).json({
          message: `positionen[${item.index}].articleId fehlt oder ist ungültig`,
        });
      }

      if (!Number.isInteger(item.menge_kartons) || item.menge_kartons <= 0) {
        return res.status(400).json({
          message: `positionen[${item.index}].menge_kartons muss eine positive Ganzzahl sein`,
        });
      }
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
        $1, $2, $3, $4::date, $5, 0, NULL, $6, NULL, NULL, NULL, NULL, NULL, NULL
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
        source: 'db-write-order-transaction',
        budgetBookingCreated: false,
      },
    });
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      console.error('POST /api/bestellungen Rollback-Fehler:', rollbackErr);
    }

    console.error('POST /api/bestellungen Fehler:', err);

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