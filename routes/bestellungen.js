const express = require('express');

const router = express.Router();

const verifyToken = require('../middleware/verifyToken');
const db = require('../db');

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

    // 🔥 ERSTER DB-READ (Schema-Test)
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
        canWrite: false,
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
          status: 'neu',
        },
        positionen: [],
      },
      meta: {
        dbConnected: true,
        supplierCount: supplierCount,
        source: 'db-read',
        nextStep: 'lieferanten-read',
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

module.exports = router;