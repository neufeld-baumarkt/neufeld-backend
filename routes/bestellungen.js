'use strict';

const express = require('express');
const router = express.Router();
const verifyToken = require('../middleware/verifyToken');
const db = require('../db');
const { sendOrderMail } = require('../services/mailer');
const { generateMellerudOrderPdf } = require('../services/pdf/mellerudPdfService');
const {
  calculateOrderPlan,
  centsToMoney,
  isValidIsoDate,
  normalizeMellerudArticleIdentity,
  normalizeMellerudArticleMaster,
  normalizeSupplierCode,
  supportsMellerudWorkflow,
} = require('../lib/orderRules');
const { resolveCanonicalOrderRecipients } = require('../lib/orderEmailPolicy');

const GLOBAL_ROLES = new Set(['Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1']);
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const normalizeText = (value) => typeof value === 'string' ? value.trim() : '';
const canReadAllOrders = (role) => GLOBAL_ROLES.has(role);
const canManageArticles = (role) => GLOBAL_ROLES.has(role);

const businessDateBerlin = () => new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit',
}).format(new Date());

const databaseDateIso = (value) => {
  if (value instanceof Date) {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit',
    }).format(value);
  }
  const match = String(value || '').match(/^\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : '';
};

function articleMasterDto(row) {
  return {
    id: row.id,
    supplier_article_no: row.supplier_article_no,
    kunden_art_nr: row.kunden_art_nr,
    ean: row.ean,
    name: row.name,
    ve_stueck: Number(row.ve_stueck),
    sort_index: Number(row.sort_index),
    aktiv: Boolean(row.aktiv),
    ek_einzel: row.ek_einzel === null || row.ek_einzel === undefined ? null : Number(row.ek_einzel),
    ek_pro_karton: row.ek_pro_karton === null || row.ek_pro_karton === undefined ? null : Number(row.ek_pro_karton),
    preis_gueltig_ab: row.gueltig_ab || null,
    preis_gueltig_bis: row.gueltig_bis || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

async function loadMellerudArticleMaster(client, articleId) {
  const result = await client.query(
    `SELECT a.id, a.supplier_article_no, a.kunden_art_nr, a.ean, a.name,
            a.ve_stueck, a.sort_index, a.aktiv, a.created_at, a.updated_at,
            p.ek_einzel, p.ek_pro_karton, p.gueltig_ab, p.gueltig_bis
     FROM "order".order_supplier_articles a
     JOIN "order".order_suppliers s ON s.id=a.supplier_id
     LEFT JOIN LATERAL (
       SELECT ek_einzel, ek_pro_karton, gueltig_ab, gueltig_bis
       FROM "order".order_supplier_article_prices
       WHERE article_id=a.id
       ORDER BY (gueltig_bis IS NULL) DESC, gueltig_ab DESC LIMIT 1
     ) p ON true
     WHERE a.id=$1 AND lower(s.code)='mellerud' LIMIT 1`,
    [articleId]
  );
  return result.rows[0] || null;
}

async function ensureArticleIdentityUnique(client, supplierId, article, excludeId = null) {
  const result = await client.query(
    `SELECT supplier_article_no, kunden_art_nr, ean
     FROM "order".order_supplier_articles
     WHERE supplier_id=$1 AND ($2::uuid IS NULL OR id<>$2::uuid)
       AND (lower(supplier_article_no)=lower($3)
         OR lower(kunden_art_nr)=lower($4)
         OR ean=$5)
     LIMIT 1`,
    [supplierId, excludeId, article.supplier_article_no, article.kunden_art_nr, article.ean]
  );
  if (result.rows.length) {
    const row = result.rows[0];
    if (String(row.supplier_article_no).toLowerCase() === article.supplier_article_no.toLowerCase()) {
      throw Object.assign(new Error('Diese Mellerud-Art.-Nr. ist bereits vergeben'), { statusCode: 409 });
    }
    if (String(row.kunden_art_nr || '').toLowerCase() === article.kunden_art_nr.toLowerCase()) {
      throw Object.assign(new Error('Diese Neufeld-Art.-Nr. ist bereits vergeben'), { statusCode: 409 });
    }
    throw Object.assign(new Error('Diese EAN ist bereits vergeben'), { statusCode: 409 });
  }
}

async function appendArticleAudit(client, articleId, action, beforeData, afterData, changedBy) {
  await client.query(
    `INSERT INTO "order".order_supplier_article_audit
       (article_id, action, before_data, after_data, changed_by)
     VALUES ($1,$2,$3::jsonb,$4::jsonb,$5)`,
    [articleId, action, beforeData ? JSON.stringify(beforeData) : null, JSON.stringify(afterData), changedBy]
  );
}

function parseOptionalInt(value) {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : NaN;
}

function requestedBranchForUser(user, requestedBranch) {
  return canReadAllOrders(user?.role) ? normalizeText(requestedBranch) : normalizeText(user?.filiale);
}

function ensureUuid(value, label) {
  if (!UUID_PATTERN.test(String(value || ''))) {
    const error = new Error(`${label} ist ungültig`);
    error.statusCode = 400;
    throw error;
  }
  return String(value);
}

async function resolveBudgetYearWeek(client, dateStr) {
  const result = await client.query(
    `SELECT EXTRACT(ISOYEAR FROM $1::date)::int AS jahr,
            EXTRACT(WEEK FROM $1::date)::int AS kw`,
    [dateStr]
  );
  return result.rows[0];
}

async function ensureWeekBudget(client, filiale, jahr, kw) {
  const result = await client.query(
    `INSERT INTO budget.week_budgets (
       filiale, jahr, kw, prozentsatz_snapshot, freigegeben, created_at, updated_at
     ) VALUES (
       $1, $2, $3,
       COALESCE((SELECT prozentsatz FROM budget.week_rules WHERE jahr=$2 AND kw=$3 LIMIT 1), 0),
       false, NOW(), NOW()
     )
     ON CONFLICT (filiale, jahr, kw) DO UPDATE SET
       prozentsatz_snapshot=CASE
         WHEN budget.week_budgets.prozentsatz_snapshot=0 THEN EXCLUDED.prozentsatz_snapshot
         ELSE budget.week_budgets.prozentsatz_snapshot
       END,
       updated_at=NOW()
     RETURNING id`,
    [filiale, jahr, kw]
  );
  return result.rows[0].id;
}

async function loadOrderHeader(client, orderId, user, { requireGlobal = false } = {}) {
  ensureUuid(orderId, 'Bestell-ID');
  if (requireGlobal && !canReadAllOrders(user?.role)) {
    const error = new Error('Diese Aktion ist nur für Superuser zulässig');
    error.statusCode = 403;
    throw error;
  }
  const params = [orderId];
  let scopeSql = '';
  if (!canReadAllOrders(user?.role)) {
    const filiale = normalizeText(user?.filiale);
    if (!filiale) {
      const error = new Error('Dem Benutzer ist keine Filiale zugeordnet');
      error.statusCode = 403;
      throw error;
    }
    params.push(filiale);
    scopeSql = `AND o.filiale=$${params.length}`;
  }
  const result = await client.query(
    `SELECT o.*, s.name AS supplier_name, s.code AS supplier_code,
            s.formular_typ AS supplier_formular_typ,
            s.order_email AS supplier_order_email, s.minimum_order_ve,
            p.email AS branch_email
     FROM "order".order_orders o
     JOIN "order".order_suppliers s ON s.id=o.supplier_id
     LEFT JOIN "order".order_supplier_branch_profiles p
       ON p.supplier_id=o.supplier_id AND p.filiale=o.filiale
     WHERE o.id=$1 ${scopeSql} LIMIT 1`,
    params
  );
  if (result.rows.length === 0) {
    const error = new Error('Bestellung nicht gefunden oder kein Zugriff');
    error.statusCode = 404;
    throw error;
  }
  return result.rows[0];
}

function orderDto(row) {
  return {
    id: row.id,
    filiale: row.filiale,
    ordered_by_name: row.ordered_by_name,
    bestelldatum: row.bestelldatum,
    jahr: row.jahr === undefined ? undefined : Number(row.jahr),
    kw: row.kw === undefined ? undefined : Number(row.kw),
    status: row.status,
    read_only: true,
    gesamtsumme_netto: row.gesamtsumme_netto,
    gesamt_ve: row.gesamt_ve,
    split_snapshot: row.split_snapshot || null,
    pdf_generated_at: row.pdf_generated_at,
    sent_at: row.sent_at,
    dispatch_status: row.dispatch_status,
    dispatch_mode: row.dispatch_mode,
    dispatch_attempted_at: row.dispatch_attempted_at,
    dispatch_error: row.dispatch_error,
    dispatch_to_snapshot: row.dispatch_to_snapshot || [],
    dispatch_cc_snapshot: row.dispatch_cc_snapshot || [],
    supplier: {
      id: row.supplier_id,
      name: row.supplier_name,
      code: row.supplier_code,
      formular_typ: row.supplier_formular_typ || row.supplier_formular_typ_snapshot,
    },
    position_count: row.position_count === undefined ? undefined : Number(row.position_count),
    profile_snapshot: {
      firma: row.firma_snapshot,
      kunden_nr: row.kunden_nr_snapshot,
      strasse: row.strasse_snapshot,
      ort: row.ort_snapshot,
      auftrags_nr: row.auftrags_nr_snapshot,
      gespraechspartner: row.gespraechspartner_snapshot,
      telefon: row.telefon_snapshot,
      email: row.email_snapshot,
    },
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

async function loadOrderDetail(client, orderId, user) {
  const order = await loadOrderHeader(client, orderId, user);
  const positions = await client.query(
    `SELECT id, article_id, supplier_article_no_snapshot, kunden_art_nr_snapshot,
            ean_snapshot, name_snapshot, ve_stueck_snapshot,
            ek_einzel_snapshot, ek_pro_karton_snapshot, menge_kartons,
            positionssumme_netto, sort_index_snapshot, created_at
     FROM "order".order_order_positions
     WHERE order_id=$1 ORDER BY sort_index_snapshot, name_snapshot`,
    [orderId]
  );
  return { ...orderDto(order), positionen: positions.rows };
}

async function resolveDispatchRecipients(client, order, mode, env) {
  const branchEmail = normalizeText(order.branch_email || order.email_snapshot);
  const supervisorName = normalizeText(env.ORDER_LIGHT_SUPERVISOR_NAME) || 'Mirko';
  let supervisorEmail = normalizeText(env.ORDER_LIGHT_SUPERVISOR_EMAIL);
  if (mode !== 'final' && !supervisorEmail) {
    const supervisorResult = await client.query(
      `SELECT email FROM public.users
       WHERE role='Supervisor'
         AND (lower(name)=lower($1) OR lower(name) LIKE lower($1)||' %')
       ORDER BY CASE WHEN lower(name)=lower($1) THEN 0 ELSE 1 END, id LIMIT 1`,
      [supervisorName]
    );
    supervisorEmail = normalizeText(supervisorResult.rows[0]?.email);
  }
  return resolveCanonicalOrderRecipients({
    mode,
    branch: order.filiale,
    storedBranchEmail: branchEmail,
    storedSupplierEmail: order.supplier_order_email,
    configuredSupervisorEmail: supervisorEmail,
  });
}

async function dispatchOrder(orderId, env = process.env) {
  const client = await db.connect();
  let order;
  const mode = normalizeText(env.ORDER_DISPATCH_MODE).toLowerCase() === 'final' ? 'final' : 'light';
  try {
    order = await loadOrderHeader(client, orderId, { role: 'Admin' }, { requireGlobal: true });
    const pdfBuffer = await generateMellerudOrderPdf(orderId);
    await client.query(
      `UPDATE "order".order_orders
       SET pdf_generated_at=NOW(), dispatch_mode=$2, updated_at=NOW() WHERE id=$1`,
      [orderId, mode]
    );
    const recipients = await resolveDispatchRecipients(client, order, mode, env);
    const delivery = await sendOrderMail({
      subject: `Neue Bestellung - ${order.supplier_name} - ${order.filiale}`,
      text: [
        'Neue verbindliche Bestellung', '',
        `Filiale: ${order.filiale}`,
        `Lieferant: ${order.supplier_name}`,
        `Bestelldatum: ${String(order.bestelldatum).slice(0, 10)}`,
        `Bestellsumme netto: ${Number(order.gesamtsumme_netto).toFixed(2)} EUR`,
        `Bestellt von: ${order.ordered_by_name}`,
        `Bestell-ID: ${order.id}`, '',
        'Interne Filial- und Budgetaufteilungen sind nicht Bestandteil dieser Lieferantenbestellung.',
      ].join('\n'),
      to: recipients.to,
      cc: recipients.cc,
      attachments: [{ filename: `Bestellung_${order.id}.pdf`, content: pdfBuffer }],
    });
    const sent = delivery?.status === 'sent';
    const dispatchStatus = sent ? 'sent' : delivery?.status === 'blocked' ? 'blocked' : 'failed';
    const errorText = sent ? null : normalizeText(delivery?.message) || 'Versand wurde nicht ausgeführt';
    await client.query(
      `UPDATE "order".order_orders SET
         status=CASE WHEN $2 THEN 'sent' ELSE status END,
         sent_at=CASE WHEN $2 THEN NOW() ELSE sent_at END,
         dispatch_status=$3, dispatch_attempted_at=NOW(), dispatch_error=$4,
         dispatch_to_snapshot=$5::text[], dispatch_cc_snapshot=$6::text[], updated_at=NOW()
       WHERE id=$1`,
      [orderId, sent, dispatchStatus, errorText, recipients.to, recipients.cc]
    );
    return { ...delivery, mode, recipients, pdfGenerated: true };
  } catch (error) {
    if (order) {
      try {
        await client.query(
          `UPDATE "order".order_orders SET dispatch_status='failed',
             dispatch_attempted_at=NOW(), dispatch_error=$2, dispatch_mode=$3, updated_at=NOW()
           WHERE id=$1`,
          [orderId, String(error.message || error).slice(0, 1000), mode]
        );
      } catch (updateError) {
        console.error('Bestellversand-Status konnte nicht gespeichert werden:', updateError);
      }
    }
    return { status: 'failed', mode, message: error.message, pdfGenerated: false };
  } finally {
    client.release();
  }
}

router.get('/form', verifyToken(), async (req, res) => {
  try {
    const result = await db.query('SELECT COUNT(*)::int AS count FROM "order".order_suppliers WHERE aktiv=true');
    return res.json({ status: 'ok', module: 'bestellungen', user: req.user,
      permissions: { authenticated: true, canRead: true, canWrite: true },
      meta: { supplierCount: result.rows[0].count } });
  } catch (error) {
    console.error('GET /api/bestellungen/form:', error);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

router.get('/lieferanten', verifyToken(), async (_req, res) => {
  try {
    const result = await db.query(
      `SELECT id, name, code, formular_typ, minimum_order_ve, aktiv,
              lower(code)='mellerud' AS implemented
       FROM "order".order_suppliers
       WHERE aktiv=true AND lower(code)<>'chamberlain' ORDER BY name`
    );
    return res.json({ status: 'ok', count: result.rows.length, items: result.rows });
  } catch (error) {
    console.error('GET /api/bestellungen/lieferanten:', error);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

router.get('/filialprofil', verifyToken(), async (req, res) => {
  try {
    const supplier = normalizeSupplierCode(req.query?.supplier);
    if (!supplier) return res.status(400).json({ message: 'supplier fehlt' });
    const params = [supplier];
    let scopeSql = '';
    if (!canReadAllOrders(req.user?.role)) {
      const filiale = normalizeText(req.user?.filiale);
      if (!filiale) return res.status(403).json({ message: 'Keine Filiale zugeordnet' });
      params.push(filiale);
      scopeSql = `AND p.filiale=$${params.length}`;
    }
    const result = await db.query(
      `SELECT p.filiale, p.firma, p.strasse, p.ort, p.kunden_nr,
              p.auftrags_nr, p.gespraechspartner, p.telefon, p.email
       FROM "order".order_supplier_branch_profiles p
       JOIN "order".order_suppliers s ON s.id=p.supplier_id
       WHERE lower(s.code)=lower($1) AND s.aktiv=true AND p.aktiv=true ${scopeSql}
       ORDER BY p.filiale`,
      params
    );
    return res.json({ status: 'ok', count: result.rows.length, items: result.rows });
  } catch (error) {
    console.error('GET /api/bestellungen/filialprofil:', error);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

async function handleArticles(req, res, includePrices) {
  try {
    const supplier = normalizeSupplierCode(req.query?.supplier);
    if (!supplier) return res.status(400).json({ message: 'supplier fehlt' });
    const stichtag = req.query?.datum || new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit',
    }).format(new Date());
    if (includePrices && !isValidIsoDate(stichtag)) return res.status(400).json({ message: 'datum ist ungültig (YYYY-MM-DD)' });
    const priceFields = includePrices ? ', p.ek_einzel, p.ek_pro_karton, p.gueltig_ab, p.gueltig_bis' : '';
    const priceJoin = includePrices ? `LEFT JOIN LATERAL (
      SELECT ek_einzel, ek_pro_karton, gueltig_ab, gueltig_bis
      FROM "order".order_supplier_article_prices
      WHERE article_id=a.id AND gueltig_ab<=$2::date
        AND (gueltig_bis IS NULL OR gueltig_bis>=$2::date)
      ORDER BY gueltig_ab DESC LIMIT 1
    ) p ON true` : '';
    const result = await db.query(
      `SELECT a.id, a.supplier_article_no, a.kunden_art_nr, a.ean,
              a.name, a.ve_stueck, a.sort_index ${priceFields}
       FROM "order".order_supplier_articles a
       JOIN "order".order_suppliers s ON s.id=a.supplier_id
       ${priceJoin}
       WHERE lower(s.code)=lower($1) AND s.aktiv=true AND a.aktiv=true
       ORDER BY a.sort_index, a.name`,
      includePrices ? [supplier, stichtag] : [supplier]
    );
    return res.json({ status: 'ok', datum: includePrices ? stichtag : undefined, count: result.rows.length, items: result.rows });
  } catch (error) {
    console.error('GET Bestellartikel:', error);
    return res.status(500).json({ message: 'Serverfehler' });
  }
}

router.get('/artikel', verifyToken(), (req, res) => handleArticles(req, res, false));
router.get('/artikel-mit-ek', verifyToken(), (req, res) => handleArticles(req, res, true));

router.get('/artikelverwaltung', verifyToken(), async (req, res) => {
  try {
    if (!canManageArticles(req.user?.role)) {
      return res.status(403).json({ message: 'Artikelstammdaten dürfen nur von Superusern verwaltet werden' });
    }
    const result = await db.query(
      `SELECT a.id, a.supplier_article_no, a.kunden_art_nr, a.ean, a.name,
              a.ve_stueck, a.sort_index, a.aktiv, a.created_at, a.updated_at,
              p.ek_einzel, p.ek_pro_karton, p.gueltig_ab, p.gueltig_bis
       FROM "order".order_supplier_articles a
       JOIN "order".order_suppliers s ON s.id=a.supplier_id
       LEFT JOIN LATERAL (
         SELECT ek_einzel, ek_pro_karton, gueltig_ab, gueltig_bis
         FROM "order".order_supplier_article_prices
         WHERE article_id=a.id
         ORDER BY (gueltig_bis IS NULL) DESC, gueltig_ab DESC LIMIT 1
       ) p ON true
       WHERE lower(s.code)='mellerud'
       ORDER BY a.aktiv DESC, a.sort_index, a.name`
    );
    return res.json({ status: 'ok', count: result.rows.length, items: result.rows.map(articleMasterDto) });
  } catch (error) {
    console.error('GET /api/bestellungen/artikelverwaltung:', error);
    return res.status(500).json({ message: 'Artikelstamm konnte nicht geladen werden' });
  }
});

router.post('/artikelverwaltung', verifyToken(), async (req, res) => {
  let client;
  try {
    if (!canManageArticles(req.user?.role)) {
      return res.status(403).json({ message: 'Artikelstammdaten dürfen nur von Superusern verwaltet werden' });
    }
    const article = normalizeMellerudArticleMaster(req.body?.article);
    client = await db.connect();
    await client.query('BEGIN');
    const supplierResult = await client.query(
      `SELECT id FROM "order".order_suppliers WHERE lower(code)='mellerud' LIMIT 1 FOR UPDATE`
    );
    if (!supplierResult.rows.length) throw Object.assign(new Error('Mellerud-Lieferant wurde nicht gefunden'), { statusCode: 404 });
    const supplierId = supplierResult.rows[0].id;
    await ensureArticleIdentityUnique(client, supplierId, article);
    let sortIndex = article.sort_index;
    if (sortIndex === null) {
      const sortResult = await client.query(
        `SELECT COALESCE(MAX(sort_index),0)+10 AS next_sort
         FROM "order".order_supplier_articles WHERE supplier_id=$1`,
        [supplierId]
      );
      sortIndex = Number(sortResult.rows[0].next_sort);
    }
    const inserted = await client.query(
      `INSERT INTO "order".order_supplier_articles
         (supplier_id, supplier_article_no, kunden_art_nr, ean, name, ve_stueck, sort_index, aktiv)
       VALUES ($1,$2,$3,$4,$5,$6,$7,true) RETURNING id`,
      [supplierId, article.supplier_article_no, article.kunden_art_nr, article.ean,
        article.name, article.ve_stueck, sortIndex]
    );
    const articleId = inserted.rows[0].id;
    await client.query(
      `INSERT INTO "order".order_supplier_article_prices
         (article_id, ek_einzel, ek_pro_karton, gueltig_ab, gueltig_bis, created_by)
       VALUES ($1,$2,$3,$4::date,NULL,$5)`,
      [articleId, article.ek_einzel, article.ek_pro_karton, businessDateBerlin(), req.user.name]
    );
    const created = await loadMellerudArticleMaster(client, articleId);
    await appendArticleAudit(client, articleId, 'created', null, articleMasterDto(created), req.user.name);
    await client.query('COMMIT');
    client.release();
    client = null;
    return res.status(201).json({ status: 'ok', item: articleMasterDto(created) });
  } catch (error) {
    if (client) { try { await client.query('ROLLBACK'); } catch {} }
    const status = error.statusCode || (error.code === '23505' ? 409 : 400);
    return res.status(status).json({ message: error.code === '23505' ? 'Artikelnummer oder Sortierung ist bereits vergeben' : error.message });
  } finally {
    if (client) client.release();
  }
});

router.patch('/artikelverwaltung/:id', verifyToken(), async (req, res) => {
  let client;
  try {
    if (!canManageArticles(req.user?.role)) {
      return res.status(403).json({ message: 'Artikelstammdaten dürfen nur von Superusern verwaltet werden' });
    }
    const articleId = ensureUuid(req.params.id, 'Artikel-ID');
    const article = normalizeMellerudArticleMaster(req.body?.article);
    client = await db.connect();
    await client.query('BEGIN');
    const currentResult = await client.query(
      `SELECT a.*, s.code AS supplier_code
       FROM "order".order_supplier_articles a
       JOIN "order".order_suppliers s ON s.id=a.supplier_id
       WHERE a.id=$1 FOR UPDATE`,
      [articleId]
    );
    const current = currentResult.rows[0];
    if (!current || normalizeSupplierCode(current.supplier_code) !== 'mellerud') {
      throw Object.assign(new Error('Mellerud-Artikel nicht gefunden'), { statusCode: 404 });
    }
    const before = await loadMellerudArticleMaster(client, articleId);
    await ensureArticleIdentityUnique(client, current.supplier_id, article, articleId);
    const sortIndex = article.sort_index === null ? Number(current.sort_index) : article.sort_index;
    await client.query(
      `UPDATE "order".order_supplier_articles
       SET supplier_article_no=$1, kunden_art_nr=$2, ean=$3, name=$4,
           ve_stueck=$5, sort_index=$6, updated_at=NOW()
       WHERE id=$7`,
      [article.supplier_article_no, article.kunden_art_nr, article.ean, article.name,
        article.ve_stueck, sortIndex, articleId]
    );
    const priceResult = await client.query(
      `SELECT * FROM "order".order_supplier_article_prices
       WHERE article_id=$1 AND gueltig_bis IS NULL
       ORDER BY gueltig_ab DESC LIMIT 1 FOR UPDATE`,
      [articleId]
    );
    const currentPrice = priceResult.rows[0];
    const priceChanged = !currentPrice
      || Number(currentPrice.ek_einzel) !== article.ek_einzel
      || Number(currentPrice.ek_pro_karton) !== article.ek_pro_karton;
    if (priceChanged) {
      const today = businessDateBerlin();
      if (currentPrice && databaseDateIso(currentPrice.gueltig_ab) >= today) {
        await client.query(
          `UPDATE "order".order_supplier_article_prices
           SET ek_einzel=$1, ek_pro_karton=$2 WHERE id=$3`,
          [article.ek_einzel, article.ek_pro_karton, currentPrice.id]
        );
      } else {
        if (currentPrice) {
          await client.query(
            `UPDATE "order".order_supplier_article_prices
             SET gueltig_bis=($2::date - INTERVAL '1 day')::date WHERE id=$1`,
            [currentPrice.id, today]
          );
        }
        await client.query(
          `INSERT INTO "order".order_supplier_article_prices
             (article_id, ek_einzel, ek_pro_karton, gueltig_ab, gueltig_bis, created_by)
           VALUES ($1,$2,$3,$4::date,NULL,$5)`,
          [articleId, article.ek_einzel, article.ek_pro_karton, today, req.user.name]
        );
      }
    }
    const updated = await loadMellerudArticleMaster(client, articleId);
    await appendArticleAudit(client, articleId, 'updated', articleMasterDto(before), articleMasterDto(updated), req.user.name);
    await client.query('COMMIT');
    client.release();
    client = null;
    return res.json({ status: 'ok', item: articleMasterDto(updated) });
  } catch (error) {
    if (client) { try { await client.query('ROLLBACK'); } catch {} }
    const status = error.statusCode || (error.code === '23505' ? 409 : 400);
    return res.status(status).json({ message: error.code === '23505' ? 'Artikelnummer oder Sortierung ist bereits vergeben' : error.message });
  } finally {
    if (client) client.release();
  }
});

router.patch('/artikelverwaltung/:id/status', verifyToken(), async (req, res) => {
  let client;
  try {
    if (!canManageArticles(req.user?.role)) {
      return res.status(403).json({ message: 'Artikelstammdaten dürfen nur von Superusern verwaltet werden' });
    }
    if (typeof req.body?.aktiv !== 'boolean') return res.status(400).json({ message: 'aktiv muss true oder false sein' });
    const articleId = ensureUuid(req.params.id, 'Artikel-ID');
    client = await db.connect();
    await client.query('BEGIN');
    const before = await loadMellerudArticleMaster(client, articleId);
    if (!before) throw Object.assign(new Error('Mellerud-Artikel nicht gefunden'), { statusCode: 404 });
    await client.query(
      `UPDATE "order".order_supplier_articles SET aktiv=$1, updated_at=NOW() WHERE id=$2`,
      [req.body.aktiv, articleId]
    );
    const updated = await loadMellerudArticleMaster(client, articleId);
    await appendArticleAudit(client, articleId, req.body.aktiv ? 'activated' : 'deactivated',
      articleMasterDto(before), articleMasterDto(updated), req.user.name);
    await client.query('COMMIT');
    client.release();
    client = null;
    return res.json({ status: 'ok', item: articleMasterDto(updated) });
  } catch (error) {
    if (client) { try { await client.query('ROLLBACK'); } catch {} }
    return res.status(error.statusCode || 400).json({ message: error.message });
  } finally {
    if (client) client.release();
  }
});

router.patch('/artikel/:id', verifyToken(), async (req, res) => {
  let client;
  try {
    if (!canManageArticles(req.user?.role)) {
      return res.status(403).json({ message: 'Artikelstammdaten dürfen nur von Superusern bearbeitet werden' });
    }

    const articleId = ensureUuid(req.params.id, 'Artikel-ID');
    const identity = normalizeMellerudArticleIdentity(req.body?.article);
    client = await db.connect();
    await client.query('BEGIN');
    const before = await loadMellerudArticleMaster(client, articleId);
    if (!before) throw Object.assign(new Error('Mellerud-Artikel nicht gefunden'), { statusCode: 404 });
    await ensureArticleIdentityUnique(client, (await client.query(
      `SELECT supplier_id FROM "order".order_supplier_articles WHERE id=$1`, [articleId]
    )).rows[0].supplier_id, { ...identity, ean: before.ean }, articleId);
    const result = await client.query(
      `UPDATE "order".order_supplier_articles a
       SET supplier_article_no=$1, kunden_art_nr=$2, updated_at=NOW()
       FROM "order".order_suppliers s
       WHERE a.id=$3 AND a.supplier_id=s.id AND lower(s.code)='mellerud'
       RETURNING a.id, a.supplier_article_no, a.kunden_art_nr, a.ean,
                 a.name, a.ve_stueck, a.sort_index, a.aktiv`,
      [identity.supplier_article_no, identity.kunden_art_nr, articleId]
    );
    if (result.rows.length === 0) {
      throw Object.assign(new Error('Mellerud-Artikel nicht gefunden'), { statusCode: 404 });
    }
    const updated = await loadMellerudArticleMaster(client, articleId);
    await appendArticleAudit(client, articleId, 'updated', articleMasterDto(before), articleMasterDto(updated), req.user.name);
    await client.query('COMMIT');
    client.release();
    client = null;
    return res.json({ status: 'ok', item: result.rows[0] });
  } catch (error) {
    if (client) { try { await client.query('ROLLBACK'); } catch {} }
    if (error?.code === '23505') {
      return res.status(409).json({ message: 'Diese Mellerud-Artikelnummer ist bereits vergeben' });
    }
    const statusCode = error.statusCode || 400;
    return res.status(statusCode).json({ message: error.message || 'Artikelnummern konnten nicht gespeichert werden' });
  } finally {
    if (client) client.release();
  }
});

router.get('/', verifyToken(), async (req, res) => {
  try {
    const jahr = parseOptionalInt(req.query?.jahr);
    const kw = parseOptionalInt(req.query?.kw);
    if (Number.isNaN(jahr) || (jahr !== null && (jahr < 2000 || jahr > 2100))) return res.status(400).json({ message: 'jahr ist ungültig' });
    if (Number.isNaN(kw) || (kw !== null && (kw < 1 || kw > 53))) return res.status(400).json({ message: 'kw ist ungültig' });
    const requestedFiliale = normalizeText(req.query?.filiale);
    if (!canReadAllOrders(req.user?.role) && requestedFiliale && requestedFiliale !== req.user?.filiale) {
      return res.status(403).json({ message: 'Kein Zugriff auf fremde Filial-Bestellungen' });
    }
    const effectiveFiliale = requestedBranchForUser(req.user, requestedFiliale);
    const params = [];
    const where = [];
    if (jahr !== null) { params.push(jahr); where.push(`EXTRACT(ISOYEAR FROM o.bestelldatum)::int=$${params.length}`); }
    if (kw !== null) { params.push(kw); where.push(`EXTRACT(WEEK FROM o.bestelldatum)::int=$${params.length}`); }
    if (effectiveFiliale && effectiveFiliale.toLowerCase() !== 'alle') { params.push(effectiveFiliale); where.push(`o.filiale=$${params.length}`); }
    const result = await db.query(
      `SELECT o.*, EXTRACT(ISOYEAR FROM o.bestelldatum)::int AS jahr,
              EXTRACT(WEEK FROM o.bestelldatum)::int AS kw,
              s.name AS supplier_name, s.code AS supplier_code,
              s.formular_typ AS supplier_formular_typ,
              COALESCE(p.position_count,0)::int AS position_count
       FROM "order".order_orders o
       JOIN "order".order_suppliers s ON s.id=o.supplier_id
       LEFT JOIN (SELECT order_id, COUNT(*) AS position_count
                  FROM "order".order_order_positions GROUP BY order_id) p ON p.order_id=o.id
       ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
       ORDER BY o.bestelldatum DESC, o.created_at DESC`,
      params
    );
    return res.json({ status: 'ok', count: result.rows.length, items: result.rows.map(orderDto),
      permissions: { canReadAll: canReadAllOrders(req.user?.role) } });
  } catch (error) {
    console.error('GET /api/bestellungen:', error);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

router.get('/:id/pdf', verifyToken(), async (req, res) => {
  try {
    await loadOrderHeader(db, req.params.id, req.user, { requireGlobal: true });
    const pdf = await generateMellerudOrderPdf(req.params.id);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="Bestellung_${req.params.id}.pdf"`);
    return res.send(pdf);
  } catch (error) {
    console.error('GET /api/bestellungen/:id/pdf:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'PDF konnte nicht erzeugt werden' });
  }
});

router.post('/:id/resend', verifyToken(), async (req, res) => {
  try {
    await loadOrderHeader(db, req.params.id, req.user, { requireGlobal: true });
    const delivery = await dispatchOrder(req.params.id);
    return res.status(delivery.status === 'failed' ? 502 : 200).json({
      status: delivery.status,
      message: delivery.status === 'sent' ? 'Bestellung wurde versendet' : delivery.message,
      delivery,
    });
  } catch (error) {
    return res.status(error.statusCode || 500).json({ message: error.message || 'Versand fehlgeschlagen' });
  }
});

router.get('/:id', verifyToken(), async (req, res) => {
  try {
    const detail = await loadOrderDetail(db, req.params.id, req.user);
    return res.json({ status: 'ok', order: detail,
      permissions: { canDownloadPdf: canReadAllOrders(req.user?.role), canResend: canReadAllOrders(req.user?.role) } });
  } catch (error) {
    console.error('GET /api/bestellungen/:id:', error);
    return res.status(error.statusCode || 500).json({ message: error.statusCode ? error.message : 'Serverfehler' });
  }
});

router.post('/', verifyToken(), async (req, res) => {
  let client;
  try {
    const { order } = req.body || {};
    if (!order || typeof order !== 'object') return res.status(400).json({ message: 'order fehlt oder ist ungültig' });
    const supplierCode = normalizeSupplierCode(order.supplier);
    if (!supplierCode) return res.status(400).json({ message: 'order.supplier fehlt oder ist ungültig' });
    if (!isValidIsoDate(order.bestelldatum)) return res.status(400).json({ message: 'order.bestelldatum ist ungültig (YYYY-MM-DD)' });
    if (!Array.isArray(order.positionen) || order.positionen.length === 0) return res.status(400).json({ message: 'order.positionen fehlen oder sind leer' });
    if (!normalizeText(req.user?.name)) return res.status(400).json({ message: 'Benutzername im Token fehlt' });
    const requestedArticleIds = order.positionen.map((item, index) => ensureUuid(item?.articleId, `order.positionen[${index}].articleId`));
    const effectiveFiliale = requestedBranchForUser(req.user, order.filiale);
    if (!effectiveFiliale) return res.status(400).json({ message: 'Keine zulässige Bestellfiliale vorhanden' });

    client = await db.connect();
    await client.query('BEGIN');
    const supplierResult = await client.query(
      `SELECT id, name, code, formular_typ, order_email, minimum_order_ve
       FROM "order".order_suppliers WHERE lower(code)=lower($1) AND aktiv=true LIMIT 1`,
      [supplierCode]
    );
    if (supplierResult.rows.length === 0) { const error = new Error('Lieferant nicht gefunden oder inaktiv'); error.statusCode = 404; throw error; }
    const supplier = supplierResult.rows[0];
    if (!supportsMellerudWorkflow(supplier)) {
      const error = new Error('Für diesen Lieferanten ist noch kein verbindlicher Bestellworkflow verfügbar'); error.statusCode = 409; throw error;
    }
    const branchesResult = await client.query('SELECT name FROM public.filialen WHERE aktiv=true ORDER BY name');
    const activeBranches = branchesResult.rows.map((row) => row.name);
    const profileResult = await client.query(
      `SELECT filiale, firma, strasse, ort, kunden_nr, auftrags_nr,
              gespraechspartner, telefon, email
       FROM "order".order_supplier_branch_profiles
       WHERE supplier_id=$1 AND filiale=$2 AND aktiv=true LIMIT 1`,
      [supplier.id, effectiveFiliale]
    );
    if (profileResult.rows.length === 0) { const error = new Error(`Kein aktives Filialprofil für ${effectiveFiliale} vorhanden`); error.statusCode = 409; throw error; }
    const profile = profileResult.rows[0];
    const articlesResult = await client.query(
      `SELECT a.id, a.supplier_article_no, a.kunden_art_nr, a.ean, a.name,
              a.ve_stueck, a.sort_index, p.ek_einzel, p.ek_pro_karton
       FROM "order".order_supplier_articles a
       JOIN LATERAL (
         SELECT ek_einzel, ek_pro_karton FROM "order".order_supplier_article_prices
         WHERE article_id=a.id AND gueltig_ab<=$2::date
           AND (gueltig_bis IS NULL OR gueltig_bis>=$2::date)
         ORDER BY gueltig_ab DESC LIMIT 1
       ) p ON true
       WHERE a.id=ANY($1::uuid[]) AND a.supplier_id=$3 AND a.aktiv=true`,
      [requestedArticleIds, order.bestelldatum, supplier.id]
    );
    const plan = calculateOrderPlan({
      articles: articlesResult.rows,
      requestedPositions: order.positionen,
      splitDetails: order.split_details,
      sourceFiliale: effectiveFiliale,
      validBranches: activeBranches,
      minimumOrderVe: supplier.minimum_order_ve,
    });
    const orderResult = await client.query(
      `INSERT INTO "order".order_orders (
         supplier_id, filiale, ordered_by_name, bestelldatum, status,
         gesamtsumme_netto, gesamt_ve, budget_booking_id, split_snapshot,
         supplier_formular_typ_snapshot, firma_snapshot, kunden_nr_snapshot,
         strasse_snapshot, ort_snapshot, auftrags_nr_snapshot,
         gespraechspartner_snapshot, telefon_snapshot, email_snapshot,
         dispatch_status, dispatch_mode
       ) VALUES ($1,$2,$3,$4::date,'saved',$5,$6,NULL,$7::jsonb,$8,$9,$10,$11,$12,$13,$14,$15,$16,'pending',$17)
       RETURNING *`,
      [
        supplier.id, plan.sourceFiliale, req.user.name, order.bestelldatum,
        plan.totalNet, plan.totalVe, JSON.stringify(plan.splitSnapshot), supplier.formular_typ,
        profile.firma, profile.kunden_nr, profile.strasse, profile.ort,
        profile.auftrags_nr, profile.gespraechspartner, profile.telefon, profile.email,
        normalizeText(process.env.ORDER_DISPATCH_MODE).toLowerCase() === 'final' ? 'final' : 'light',
      ]
    );
    const createdOrder = orderResult.rows[0];
    const createdPositions = [];
    for (const item of plan.positions) {
      const article = item.article;
      const position = await client.query(
        `INSERT INTO "order".order_order_positions (
           order_id, article_id, supplier_article_no_snapshot, kunden_art_nr_snapshot,
           ean_snapshot, name_snapshot, ve_stueck_snapshot, ek_einzel_snapshot,
           ek_pro_karton_snapshot, menge_kartons, positionssumme_netto, sort_index_snapshot
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
        [
          createdOrder.id, article.id, article.supplier_article_no, article.kunden_art_nr,
          article.ean, article.name, item.veSize,
          centsToMoney(item.pricePerVeCents) / item.veSize,
          centsToMoney(item.pricePerVeCents), item.quantityVe,
          centsToMoney(item.totalCents), article.sort_index,
        ]
      );
      createdPositions.push(position.rows[0]);
    }
    const { jahr, kw } = await resolveBudgetYearWeek(client, order.bestelldatum);
    const sourceWeekBudgetId = await ensureWeekBudget(client, plan.sourceFiliale, jahr, kw);
    let budgetBookingId;
    if (plan.budgetSplits.length > 0) {
      const resolvedSplits = [];
      for (const split of plan.budgetSplits) {
        resolvedSplits.push({ target_week_budget_id: await ensureWeekBudget(client, split.filiale, jahr, kw), betrag: split.betrag });
      }
      const splitResult = await client.query(
        'SELECT budget.create_split_booking($1::jsonb,$2::text) AS result',
        [JSON.stringify({ source_week_budget_id: sourceWeekBudgetId, gesamtbetrag: plan.totalNet,
          typ: 'bestellung', source: 'BESTELLUNG', status: 'offen', datum: order.bestelldatum,
          lieferant: supplier.name, aktion_nr: null, beschreibung: null, splits: resolvedSplits }), req.user.name]
      );
      budgetBookingId = splitResult.rows[0]?.result?.parent_id;
      if (!budgetBookingId) throw new Error('Split-Budgetbuchung konnte nicht erzeugt werden');
    } else {
      const booking = await client.query(
        `INSERT INTO budget.bookings (week_budget_id,datum,typ,betrag,lieferant,status,created_by,created_at,source)
         VALUES ($1,$2::date,'bestellung',$3,$4,'offen',$5,NOW(),'BESTELLUNG') RETURNING id`,
        [sourceWeekBudgetId, order.bestelldatum, plan.totalNet, supplier.name, req.user.name]
      );
      budgetBookingId = booking.rows[0].id;
    }
    await client.query('COMMIT');
    client.release();
    client = null;
    const delivery = await dispatchOrder(createdOrder.id);
    const saved = await loadOrderDetail(db, createdOrder.id, req.user);
    return res.status(201).json({
      status: 'ok', message: 'Bestellung wurde verbindlich und unveränderlich gespeichert',
      order: saved, items: createdPositions, delivery,
      meta: { budgetBookingCreated: true, budgetBookingId, orderBudgetLinked: false, readOnly: true },
    });
  } catch (error) {
    if (client) { try { await client.query('ROLLBACK'); } catch (rollbackError) { console.error('Bestell-Rollback:', rollbackError); } }
    console.error('POST /api/bestellungen:', error);
    const status = error.statusCode || (error.code ? (error.code === '23505' ? 409 : 500) : 400);
    return res.status(status).json({ message: status >= 500 ? 'Serverfehler' : error.message });
  } finally {
    if (client) client.release();
  }
});

module.exports = router;
module.exports.dispatchOrder = dispatchOrder;
