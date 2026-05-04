const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts } = require('pdf-lib');
const db = require('../../db');

const TEMPLATE_PATH = path.join(
  __dirname,
  'templates',
  'mellerud_original.pdf'
);

const CARTON_X = 520;
const START_Y = 520;
const ROW_STEP = 15;

const PAGE_ARTICLE_NUMBERS = {
  0: [
    '2003109168',
    '2001004026',
    '2001002008',
    '2001000110',
    '2001004040',
    '2001000127',
    '2001002527',
    '2001001575',
    '2001001766',
    '2001002404',
    '2001002060',
    '2001000042',
    '2001000356',
    '2001000806',
    '2004050047',
    '2001000851',
    '2001002114',
    '2001001780',
    '2001001629',
    '2003203265',
    '2003203579',
    '2001000943',
    '2001002701',
    '2001001391',
    '2001001254',
    '2001001445',
    '2002010270',
    '2001000059',
    '2001000066',
    '2003203531',
    '2001000332',
    '2001002275',
    '2001001131',
    '2003203272',
  ],

  1: [
    '2001000349',
    '2001001476',
    '2001001803',
    '2001002718',
    '2001001896',
    '2001000301',
    '2001004033',
    '2001001438',
    '2001002046',
    '2001002374',
    '2001002800',
    '2001002640',
    '2001002138',
    '2001002756',
    '2001002794',
    '2001010447',
    '2001010546',
    '2001002671',
    '2001001186',
    '2001001032',
    '2001002176',
    '2003203548',
    '2001000219',
    '2001000073',
    '2003203555',
    '2001001612',
    '2003203524',
    '2001000271',
    '2001002169',
    '2001001544',
    '2001002688',
    '2001000233',
    '2001010409',
    '2001010614',
    '2001002428',
    '2001002442',
    '2001000875',
  ],

  2: [
    '2001000158',
    '2001000950',
    '2001000172',
    '2001002350',
    '2001002367',
    '2001000165',
    '2001001513',
    '2001001490',
    '2049007737',
    '2049007720',
    '2003109151',
    '2003109106',
    '2001001056',
    '2001000134',
    '2001001995',
    '2001005108',
    '2001000493',
    '2001009281',
    '2001009250',
    '2001001582',
    '2003203500',
    '2001009618',
    '2003203517',
    '2001009601',
    '2001000097',
    '2001009267',
    '2001009205',
    '2001000240',
    '2049408345',
    '2049408291',
    '2001001773',
    '2003203562',
    '2049300074',
    '2001002077',
    '2001000035',
    '2001001469',
    '2001002695',
  ],

  3: [
    '2001002824',
    '2001003364',
    '2001005009',
    '2001000820',
    '2001004019',
    '2001001704',
    '2001000325',
    '2001000936',
    '2001001636',
    '2001002039',
    '2001005207',
    '2001000004',
    '2001000011',
    '2001000981',
  ],
};

function buildCartonFieldMap() {
  const map = new Map();

  Object.entries(PAGE_ARTICLE_NUMBERS).forEach(([pageIndexRaw, articleNumbers]) => {
    const pageIndex = Number(pageIndexRaw);

    articleNumbers.forEach((supplierArticleNo, rowIndex) => {
      map.set(supplierArticleNo, {
        pageIndex,
        x: CARTON_X,
        y: START_Y - rowIndex * ROW_STEP,
      });
    });
  });

  return map;
}

const CARTON_FIELD_MAP = buildCartonFieldMap();

async function loadOrderData(orderId) {
  const orderRes = await db.query(
    `
    SELECT
      o.*,
      s.code AS supplier_code
    FROM "order".order_orders o
    JOIN "order".order_suppliers s
      ON s.id = o.supplier_id
    WHERE o.id = $1
      AND s.code = 'mellerud'
    LIMIT 1
    `,
    [orderId]
  );

  if (orderRes.rows.length === 0) {
    throw new Error('Mellerud-Order nicht gefunden');
  }

  const positionsRes = await db.query(
    `
    SELECT
      supplier_article_no_snapshot,
      menge_kartons
    FROM "order".order_order_positions
    WHERE order_id = $1
    ORDER BY sort_index_snapshot ASC NULLS LAST
    `,
    [orderId]
  );

  return {
    order: orderRes.rows[0],
    positions: positionsRes.rows,
  };
}

function buildCartonMapBySupplierArticleNo(positions) {
  const map = new Map();

  positions.forEach((position) => {
    const supplierArticleNo = String(position.supplier_article_no_snapshot || '').trim();
    const cartons = Number(position.menge_kartons || 0);

    if (!supplierArticleNo || !Number.isFinite(cartons) || cartons <= 0) {
      return;
    }

    map.set(supplierArticleNo, cartons);
  });

  return map;
}

async function generateMellerudOrderPdf(orderId) {
  if (!orderId || typeof orderId !== 'string') {
    throw new Error('orderId fehlt oder ist ungültig');
  }

  if (!fs.existsSync(TEMPLATE_PATH)) {
    throw new Error(`Mellerud PDF Template nicht gefunden: ${TEMPLATE_PATH}`);
  }

  const { positions } = await loadOrderData(orderId);
  const cartonMap = buildCartonMapBySupplierArticleNo(positions);

  const existingPdfBytes = fs.readFileSync(TEMPLATE_PATH);
  const pdfDoc = await PDFDocument.load(existingPdfBytes);
  const font = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const pages = pdfDoc.getPages();

  const missingMappings = [];

  cartonMap.forEach((cartons, supplierArticleNo) => {
    const coord = CARTON_FIELD_MAP.get(supplierArticleNo);

    if (!coord) {
      missingMappings.push(supplierArticleNo);
      return;
    }

    const page = pages[coord.pageIndex];

    if (!page) {
      missingMappings.push(supplierArticleNo);
      return;
    }

    page.drawText(String(cartons), {
      x: coord.x,
      y: coord.y,
      size: 10,
      font,
    });
  });

  if (missingMappings.length > 0) {
    console.warn('MELLERUD PDF: fehlende Koordinaten für Artikel:', missingMappings);
  }

  const pdfBytes = await pdfDoc.save();

  await db.query(
    `
    UPDATE "order".order_orders
    SET
      pdf_generated_at = NOW(),
      updated_at = NOW()
    WHERE id = $1
    `,
    [orderId]
  );

  return Buffer.from(pdfBytes);
}

module.exports = {
  generateMellerudOrderPdf,
};