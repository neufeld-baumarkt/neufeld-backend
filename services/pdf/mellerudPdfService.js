const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts } = require('pdf-lib');
const db = require('../../db');

/**
 * TEMPLATE DATEI (Original!)
 * → HIER MUSS DEIN ORIGINAL PDF LIEGEN
 */
const TEMPLATE_PATH = path.join(
  __dirname,
  'templates',
  'mellerud_original.pdf'
);

/**
 * ⚠️ KOORDINATEN DEFINIEREN
 * → das ist der EINZIGE Bereich den wir später feinjustieren
 */
const CARTON_COORDS = {
  // Beispiel Seite 1
  0: [
    { x: 520, y: 520 }, // Artikel 1
    { x: 520, y: 505 }, // Artikel 2
    { x: 520, y: 490 }, // Artikel 3
    { x: 520, y: 475 },
    { x: 520, y: 460 },
    { x: 520, y: 445 },
    { x: 520, y: 430 },
    { x: 520, y: 415 },
    { x: 520, y: 400 },
    { x: 520, y: 385 },
    { x: 520, y: 370 },
    { x: 520, y: 355 },
    { x: 520, y: 340 },
    { x: 520, y: 325 },
    { x: 520, y: 310 },
    { x: 520, y: 295 },
    { x: 520, y: 280 },
    { x: 520, y: 265 },
    { x: 520, y: 250 },
    { x: 520, y: 235 },
    { x: 520, y: 220 },
    { x: 520, y: 205 },
    { x: 520, y: 190 },
    { x: 520, y: 175 },
    { x: 520, y: 160 },
    { x: 520, y: 145 },
    { x: 520, y: 130 },
    { x: 520, y: 115 },
    { x: 520, y: 100 },
    { x: 520, y: 85 }
  ],

  // Seite 2 → später erweitern
  1: []
};

async function loadOrderData(orderId) {
  const orderRes = await db.query(
    `
    SELECT o.*, s.code
    FROM "order".order_orders o
    JOIN "order".order_suppliers s ON s.id = o.supplier_id
    WHERE o.id = $1
    `,
    [orderId]
  );

  if (orderRes.rows.length === 0) {
    throw new Error('Order nicht gefunden');
  }

  const positionsRes = await db.query(
    `
    SELECT *
    FROM "order".order_order_positions
    WHERE order_id = $1
    `,
    [orderId]
  );

  return {
    order: orderRes.rows[0],
    positions: positionsRes.rows
  };
}

function buildCartonMap(positions) {
  const map = new Map();

  positions.forEach((p) => {
    map.set(p.article_id, p.menge_kartons);
  });

  return map;
}

async function generateMellerudOrderPdf(orderId) {
  const { order, positions } = await loadOrderData(orderId);

  const existingPdfBytes = fs.readFileSync(TEMPLATE_PATH);

  const pdfDoc = await PDFDocument.load(existingPdfBytes);

  const font = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  const pages = pdfDoc.getPages();

  const cartonMap = buildCartonMap(positions);

  /**
   * WICHTIG:
   * Reihenfolge der DB-Artikel = Reihenfolge im Formular
   * → deshalb KEIN extra Artikel-Query nötig
   */

  const values = Array.from(cartonMap.values());

  let globalIndex = 0;

  pages.forEach((page, pageIndex) => {
    const coords = CARTON_COORDS[pageIndex] || [];

    coords.forEach((pos) => {
      const value = values[globalIndex];

      if (value && value > 0) {
        page.drawText(String(value), {
          x: pos.x,
          y: pos.y,
          size: 10,
          font
        });
      }

      globalIndex++;
    });
  });

  const pdfBytes = await pdfDoc.save();

  return Buffer.from(pdfBytes);
}

module.exports = {
  generateMellerudOrderPdf
};