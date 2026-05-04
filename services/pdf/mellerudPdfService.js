const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts } = require('pdf-lib');
const db = require('../../db');

const TEMPLATE_PATH = path.join(
  __dirname,
  'templates',
  'mellerud_original.pdf'
);

// 🔥 HIER IST DER KERN
// interne Artikelnummer → PDF Position
const ARTICLE_COORDS = {
  '150000004': { page: 1, x: 520, y: 520 }, // Grabstein Reiniger
  '150000006': { page: 1, x: 520, y: 400 }, // Fliesen Reiniger
  '150000012': { page: 2, x: 520, y: 460 }, // Oberflächen Reiniger
};

async function loadOrderData(orderId) {
  const positionsRes = await db.query(
    `
    SELECT
      supplier_article_no_snapshot,
      menge_kartons
    FROM "order".order_order_positions
    WHERE order_id = $1
    `,
    [orderId]
  );

  return positionsRes.rows;
}

async function generateMellerudOrderPdf(orderId) {
  const positions = await loadOrderData(orderId);

  const existingPdfBytes = fs.readFileSync(TEMPLATE_PATH);
  const pdfDoc = await PDFDocument.load(existingPdfBytes);
  const font = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const pages = pdfDoc.getPages();

  positions.forEach((pos) => {
    const artikel = String(pos.supplier_article_no_snapshot);
    const menge = Number(pos.menge_kartons);

    if (!menge || menge <= 0) return;

    const coord = ARTICLE_COORDS[artikel];

    if (!coord) {
      console.warn('Kein Mapping für:', artikel);
      return;
    }

    const page = pages[coord.page];

    page.drawText(String(menge), {
      x: coord.x,
      y: coord.y,
      size: 10,
      font,
    });
  });

  const pdfBytes = await pdfDoc.save();
  return Buffer.from(pdfBytes);
}

module.exports = {
  generateMellerudOrderPdf
};