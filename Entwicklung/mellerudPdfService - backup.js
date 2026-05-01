const puppeteer = require('puppeteer');
const db = require('../../db');

function escapeHtml(value) {
  if (value === null || value === undefined) return '';

  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function formatDate(value) {
  if (!value) return '';

  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '';

  return d.toLocaleDateString('de-DE', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
  });
}

function buildMellerudHtml({ order, supplier, articles, orderedMap }) {
  const rowsHtml = articles
    .map((article) => {
      const ordered = orderedMap.get(article.id) || null;

      return `
        <tr>
          <td class="ean">${escapeHtml(article.ean)}</td>
          <td class="artnr">${escapeHtml(article.supplier_article_no)}</td>
          <td class="name">${escapeHtml(article.name)}</td>
          <td class="ve">${escapeHtml(article.ve_stueck)}</td>
          <td class="kartons">${ordered ? escapeHtml(ordered.menge_kartons) : ''}</td>
        </tr>
      `;
    })
    .join('');

  return `<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8" />
<title>Mellerud Bestellung</title>
<style>
@page { size: A4; margin: 14mm 13mm 16mm 13mm; }
body { font-family: Arial; font-size: 10px; }
table { width: 100%; border-collapse: collapse; }
th, td { font-size: 10px; padding: 2px; }
.kartons { border-bottom: 1px solid #000; font-weight: bold; }
</style>
</head>
<body>

<h2>MELLERUD Bestellung</h2>

<div>
<strong>Filiale:</strong> ${escapeHtml(order.filiale)} |
<strong>Datum:</strong> ${formatDate(order.bestelldatum)} |
<strong>Besteller:</strong> ${escapeHtml(order.ordered_by_name)}
</div>

<table>
<thead>
<tr>
<th>EAN</th>
<th>Art-Nr</th>
<th>Name</th>
<th>VE</th>
<th>Kartons</th>
</tr>
</thead>
<tbody>
${rowsHtml}
</tbody>
</table>

</body>
</html>`;
}

async function loadMellerudPdfData(orderId) {
  const orderResult = await db.query(
    `
    SELECT o.*, s.name AS supplier_name, s.code AS supplier_code
    FROM "order".order_orders o
    INNER JOIN "order".order_suppliers s ON s.id = o.supplier_id
    WHERE o.id = $1 AND s.code = 'mellerud'
    LIMIT 1
    `,
    [orderId]
  );

  if (orderResult.rows.length === 0) {
    throw new Error(`Mellerud-Bestellung nicht gefunden: ${orderId}`);
  }

  const order = orderResult.rows[0];

  const positionsResult = await db.query(
    `
    SELECT *
    FROM "order".order_order_positions
    WHERE order_id = $1
    `,
    [orderId]
  );

  const articlesResult = await db.query(
    `
    SELECT *
    FROM "order".order_supplier_articles
    WHERE supplier_id = $1 AND aktiv = true
    `,
    [order.supplier_id]
  );

  const orderedMap = new Map();
  for (const pos of positionsResult.rows) {
    orderedMap.set(pos.article_id, pos);
  }

  return {
    order,
    supplier: {
      name: order.supplier_name,
      code: order.supplier_code,
    },
    articles: articlesResult.rows,
    orderedMap,
  };
}

async function generateMellerudOrderPdf(orderId) {
  if (!orderId || typeof orderId !== 'string') {
    throw new Error('orderId fehlt oder ist ungültig');
  }

  const data = await loadMellerudPdfData(orderId);
  const html = buildMellerudHtml(data);

  let browser;

  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    const page = await browser.newPage();

    await page.setContent(html, {
      waitUntil: 'networkidle0',
    });

    let pdfBuffer = await page.pdf({
      format: 'A4',
      printBackground: true,
    });

    // 🔥 FIX: Uint8Array → Buffer
    if (pdfBuffer instanceof Uint8Array) {
      pdfBuffer = Buffer.from(pdfBuffer);
    }

    if (!pdfBuffer || !Buffer.isBuffer(pdfBuffer)) {
      throw new Error('PDF-Erzeugung lieferte keinen Buffer');
    }

    await db.query(
      `
      UPDATE "order".order_orders
      SET pdf_generated_at = NOW()
      WHERE id = $1
      `,
      [orderId]
    );

    return pdfBuffer;
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

module.exports = {
  generateMellerudOrderPdf,
};