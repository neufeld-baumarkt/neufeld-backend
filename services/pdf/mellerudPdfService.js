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

function chunkArray(items, pageSize) {
  const chunks = [];
  for (let i = 0; i < items.length; i += pageSize) {
    chunks.push(items.slice(i, i + pageSize));
  }
  return chunks;
}

function buildMellerudLogo() {
  return `
    <div class="logo-box">
      <div class="logo-main">MELLERUD</div>
      <div class="logo-sub">UND GUT.</div>
    </div>
  `;
}

function buildHeader() {
  return `
    <div class="page-header">
      <div class="header-inner">
        ${buildMellerudLogo()}
        <div class="header-title">Auftragsformular Classic</div>
      </div>
      <div class="red-line"></div>
    </div>
  `;
}

function buildFooter(pageNumber) {
  return `
    <div class="page-footer">
      <div class="footer-version">05.2021</div>
      <div class="footer-blue">
        <div class="footer-company">
          <strong>MELLERUD CHEMIE GMBH</strong><br />
          Bernhard-Röttgen-Waldweg 20 - 41379 Brüggen - Tel. 02163/950900 - Fax 02163/95090120<br />
          E-Mail: innendienst@mellerud.de - Internet: www.mellerud.de
        </div>
        <div class="footer-page">${pageNumber}</div>
      </div>
    </div>
  `;
}

function buildMetaBlock(order) {
  return `
    <div class="meta-grid">
      <div class="meta-row">
        <div class="meta-label">Firma:</div>
        <div class="meta-line">${escapeHtml(order.firma_snapshot)}</div>
      </div>
      <div class="meta-row">
        <div class="meta-label">Kunden-Nr.:</div>
        <div class="meta-line">${escapeHtml(order.kunden_nr_snapshot)}</div>
      </div>

      <div class="meta-row">
        <div class="meta-label">Straße:</div>
        <div class="meta-line">${escapeHtml(order.strasse_snapshot)}</div>
      </div>
      <div class="meta-row">
        <div class="meta-label">Auftrags-Nr.:</div>
        <div class="meta-line">${escapeHtml(order.auftrags_nr_snapshot)}</div>
      </div>

      <div class="meta-row">
        <div class="meta-label">Ort:</div>
        <div class="meta-line">${escapeHtml(order.ort_snapshot)}</div>
      </div>
      <div class="meta-row">
        <div class="meta-label">Gesprächspartner:</div>
        <div class="meta-line">${escapeHtml(order.gespraechspartner_snapshot)}</div>
      </div>
    </div>

    <div class="order-meta">
      <span><strong>Filiale:</strong> ${escapeHtml(order.filiale)}</span>
      <span><strong>Bestelldatum:</strong> ${escapeHtml(formatDate(order.bestelldatum))}</span>
      <span><strong>Bestellt von:</strong> ${escapeHtml(order.ordered_by_name)}</span>
      <span><strong>Bestell-ID:</strong> ${escapeHtml(order.id)}</span>
    </div>
  `;
}

function buildArticleRows(articles, orderedMap) {
  return articles
    .map((article) => {
      const ordered = orderedMap.get(article.id) || null;

      return `
        <tr>
          <td class="col-ean">${escapeHtml(article.ean)}</td>
          <td class="col-artnr">${escapeHtml(article.supplier_article_no)}</td>
          <td class="col-name">${escapeHtml(article.name)}</td>
          <td class="col-ve">${escapeHtml(article.ve_stueck)}</td>
          <td class="col-kartons">${ordered ? escapeHtml(ordered.menge_kartons) : ''}</td>
        </tr>
      `;
    })
    .join('');
}

function buildArticleTable(articles, orderedMap) {
  return `
    <table class="article-table">
      <thead>
        <tr>
          <th class="col-ean">EAN-Nr.</th>
          <th class="col-artnr">MELLERUD<br />Art.-Nr.</th>
          <th class="col-name">Artikel-Bezeichnung</th>
          <th class="col-ve">VE / Stück</th>
          <th class="col-kartons">Kartons</th>
        </tr>
      </thead>
      <tbody>
        ${buildArticleRows(articles, orderedMap)}
      </tbody>
    </table>
  `;
}

function buildSignatureBlock() {
  return `
    <div class="signature-area">
      <div class="signature-row">
        <div class="signature-field">
          <div class="signature-line"></div>
          <div class="signature-label">Bestelldatum</div>
        </div>

        <div class="signature-field">
          <div class="signature-line"></div>
          <div class="signature-label">Unterschrift Besteller</div>
        </div>

        <div class="stamp-field">
          <div class="stamp-box"></div>
          <div class="signature-label">Firmenstempel</div>
        </div>
      </div>

      <div class="legal-text">
        Die Ware bleibt bis zur vollständigen Bezahlung unser Eigentum. Gerichtsstand: 41747 Viersen
      </div>
    </div>
  `;
}

function buildMellerudHtml({ order, articles, orderedMap }) {
  const firstPageRows = 32;
  const followingPageRows = 37;

  const firstPageArticles = articles.slice(0, firstPageRows);
  const restArticles = articles.slice(firstPageRows);
  const restPages = chunkArray(restArticles, followingPageRows);
  const allPages = [firstPageArticles, ...restPages];

  return `
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <title>Mellerud Bestellung</title>
  <style>
    @page {
      size: A4;
      margin: 0;
    }

    * {
      box-sizing: border-box;
    }

    html,
    body {
      margin: 0;
      padding: 0;
      font-family: Arial, Helvetica, sans-serif;
      color: #000;
      background: #fff;
    }

    .page {
      position: relative;
      width: 210mm;
      height: 297mm;
      padding: 10mm 14mm 22mm 14mm;
      page-break-after: always;
      overflow: hidden;
      background: #fff;
    }

    .page:last-child {
      page-break-after: auto;
    }

    .page-header {
      width: 100%;
      margin-bottom: 8mm;
    }

    .header-inner {
      display: grid;
      grid-template-columns: 38mm 1fr;
      align-items: start;
      min-height: 18mm;
    }

    .logo-box {
      width: 35mm;
      height: 16mm;
      background: #e30613;
      color: #fff;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      font-weight: bold;
      margin-left: 0.5mm;
    }

    .logo-main {
      font-size: 15px;
      line-height: 1;
      letter-spacing: 0.4px;
    }

    .logo-sub {
      font-size: 7px;
      line-height: 1;
      margin-top: 3px;
      letter-spacing: 0.6px;
    }

    .header-title {
      color: #004a83;
      font-size: 25px;
      font-weight: bold;
      padding-top: 3mm;
      text-align: center;
      padding-right: 22mm;
    }

    .red-line {
      height: 0.35mm;
      background: #d11f2a;
      margin-top: 6mm;
      margin-left: -14mm;
      margin-right: -14mm;
    }

    .meta-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      column-gap: 9mm;
      row-gap: 4mm;
      margin: 0 0 6mm 0;
      color: #004a83;
      font-size: 12px;
      font-weight: bold;
    }

    .meta-row {
      display: grid;
      grid-template-columns: auto 1fr;
      column-gap: 2mm;
      align-items: end;
    }

    .meta-label {
      white-space: nowrap;
    }

    .meta-line {
      min-height: 5mm;
      border-bottom: 0.35mm solid #004a83;
      color: #000;
      font-size: 10px;
      font-weight: normal;
      padding-left: 1mm;
      line-height: 5mm;
      overflow: hidden;
      white-space: nowrap;
    }

    .order-meta {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1.5mm 6mm;
      margin-bottom: 5mm;
      font-size: 8px;
      color: #000;
    }

    .article-table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      font-size: 6.2px;
    }

    .article-table th {
      background: #004a83;
      color: #fff;
      font-weight: bold;
      text-align: center;
      border: 0.25mm solid #8aa9c0;
      padding: 1.8mm 1mm;
      line-height: 1.05;
      vertical-align: middle;
    }

    .article-table td {
      border: 0.25mm solid #a9a9a9;
      padding: 1mm 1mm;
      line-height: 1.05;
      height: 5.05mm;
      vertical-align: middle;
      overflow: hidden;
    }

    .col-ean {
      width: 25mm;
      text-align: center;
      white-space: nowrap;
    }

    .col-artnr {
      width: 24mm;
      text-align: center;
      white-space: nowrap;
    }

    .col-name {
      width: auto;
      text-align: left;
    }

    th.col-name {
      text-align: center;
    }

    .col-ve {
      width: 22mm;
      text-align: center;
      white-space: nowrap;
    }

    .col-kartons {
      width: 23mm;
      text-align: center;
      white-space: nowrap;
      font-weight: bold;
    }

    .page-footer {
      position: absolute;
      left: 0;
      right: 0;
      bottom: 0;
      height: 18mm;
    }

    .footer-version {
      position: absolute;
      right: 13mm;
      top: -5mm;
      font-size: 6px;
      color: #000;
    }

    .footer-blue {
      position: absolute;
      left: 0;
      right: 0;
      bottom: 0;
      height: 14mm;
      background: #004a83;
      color: #fff;
      text-align: center;
      font-size: 6.5px;
      line-height: 1.2;
      padding-top: 3mm;
    }

    .footer-company strong {
      font-size: 7px;
    }

    .footer-page {
      position: absolute;
      right: 14mm;
      bottom: 3mm;
      font-size: 8px;
      font-weight: bold;
    }

    .signature-area {
      position: absolute;
      left: 14mm;
      right: 14mm;
      bottom: 20mm;
      color: #004a83;
      font-size: 8px;
      font-weight: bold;
    }

    .signature-row {
      display: grid;
      grid-template-columns: 1fr 1.4fr 1fr;
      gap: 9mm;
      align-items: end;
    }

    .signature-field {
      min-height: 17mm;
      display: flex;
      flex-direction: column;
      justify-content: flex-end;
    }

    .signature-line {
      border-bottom: 0.35mm solid #004a83;
      height: 8mm;
      margin-bottom: 1mm;
    }

    .signature-label {
      text-align: center;
    }

    .stamp-field {
      min-height: 25mm;
      display: flex;
      flex-direction: column;
      justify-content: flex-end;
    }

    .stamp-box {
      height: 22mm;
      border: 0.35mm solid #004a83;
      margin-bottom: 1mm;
    }

    .legal-text {
      margin-top: 5mm;
      text-align: center;
      font-size: 6.8px;
      color: #004a83;
      font-weight: normal;
    }
  </style>
</head>
<body>
  ${allPages
    .map((pageArticles, pageIndex) => {
      const pageNumber = pageIndex + 1;
      const isFirstPage = pageIndex === 0;
      const isLastPage = pageIndex === allPages.length - 1;

      return `
        <section class="page">
          ${buildHeader()}

          ${isFirstPage ? buildMetaBlock(order) : ''}

          ${buildArticleTable(pageArticles, orderedMap)}

          ${isLastPage ? buildSignatureBlock() : ''}

          ${buildFooter(pageNumber)}
        </section>
      `;
    })
    .join('')}
</body>
</html>
  `;
}

async function loadMellerudPdfData(orderId) {
  const orderResult = await db.query(
    `
    SELECT
      o.id,
      o.supplier_id,
      o.filiale,
      o.ordered_by_name,
      o.bestelldatum,
      o.status,
      o.gesamtsumme_netto,
      o.supplier_formular_typ_snapshot,
      o.firma_snapshot,
      o.kunden_nr_snapshot,
      o.strasse_snapshot,
      o.ort_snapshot,
      o.auftrags_nr_snapshot,
      o.gespraechspartner_snapshot,
      o.created_at,
      o.updated_at,
      s.name AS supplier_name,
      s.code AS supplier_code,
      s.formular_typ AS supplier_formular_typ_live
    FROM "order".order_orders o
    INNER JOIN "order".order_suppliers s
      ON s.id = o.supplier_id
    WHERE o.id = $1
      AND s.code = 'mellerud'
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
    SELECT
      article_id,
      supplier_article_no_snapshot,
      ean_snapshot,
      name_snapshot,
      ve_stueck_snapshot,
      menge_kartons,
      positionssumme_netto,
      sort_index_snapshot
    FROM "order".order_order_positions
    WHERE order_id = $1
    ORDER BY sort_index_snapshot ASC NULLS LAST, name_snapshot ASC
    `,
    [orderId]
  );

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
    ORDER BY sort_index ASC NULLS LAST, name ASC
    `,
    [order.supplier_id]
  );

  const orderedMap = new Map();

  for (const position of positionsResult.rows) {
    orderedMap.set(position.article_id, {
      menge_kartons: position.menge_kartons,
      positionssumme_netto: position.positionssumme_netto,
    });
  }

  return {
    order,
    supplier: {
      id: order.supplier_id,
      name: order.supplier_name,
      code: order.supplier_code,
      formular_typ: order.supplier_formular_typ_live || order.supplier_formular_typ_snapshot || null,
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

  if (!Array.isArray(data.articles) || data.articles.length === 0) {
    throw new Error('Keine aktiven Mellerud-Artikel für PDF gefunden');
  }

  const html = buildMellerudHtml(data);

  let browser = null;

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
      preferCSSPageSize: true,
      displayHeaderFooter: false,
    });

    if (pdfBuffer instanceof Uint8Array) {
      pdfBuffer = Buffer.from(pdfBuffer);
    }

    if (!pdfBuffer || !Buffer.isBuffer(pdfBuffer)) {
      throw new Error('PDF-Erzeugung lieferte keinen Buffer');
    }

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