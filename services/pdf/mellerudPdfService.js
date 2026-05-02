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

/* 🔥 KEIN LOGO MEHR */
function buildHeader() {
  return `
    <div class="page-header">
      <div class="header-inner no-logo">
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
  return articles.map(article => {
    const ordered = orderedMap.get(article.id) || null;
    return `
      <tr>
        <td>${escapeHtml(article.ean)}</td>
        <td>${escapeHtml(article.supplier_article_no)}</td>
        <td>${escapeHtml(article.name)}</td>
        <td>${escapeHtml(article.ve_stueck)}</td>
        <td>${ordered ? escapeHtml(ordered.menge_kartons) : ''}</td>
      </tr>
    `;
  }).join('');
}

function buildArticleTable(articles, orderedMap) {
  return `
    <table class="article-table">
      <thead>
        <tr>
          <th>EAN-Nr.</th>
          <th>Art.-Nr.</th>
          <th>Artikel-Bezeichnung</th>
          <th>VE</th>
          <th>Kartons</th>
        </tr>
      </thead>
      <tbody>
        ${buildArticleRows(articles, orderedMap)}
      </tbody>
    </table>
  `;
}

function buildMellerudHtml({ order, articles, orderedMap }) {
  return `
<!doctype html>
<html>
<head>
<style>
body {
  font-family: Arial;
}

.header-inner {
  display: flex;
  justify-content: center;
  align-items: center;
}

.header-title {
  font-size: 24px;
  color: #004a83;
  font-weight: bold;
}

.red-line {
  height: 2px;
  background: red;
  margin-top: 10px;
}

.article-table {
  width: 100%;
  border-collapse: collapse;
}

.article-table th {
  background: #004a83;
  color: white;
}

.article-table td, .article-table th {
  border: 1px solid #ccc;
  padding: 4px;
}

</style>
</head>
<body>

${buildHeader()}
${buildMetaBlock(order)}
${buildArticleTable(articles, orderedMap)}

</body>
</html>
`;
}

async function loadMellerudPdfData(orderId) {
  const orderResult = await db.query(
    `SELECT * FROM "order".order_orders WHERE id = $1`,
    [orderId]
  );

  const positionsResult = await db.query(
    `SELECT * FROM "order".order_order_positions WHERE order_id = $1`,
    [orderId]
  );

  const articlesResult = await db.query(
    `SELECT * FROM "order".order_supplier_articles WHERE aktiv = true`,
  );

  const orderedMap = new Map();

  for (const pos of positionsResult.rows) {
    orderedMap.set(pos.article_id, {
      menge_kartons: pos.menge_kartons
    });
  }

  return {
    order: orderResult.rows[0],
    articles: articlesResult.rows,
    orderedMap
  };
}

async function generateMellerudOrderPdf(orderId) {
  const data = await loadMellerudPdfData(orderId);

  const html = buildMellerudHtml(data);

  const browser = await puppeteer.launch({
    args: ['--no-sandbox']
  });

  const page = await browser.newPage();
  await page.setContent(html);

  const pdf = await page.pdf();

  await browser.close();

  return pdf;
}

module.exports = {
  generateMellerudOrderPdf,
};