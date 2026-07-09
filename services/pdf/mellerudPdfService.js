const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');
const db = require('../../db');

const PAGE_WIDTH = 841.89;
const PAGE_HEIGHT = 595.28;

const MELLERUD_FORM_ARTICLES = [
  { ean: '4004666005047', lan: '2001005047', kundenArtNr: '150000261', name: 'Schimmel Schutz 0,5l', ve: '6', ek: '5,04 EUR', uvp: '9,99 EUR' },
  { ean: '4004666004835', lan: '2001004835', kundenArtNr: '150000262', name: 'Schimmel Entferner 0,5l', ve: '6', ek: '4,27 EUR', uvp: '8,69 EUR' },
  { ean: '4004666009250', lan: '2001009250', kundenArtNr: '150000039', name: 'Schimmel Frei Haftgel 0,25l', ve: '6', ek: '3,46 EUR', uvp: '6,49 EUR' },
  { ean: '4004666004675', lan: '2001004675', kundenArtNr: '150000263', name: 'Schimmel Vernichter 0,5l', ve: '6', ek: '4,01 EUR', uvp: '8,79 EUR' },
  { ean: '4004666004750', lan: '2001004750', kundenArtNr: '150000264', name: 'Fugen Reiniger 0,5l', ve: '6', ek: '3,75 EUR', uvp: '7,69 EUR' },
  { ean: '4004666004712', lan: '2001004712', kundenArtNr: '150000265', name: 'Küchen Entfetter 0,5l', ve: '6', ek: '3,01 EUR', uvp: '6,69 EUR' },
  { ean: '4004666002275', lan: '2001002275', kundenArtNr: '131030554', name: 'Glaskeramik Kochfeld Reiniger 0,5l', ve: '6', ek: '4,38 EUR', uvp: '7,69 EUR' },
  { ean: '4004666001780', lan: '2001001780', kundenArtNr: '131030550', name: 'Edelstahl & Chrom Pflege 0,25l', ve: '6', ek: '4,22 EUR', uvp: '7,49 EUR' },
  { ean: '4004666005306', lan: '2001005306', kundenArtNr: '150000266', name: 'Backofen Reiniger 0,5l', ve: '6', ek: '4,50 EUR', uvp: '7,49 EUR' },
  { ean: '4004666008291', lan: '2049408291', kundenArtNr: '110082120', name: 'Silicon & Glaskeramik Kochfeld Schaber inkl. Ersatzklinge', ve: '10', ek: '2,84 EUR', uvp: '4,99 EUR' },
  { ean: '4004666008345', lan: '2049408345', kundenArtNr: '110082121', name: 'Silicon & Glaskeramik Kochfeld Schaber Ersatzklingen 5er Pack', ve: '10', ek: '1,98 EUR', uvp: '3,99 EUR' },
  { ean: '4004666001032', lan: '2001001032', kundenArtNr: '131030551', name: 'Kaffeemaschinen Entkalker 0,5l', ve: '6', ek: '2,68 EUR', uvp: '6,69 EUR' },
  { ean: '4004666001636', lan: '2001001636', kundenArtNr: '131030611', name: 'Wasch & Spülmaschinen Reiniger & Pflege 0,5l', ve: '6', ek: '3,21 EUR', uvp: '6,69 EUR' },
  { ean: '4004666009106', lan: '2003109106', kundenArtNr: '131030671', name: 'Rohr Frei Granulat 0,6kg', ve: '6', ek: '2,28 EUR', uvp: '4,99 EUR' },
  { ean: '4004666009151', lan: '2003109151', kundenArtNr: '131030559', name: 'Rohr Frei Aktivgel 1,0l', ve: '4', ek: '3,25 EUR', uvp: '5,49 EUR' },
  { ean: '4004666004910', lan: '2001004910', kundenArtNr: '150000267', name: 'Duschkabinen Reiniger 0,5l', ve: '6', ek: '3,36 EUR', uvp: '6,49 EUR' },
  { ean: '4004666005139', lan: '2001005139', kundenArtNr: '150000268', name: 'Bad & Sanitär Kraftreiniger 0,5l', ve: '6', ek: '4,13 EUR', uvp: '7,69 EUR' },
  { ean: '4004666000219', lan: '2001000219', kundenArtNr: '131030555', name: 'Kalk & Rost Löser 0,5l', ve: '6', ek: '2,76 EUR', uvp: '6,29 EUR' },
  { ean: '4004666002077', lan: '2001002077', kundenArtNr: '199710012', name: 'Spülkasten Reiniger 0,5l', ve: '6', ek: '4,55 EUR', uvp: '8,69 EUR' },
  { ean: '4004666004873', lan: '2001004873', kundenArtNr: '150000269', name: 'Braunstein Entferner 0,5l', ve: '6', ek: '5,00 EUR', uvp: '8,49 EUR' },
  { ean: '4004666000820', lan: '2001000820', kundenArtNr: '131030560', name: 'Urin & Kalkstein Entferner 1,0l', ve: '4', ek: '3,64 EUR', uvp: '7,69 EUR' },
  { ean: '4004666001773', lan: '2001001773', kundenArtNr: '156060127', name: 'Silicon Entferner 0,25l', ve: '6', ek: '5,17 EUR', uvp: '9,99 EUR' },
  { ean: '4004666001766', lan: '2001001766', kundenArtNr: '131030455', name: 'Aufkleber & Klebereste Entferner 0,25l', ve: '6', ek: '4,06 EUR', uvp: '7,69 EUR' },
  { ean: '4004666004637', lan: '2001004637', kundenArtNr: '150000270', name: 'Kamin & Ofenglas Reiniger 0,5l', ve: '6', ek: '4,76 EUR', uvp: '8,49 EUR' },
  { ean: '4004666005177', lan: '2001005177', kundenArtNr: '150000271', name: 'Nikotin Entferner 0,5l', ve: '6', ek: '5,02 EUR', uvp: '8,49 EUR' },
  { ean: '4004666005504', lan: '2001005504', kundenArtNr: '150000272', name: 'Neu: BBQ& Outdoorküchen Reiniger 460ml alt: Grill & BBQ Reiniger', ve: '6', ek: '4,74 EUR', uvp: '8,49 EUR' },
  { ean: '4004666005467', lan: '2001005467', kundenArtNr: '150000273', name: 'Neu: Grillrost Reiniger 0,5l alt: Fett & Verkrustungen Entferner', ve: '6', ek: '4,64 EUR', uvp: '8,49 EUR' },
  { ean: '4004666300074', lan: '2049408291', kundenArtNr: '150010042', name: 'Spezial Reinigungsschwamm braun', ve: '10', ek: '1,70 EUR', uvp: '3,49 EUR' },
  { ean: '4004666000165', lan: '2001000165', kundenArtNr: '131030558', name: 'Öl & Fettflecken Entferner 0,5l', ve: '6', ek: '5,81 EUR', uvp: '11,50 EUR' },
  { ean: '4004666004958', lan: '2001004958', kundenArtNr: '150000274', name: 'Rostflecken Entferner 0,5l', ve: '6', ek: '6,29 EUR', uvp: '10,99 EUR' },
  { ean: '4004666001476', lan: '2001001476', kundenArtNr: '131030458', name: 'Graffiti & PU Schaum Entferner 0,5l', ve: '6', ek: '7,50 EUR', uvp: '13,99 EUR' },
  { ean: '4004666001629', lan: '2001001629', kundenArtNr: '131040183', name: 'Edelstahl & Metall Reiniger 0,5l', ve: '6', ek: '3,97 EUR', uvp: '7,99 EUR' },
  { ean: '4004666004798', lan: '2001004798', kundenArtNr: '150000275', name: 'Grabstein Reiniger 0,5l', ve: '6', ek: '3,44 EUR', uvp: '7,69 EUR' },
  { ean: '4004666000110', lan: '2001000110', kundenArtNr: '131030467', name: 'Algen & Grünbelag Entferner 1,0l', ve: '4', ek: '3,84 EUR', uvp: '7,99 EUR' },
  { ean: '4004666000127', lan: '2001000127', kundenArtNr: '131030468', name: 'Algen & Grünbelag Entferner 2,5l', ve: '1', ek: '8,43 EUR', uvp: '15,99 EUR' },
  { ean: '4004666000301', lan: '2001000301', kundenArtNr: '131030531', name: 'Grundreiniger Intensiv 1,0l', ve: '4', ek: '4,06 EUR', uvp: '7,69 EUR' },
  { ean: '4004666005542', lan: '2001005542', kundenArtNr: '150000276', name: 'Staubfrei Reiniger & Pflege 0,5 l', ve: '6', ek: '3,43 EUR', uvp: '5,99 EUR' },
  { ean: '4004666005627', lan: '2001005627', kundenArtNr: '150000277', name: 'Glas & Spiegel Reiniger 0,5 l', ve: '6', ek: '2,85 EUR', uvp: '4,99 EUR' },
  { ean: '4004666005429', lan: '2001005429', kundenArtNr: '150000278', name: 'Neu: Kunstoff Reiniger 0,5 l alt: Kunststoff Oberflächen Reiniger', ve: '6', ek: '3,11 EUR', uvp: '6,49 EUR' },
  { ean: '4004666001544', lan: '2001001544', kundenArtNr: '2001001544', name: 'Kunststoff Fenster Reiniger 1,0l', ve: '4', ek: '4,29 EUR', uvp: '7,69 EUR' },
  { ean: '4004666003289', lan: '2003003289', kundenArtNr: '150000144', name: 'Teppich Spezialreiniger Aktivschaum 0,4 L', ve: '6', ek: '4,57 EUR', uvp: '7,99 EUR' },
  { ean: '4004666010409', lan: '2001010409', kundenArtNr: '131030547', name: 'Laminat & Vinyl Reiniger & Pflege 1,0l', ve: '4', ek: '4,03 EUR', uvp: '7,69 EUR' },
  { ean: '4004666001513', lan: '2001001513', kundenArtNr: '131030548', name: 'Parkett Reiniger & Pflege 1,0l', ve: '4', ek: '4,28 EUR', uvp: '8,69 EUR' },
  { ean: '4004666001490', lan: '2001001490', kundenArtNr: '131030549', name: 'Parkett & Holzboden Versiegelung 1,0l', ve: '4', ek: '6,09 EUR', uvp: '10,99 EUR' },
  { ean: '4004666000943', lan: '2001000943', kundenArtNr: '150000006', name: 'Fliesen & Feinsteinzeug Reiniger 1,0l', ve: '4', ek: '3,92 EUR', uvp: '7,69 EUR' },
  { ean: '4004666001803', lan: '2001001803', kundenArtNr: '131030470', name: 'Granitboden Seife 1,0l', ve: '4', ek: '4,06 EUR', uvp: '7,69 EUR' },
  { ean: '4004666000950', lan: '2001000950', kundenArtNr: '131030674', name: 'Marmor Reiniger 1,0l', ve: '4', ek: '4,24 EUR', uvp: '8,69 EUR' },
  { ean: '4004666000981', lan: '2001000981', kundenArtNr: '157060262', name: 'Zementschleier Entferner Säurefrei 1,0l', ve: '4', ek: '4,30 EUR', uvp: '8,69 EUR' },
  { ean: '4004666000004', lan: '2001000004', kundenArtNr: '157060251', name: 'Zementschleier Entferner Säurehaltig 1,0l', ve: '4', ek: '3,22 EUR', uvp: '7,69 EUR' },
  { ean: '4004666000059', lan: '2001000059', kundenArtNr: '131030530', name: 'Fliesen & Stein Grundreiniger säurehaltig 1,0l', ve: '4', ek: '3,73 EUR', uvp: '7,69 EUR' },
  { ean: '4004666002695', lan: '2001002695', kundenArtNr: '150000006', name: 'Stein & Platten Grundreiniger säurefrei 1,0l', ve: '4', ek: '4,06 EUR', uvp: '8,69 EUR' },
  { ean: '4004666001469', lan: '2001001469', kundenArtNr: '131030673', name: 'Stein & Platten Imprägnierung 1,0l', ve: '4', ek: '8,96 EUR', uvp: '17,50 EUR' },
  { ean: '4004666002824', lan: '2001002824', kundenArtNr: '157060293', name: 'Stein & Platten Versiegelung 0,5l', ve: '6', ek: '4,12 EUR', uvp: '8,69 EUR' },
  { ean: '4004666010614', lan: '2001010614', kundenArtNr: '150000072', name: 'Leder Reiniger & Pflege 0,25 l', ve: '6', ek: '6,21 EUR', uvp: '9,99 EUR' },
  { ean: '4004666002367', lan: '2001002367', kundenArtNr: '150000012', name: 'Oberflächen Grundreiniger 0,5 l', ve: '6', ek: '4,37 EUR', uvp: '7,49 EUR' },
  { ean: '4004666000936', lan: '2001000936', kundenArtNr: '150000001', name: 'Wand &Bodenfliesen Reiniger 1,0 l', ve: '4', ek: '3,71 EUR', uvp: '7,69 EUR' },
  { ean: '4004666009281', lan: '2001009281', kundenArtNr: '150000070', name: 'Schimmel Entferner 0,25 l', ve: '6', ek: '3,28 EUR', uvp: '6,49 EUR' },
];

function cleanText(value) {
  return String(value ?? '')
    .replace(/€/g, 'EUR')
    .replace(/[–—]/g, '-')
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function formatDateDe(value) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return cleanText(value).slice(0, 10);
  return date.toLocaleDateString('de-DE');
}

function normalizeKey(value) {
  return cleanText(value).toLowerCase();
}

function drawText(page, text, x, y, options = {}) {
  page.drawText(cleanText(text), {
    x,
    y,
    size: options.size || 8,
    font: options.font,
    color: options.color || rgb(0, 0, 0),
  });
}

function drawLine(page, x1, y1, x2, y2, options = {}) {
  page.drawLine({
    start: { x: x1, y: y1 },
    end: { x: x2, y: y2 },
    thickness: options.thickness || 0.5,
    color: options.color || rgb(0, 0, 0),
  });
}

function drawRect(page, x, y, width, height, options = {}) {
  page.drawRectangle({
    x,
    y,
    width,
    height,
    borderWidth: options.borderWidth ?? 0.5,
    borderColor: options.borderColor || rgb(0, 0, 0),
    color: options.color,
  });
}

function splitText(text, maxChars) {
  const words = cleanText(text).split(' ');
  const lines = [];
  let current = '';

  for (const word of words) {
    const next = current ? `${current} ${word}` : word;
    if (next.length > maxChars && current) {
      lines.push(current);
      current = word;
    } else {
      current = next;
    }
  }

  if (current) lines.push(current);
  return lines.slice(0, 2);
}

function getFilialAssetPath(filiale, filename) {
  const safeFiliale = cleanText(filiale).toLowerCase()
    .replace(/ü/g, 'ue')
    .replace(/ö/g, 'oe')
    .replace(/ä/g, 'ae')
    .replace(/ß/g, 'ss')
    .replace(/[^a-z0-9_-]/g, '');

  return path.join(__dirname, 'assets', 'filialen', safeFiliale, filename);
}

async function embedOptionalImage(pdfDoc, filiale, filename) {
  const filePath = getFilialAssetPath(filiale, filename);

  if (!fs.existsSync(filePath)) {
    return null;
  }

  const bytes = fs.readFileSync(filePath);

  if (filename.toLowerCase().endsWith('.jpg') || filename.toLowerCase().endsWith('.jpeg')) {
    return pdfDoc.embedJpg(bytes);
  }

  return pdfDoc.embedPng(bytes);
}

async function loadOrderData(orderId) {
  const orderRes = await db.query(
    `
    SELECT
      o.id,
      o.filiale,
      o.ordered_by_name,
      o.bestelldatum,
      o.status,
      o.gesamtsumme_netto,
      o.firma_snapshot,
      o.kunden_nr_snapshot,
      o.strasse_snapshot,
      o.ort_snapshot,
      o.auftrags_nr_snapshot,
      o.gespraechspartner_snapshot,
      o.telefon_snapshot,
      o.email_snapshot,
      s.name AS supplier_name,
      s.code AS supplier_code
    FROM "order".order_orders o
    INNER JOIN "order".order_suppliers s
      ON s.id = o.supplier_id
    WHERE o.id = $1
    LIMIT 1
    `,
    [orderId]
  );

  if (orderRes.rows.length === 0) {
    throw new Error(`Bestellung nicht gefunden: ${orderId}`);
  }

  const positionsRes = await db.query(
    `
    SELECT
      supplier_article_no_snapshot,
      ean_snapshot,
      name_snapshot,
      ve_stueck_snapshot,
      ek_pro_karton_snapshot,
      menge_kartons,
      positionssumme_netto,
      sort_index_snapshot
    FROM "order".order_order_positions
    WHERE order_id = $1
    ORDER BY sort_index_snapshot ASC, name_snapshot ASC
    `,
    [orderId]
  );

  return {
    order: orderRes.rows[0],
    positions: positionsRes.rows,
  };
}

function buildQuantityMaps(positions) {
  const byEan = new Map();
  const byLan = new Map();

  for (const pos of positions) {
    const menge = Number(pos.menge_kartons);
    if (!Number.isFinite(menge) || menge <= 0) continue;

    const ean = normalizeKey(pos.ean_snapshot);
    const lan = normalizeKey(pos.supplier_article_no_snapshot);

    if (ean) byEan.set(ean, (byEan.get(ean) || 0) + menge);
    if (lan) byLan.set(lan, (byLan.get(lan) || 0) + menge);
  }

  return { byEan, byLan };
}

function buildFormRows(positions) {
  const { byEan, byLan } = buildQuantityMaps(positions);
  const matchedKeys = new Set();

  const rows = MELLERUD_FORM_ARTICLES.map((article) => {
    const eanKey = normalizeKey(article.ean);
    const lanKey = normalizeKey(article.lan);

    const menge = (byEan.get(eanKey) || byLan.get(lanKey) || '');

    if (menge) {
      matchedKeys.add(eanKey);
      matchedKeys.add(lanKey);
    }

    return {
      ...article,
      menge,
    };
  });

  for (const pos of positions) {
    const eanKey = normalizeKey(pos.ean_snapshot);
    const lanKey = normalizeKey(pos.supplier_article_no_snapshot);

    if ((eanKey && matchedKeys.has(eanKey)) || (lanKey && matchedKeys.has(lanKey))) {
      continue;
    }

    const menge = Number(pos.menge_kartons);
    if (!Number.isFinite(menge) || menge <= 0) continue;

    rows.push({
      ean: cleanText(pos.ean_snapshot) || '-',
      lan: cleanText(pos.supplier_article_no_snapshot) || '-',
      kundenArtNr: '-',
      name: cleanText(pos.name_snapshot) || '-',
      ve: cleanText(pos.ve_stueck_snapshot) || '-',
      ek: '',
      uvp: '',
      menge,
    });
  }

  return rows;
}

function drawFullHeader(page, fonts, order, totalPages) {
  const { bold, regular } = fonts;

  drawText(page, 'Sonderpreis Baumarkt Bestellformular', 28, 560, {
    font: bold,
    size: 15,
  });

  drawText(page, 'MELLERUD Bestellung', 28, 540, {
    font: bold,
    size: 12,
  });

  drawText(page, `Seite 1 / ${totalPages}`, 735, 560, {
    font: regular,
    size: 8,
  });

  drawRect(page, 28, 455, 370, 72, { borderWidth: 0.7 });

  drawText(page, 'Filiale:', 38, 511, { font: bold, size: 8 });
  drawText(page, order.filiale || '', 125, 511, { font: regular, size: 8 });

  drawText(page, 'Lieferant:', 38, 498, { font: bold, size: 8 });
  drawText(page, order.supplier_name || '', 125, 498, { font: regular, size: 8 });

  drawText(page, 'Bestelldatum:', 38, 485, { font: bold, size: 8 });
  drawText(page, formatDateDe(order.bestelldatum), 125, 485, { font: regular, size: 8 });

  drawText(page, 'Kunden-Nr.:', 38, 472, { font: bold, size: 8 });
  drawText(page, order.kunden_nr_snapshot || '', 125, 472, { font: regular, size: 8 });

  drawRect(page, 408, 455, 405, 72, { borderWidth: 0.7 });

  drawText(page, 'Firma:', 418, 511, { font: bold, size: 8 });
  drawText(page, order.firma_snapshot || '', 505, 511, { font: regular, size: 8 });

  drawText(page, 'Strasse:', 418, 498, { font: bold, size: 8 });
  drawText(page, order.strasse_snapshot || '', 505, 498, { font: regular, size: 8 });

  drawText(page, 'Ort:', 418, 485, { font: bold, size: 8 });
  drawText(page, order.ort_snapshot || '', 505, 485, { font: regular, size: 8 });

  drawText(page, 'Ansprechpartner:', 418, 472, { font: bold, size: 8 });
  drawText(page, order.gespraechspartner_snapshot || order.ordered_by_name || '', 505, 472, {
    font: regular,
    size: 8,
  });

  drawText(page, 'Telefon:', 418, 459, { font: bold, size: 8 });
  drawText(page, order.telefon_snapshot || '', 505, 459, { font: regular, size: 8 });

  drawText(page, 'E-Mail:', 610, 459, { font: bold, size: 8 });
  drawText(page, order.email_snapshot || '', 655, 459, { font: regular, size: 8 });
}

function drawCompactHeader(page, fonts, order, pageNumber, totalPages) {
  const { bold, regular } = fonts;

  drawText(page, 'MELLERUD Bestellung', 28, 560, {
    font: bold,
    size: 12,
  });

  drawText(page, `Filiale: ${order.filiale || ''}`, 610, 560, {
    font: bold,
    size: 10,
  });

  drawText(page, `Bestelldatum: ${formatDateDe(order.bestelldatum)}`, 610, 544, {
    font: regular,
    size: 8,
  });

  drawText(page, `Seite ${pageNumber} / ${totalPages}`, 610, 530, {
    font: regular,
    size: 8,
  });

  drawLine(page, 28, 520, 813, 520, { thickness: 0.5, color: rgb(0.2, 0.2, 0.2) });
}

function drawTableHeader(page, fonts, x, y, columns) {
  const { bold } = fonts;

  drawRect(page, x, y - 18, columns.reduce((sum, col) => sum + col.width, 0), 18, {
    borderWidth: 0.7,
    color: rgb(0.9, 0.9, 0.9),
  });

  let currentX = x;

  for (const col of columns) {
    drawRect(page, currentX, y - 18, col.width, 18, { borderWidth: 0.4 });
    drawText(page, col.label, currentX + 3, y - 12, {
      font: bold,
      size: col.size || 7,
    });
    currentX += col.width;
  }
}

function drawArticleRow(page, fonts, row, x, y, columns, rowHeight, isEven) {
  const { regular, bold } = fonts;
  const backgroundColor = row.menge
    ? rgb(1, 0.95, 0.78)
    : isEven
      ? rgb(1, 1, 1)
      : rgb(0.965, 0.965, 0.965);

  let currentX = x;

  drawRect(page, x, y - rowHeight, columns.reduce((sum, col) => sum + col.width, 0), rowHeight, {
    borderWidth: 0,
    color: backgroundColor,
  });

  const values = [
    row.ean,
    row.lan,
    row.kundenArtNr,
    row.name,
    row.ve,
    row.ek,
    row.uvp,
    row.menge ? String(row.menge) : '',
  ];

  values.forEach((value, index) => {
    const col = columns[index];

    drawRect(page, currentX, y - rowHeight, col.width, rowHeight, {
      borderWidth: 0.25,
      borderColor: rgb(0.45, 0.45, 0.45),
    });

    if (index === 3) {
      const lines = splitText(value, 48);
      drawText(page, lines[0] || '', currentX + 3, y - 8, {
        font: regular,
        size: 6.6,
      });

      if (lines[1]) {
        drawText(page, lines[1], currentX + 3, y - 16, {
          font: regular,
          size: 6.1,
          color: rgb(0.25, 0.25, 0.25),
        });
      }
    } else if (index === 7) {
      drawText(page, value, currentX + 24, y - 12, {
        font: bold,
        size: 10,
      });
    } else {
      drawText(page, value, currentX + 3, y - 12, {
        font: regular,
        size: 6.4,
      });
    }

    currentX += col.width;
  });
}

async function drawFinalSignatureBlock(pdfDoc, page, fonts, order) {
  const { regular, bold } = fonts;

  const blockTop = 126;
  const lineY = 88;
  const labelY = 72;

  drawLine(page, 28, blockTop, 813, blockTop, { thickness: 0.6, color: rgb(0.2, 0.2, 0.2) });

  drawText(page, 'Bestelldatum', 82, labelY, { font: regular, size: 7 });
  drawText(page, formatDateDe(order.bestelldatum), 78, lineY + 10, { font: bold, size: 9 });
  drawLine(page, 60, lineY, 180, lineY, { thickness: 0.6 });

  drawText(page, 'Unterschrift Besteller', 305, labelY, { font: regular, size: 7 });
  drawLine(page, 250, lineY, 430, lineY, { thickness: 0.6 });

  drawText(page, 'Firmenstempel', 632, labelY, { font: regular, size: 7 });
  drawRect(page, 560, 68, 150, 58, {
    borderWidth: 0.7,
    borderColor: rgb(0, 0, 0),
  });

  const unterschrift = await embedOptionalImage(pdfDoc, order.filiale, 'unterschrift.png');
  const stempel = await embedOptionalImage(pdfDoc, order.filiale, 'stempel.png');

  if (unterschrift) {
    page.drawImage(unterschrift, {
      x: 285,
      y: 91,
      width: 105,
      height: 28,
    });
  }

  if (stempel) {
    page.drawImage(stempel, {
      x: 575,
      y: 76,
      width: 120,
      height: 42,
    });
  }

  drawLine(page, 28, 44, 813, 44, { thickness: 0.5, color: rgb(0.2, 0.2, 0.2) });
  drawText(page, 'Die Ware bleibt bis zur vollstaendigen Bezahlung unser Eigentum.', 325, 31, {
    font: regular,
    size: 6,
    color: rgb(0.2, 0.2, 0.2),
  });
}

function paginateRows(rows) {
  const firstPageRows = 29;
  const normalPageRows = 35;
  const finalPageRows = 29;

  if (rows.length <= firstPageRows) {
    return [rows];
  }

  const pages = [];
  pages.push(rows.slice(0, firstPageRows));

  let remaining = rows.slice(firstPageRows);

  while (remaining.length > finalPageRows) {
    pages.push(remaining.slice(0, normalPageRows));
    remaining = remaining.slice(normalPageRows);
  }

  pages.push(remaining);

  return pages;
}

async function generateMellerudOrderPdf(orderId) {
  const { order, positions } = await loadOrderData(orderId);
  const rows = buildFormRows(positions);
  const pageRows = paginateRows(rows);

  const pdfDoc = await PDFDocument.create();

  const fonts = {
    regular: await pdfDoc.embedFont(StandardFonts.Helvetica),
    bold: await pdfDoc.embedFont(StandardFonts.HelveticaBold),
  };

  const columns = [
    { label: 'EAN', width: 84 },
    { label: 'LAN', width: 72 },
    { label: 'Kunden Art.-Nr.', width: 76 },
    { label: 'Artikelbezeichnung', width: 275 },
    { label: 'VE', width: 28 },
    { label: 'netto EK/Stueck', width: 68 },
    { label: 'empf. UVP/Stueck', width: 74 },
    { label: 'Bestellmenge VE', width: 66 },
  ];

  const tableX = 28;
  const rowHeight = 12.5;

  for (let pageIndex = 0; pageIndex < pageRows.length; pageIndex += 1) {
    const page = pdfDoc.addPage([PAGE_WIDTH, PAGE_HEIGHT]);
    const isFirstPage = pageIndex === 0;
    const isLastPage = pageIndex === pageRows.length - 1;

    const tableHeaderY = isFirstPage ? 455 : 500;

    if (isFirstPage) {
      drawFullHeader(page, fonts, order, pageRows.length);
    } else {
      drawCompactHeader(page, fonts, order, pageIndex + 1, pageRows.length);
    }

    drawTableHeader(page, fonts, tableX, tableHeaderY, columns);

    let y = tableHeaderY - 18;

    pageRows[pageIndex].forEach((row, rowIndex) => {
      y -= rowHeight;
      drawArticleRow(page, fonts, row, tableX, y + rowHeight, columns, rowHeight, rowIndex % 2 === 0);
    });

    if (isLastPage) {
      await drawFinalSignatureBlock(pdfDoc, page, fonts, order);
    }
  }

  const pdfBytes = await pdfDoc.save();
  return Buffer.from(pdfBytes);
}

module.exports = {
  generateMellerudOrderPdf,
};