const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');

const PAGE_WIDTH = 841.89;
const PAGE_HEIGHT = 595.28;

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

function formatMoneyEur(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '';

  return `${n.toFixed(2).replace('.', ',')} EUR`;
}

function calcEkEinzel(article) {
  const ekEinzel = Number(article.ek_einzel);
  if (Number.isFinite(ekEinzel)) return ekEinzel;

  const ekProKarton = Number(article.ek_pro_karton);
  const veStueck = Number(article.ve_stueck);

  if (Number.isFinite(ekProKarton) && Number.isFinite(veStueck) && veStueck > 0) {
    return ekProKarton / veStueck;
  }

  return null;
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

function clipTextToWidth(text, font, size, maxWidth) {
  const clean = cleanText(text);
  if (font.widthOfTextAtSize(clean, size) <= maxWidth) return clean;
  let clipped = clean;
  while (clipped.length > 1 && font.widthOfTextAtSize(`${clipped}…`, size) > maxWidth) {
    clipped = clipped.slice(0, -1);
  }
  return `${clipped}…`;
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
  const db = require('../../db');
  const orderRes = await db.query(
    `
    SELECT
      o.id,
      o.supplier_id,
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

  const order = orderRes.rows[0];

  const positionsRes = await db.query(
    `
    SELECT
      supplier_article_no_snapshot,
      kunden_art_nr_snapshot,
      ean_snapshot,
      name_snapshot,
      ve_stueck_snapshot,
      ek_pro_karton_snapshot,
      ek_einzel_snapshot,
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
    order,
    positions: positionsRes.rows,
  };
}

function buildFormRows(positions) {
  return positions.map((position) => {
    const ekEinzel = calcEkEinzel({
      ek_einzel: position.ek_einzel_snapshot,
      ek_pro_karton: position.ek_pro_karton_snapshot,
      ve_stueck: position.ve_stueck_snapshot,
    });
    return {
      ean: cleanText(position.ean_snapshot) || '-',
      supplierArticleNo: cleanText(position.supplier_article_no_snapshot) || '-',
      name: cleanText(position.name_snapshot) || '-',
      ve: cleanText(position.ve_stueck_snapshot) || '-',
      ek: ekEinzel !== null ? formatMoneyEur(ekEinzel) : '',
      menge: Number(position.menge_kartons) || '',
    };
  });
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
    row.supplierArticleNo,
    row.name,
    row.ve,
    row.ek,
    row.menge ? String(row.menge) : '',
  ];

  values.forEach((value, index) => {
    const col = columns[index];

    drawRect(page, currentX, y - rowHeight, col.width, rowHeight, {
      borderWidth: 0.25,
      borderColor: rgb(0.45, 0.45, 0.45),
    });

    if (index === 2) {
      drawText(page, clipTextToWidth(value, regular, 6.6, col.width - 6), currentX + 3, y - 12, {
        font: regular,
        size: 6.6,
      });
    } else if (index === 5) {
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
  drawText(page, 'Diese Bestellung wurde mit dem digitalen Bestellsystem der Neufeld Baumarkt GmbH erstellt und automatisch versendet.', 325, 31, {
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
    { label: 'EAN', width: 110 },
    { label: 'Mellerud Art.-Nr.', width: 105 },
    { label: 'Artikelbezeichnung', width: 350 },
    { label: 'VE/Stueck', width: 58 },
    { label: 'netto EK/Stueck', width: 82 },
    { label: 'Bestellmenge VE', width: 80 },
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
  buildFormRows,
  generateMellerudOrderPdf,
};
