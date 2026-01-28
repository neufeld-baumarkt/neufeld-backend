// routes/reklamationen.js – V1.1.2+ (SodaFixx-Regeln + XX Tracking-Unique Handling)
// - lfd_nr Vergabe: pro Filiale + Jahr (Jahr aus Anlegedatum `datum`, nicht Serverjahr)
// - Counter initialisiert/absichert sich automatisch aus MAX(lfd_nr) in der DB
// - Transaktionssicher (SELECT ... FOR UPDATE)
// - Edit-Fall B: bestehende lfd_nr bleiben, neue Positionen bekommen neue
// - Notiz-Feld (notiz) via PATCH, inkl. notiz_von + notiz_am automatisch
// - Neu: SodaFixx-Regeln (Regel 1/3) + Duplicate Tracking (Regel 2 via DB Unique Index)

const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// Rollen mit globaler Sicht (für GET-Anzeige)
const rollenMitGlobalzugriff = ['Admin', 'Supervisor', 'Manager-1', 'Geschäftsführer'];

// Rollen mit Bearbeitungs- und Löschrecht – NUR diese beiden!
const rollenMitBearbeitungsrecht = ['Admin', 'Supervisor'];

function getCurrentYear() {
  return new Date().getFullYear();
}

function getYearFromDateValue(datum) {
  // datum kann sein: 'YYYY-MM-DD' (String) oder Date oder null
  if (!datum) return null;

  if (datum instanceof Date && !Number.isNaN(datum.getTime())) {
    return datum.getFullYear();
  }

  if (typeof datum === 'string') {
    const m = datum.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!m) return null;
    const year = Number(m[1]);
    return Number.isFinite(year) ? year : null;
  }

  return null;
}

function normText(v) {
  return (v ?? '').toString().trim();
}

function isSodaFixx(lieferant) {
  return normText(lieferant).toLowerCase() === 'sodafixx';
}

function normalizeTrackingId(trackingId) {
  const t = normText(trackingId);
  return t.length > 0 ? t : null;
}

function parseCountLike(value) {
  // akzeptiert "18", "18.0", "18,0" -> Number
  const raw = normText(value);
  if (!raw) return NaN;
  const normalized = raw.replace(',', '.');
  const n = Number(normalized);
  return Number.isFinite(n) ? n : NaN;
}

function validateSodaFixxCartonMax18(positionen) {
  const list = Array.isArray(positionen) ? positionen : [];
  let sum = 0;

  for (let i = 0; i < list.length; i++) {
    const qty = parseCountLike(list[i]?.rekla_menge);

    if (!Number.isFinite(qty) || qty < 0) {
      return {
        ok: false,
        message: `SodaFixx: Ungültige Reklamationsmenge in Position ${i + 1}. Erwartet Zahl (z. B. 1..18).`,
      };
    }

    // Zylinder sind Stückzahlen -> ganzzahlig
    if (!Number.isInteger(qty)) {
      return {
        ok: false,
        message: `SodaFixx: Reklamationsmenge in Position ${i + 1} muss eine ganze Zahl sein.`,
      };
    }

    sum += qty;

    if (sum > 18) {
      return {
        ok: false,
        message: 'SodaFixx: Maximal 18 Zylinder pro Reklamation/Karton erlaubt (Summe aller Positionen).',
      };
    }
  }

  return { ok: true };
}

/**
 * Vergibt lfd_nr blockweise, transaktionssicher (Row-Lock) pro Filiale + Jahr.
 * - Stellt sicher, dass der Counter mindestens MAX(lfd_nr) der DB ist.
 * - Liefert Startwert (inkl.) für `count` neue Nummern.
 */
async function allocateLfdNrBlock(client, filiale, jahr, count) {
  if (!count || count <= 0) return null;

  // 1) Counter-Zeile locken/holen
  await client.query(
    `
    INSERT INTO lfd_nr_counter (filiale, jahr, current_value)
    VALUES ($1, $2, 0)
    ON CONFLICT (filiale, jahr) DO NOTHING;
    `,
    [filiale, jahr]
  );

  const lockRes = await client.query(
    `
    SELECT current_value
    FROM lfd_nr_counter
    WHERE filiale = $1 AND jahr = $2
    FOR UPDATE;
    `,
    [filiale, jahr]
  );

  const currentVal = Number(lockRes.rows[0]?.current_value ?? 0);

  // 2) MAX(lfd_nr) aus DB prüfen (Sicherheitsnetz)
  const maxRes = await client.query(
    `
    SELECT COALESCE(MAX(lfd_nr), 0) AS max_lfd
    FROM reklamation_positionen
    WHERE lfd_nr IS NOT NULL
      AND reklamation_id IN (
        SELECT id FROM reklamationen WHERE filiale = $1
          AND EXTRACT(YEAR FROM datum) = $2
      );
    `,
    [filiale, jahr]
  );

  const maxLfd = Number(maxRes.rows[0]?.max_lfd ?? 0);

  const base = Math.max(currentVal, maxLfd);

  // 3) Counter erhöhen
  const newVal = base + count;

  await client.query(
    `
    UPDATE lfd_nr_counter
    SET current_value = $3
    WHERE filiale = $1 AND jahr = $2;
    `,
    [filiale, jahr, newVal]
  );

  // Startwert für Vergabe
  return base + 1;
}

/**
 * GET /api/reklamationen – Liste nach Rolle/Filiale
 * Liefert zusätzliche Felder:
 * - min_lfd_nr: kleinste laufende Nummer je Reklamation
 * - position_count: Anzahl Positionen je Reklamation
 */
router.get('/', verifyToken(), async (req, res) => {
  const { role, filiale } = req.user;

  try {
    const global = rollenMitGlobalzugriff.includes(role);

    const query = `
      SELECT
        r.*,
        CASE WHEN r.notiz IS NOT NULL AND BTRIM(r.notiz) <> '' THEN true ELSE false END AS has_notiz,
        MIN(p.lfd_nr) AS min_lfd_nr,
        COUNT(p.id) AS position_count
      FROM reklamationen r
      LEFT JOIN reklamation_positionen p ON p.reklamation_id = r.id
      ${global ? '' : 'WHERE r.filiale = $1'}
      GROUP BY r.id
      ORDER BY r.datum DESC;
    `;

    const params = global ? [] : [filiale];
    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (error) {
    console.error('Fehler beim Abrufen der Reklamationen:', error);
    res.status(500).json({ message: 'Serverfehler beim Abrufen' });
  }
});

/**
 * GET /api/reklamationen/:id – Detail (Reklamation + Positionen)
 */
router.get('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const { role, filiale } = req.user;

  try {
    const global = rollenMitGlobalzugriff.includes(role);

    const reklaResult = await pool.query(
      `
      SELECT *
      FROM reklamationen
      WHERE id = $1
      ${global ? '' : 'AND filiale = $2'}
      `,
      global ? [id] : [id, filiale]
    );

    if (reklaResult.rows.length === 0) {
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    const reklamation = reklaResult.rows[0];

    const positionenResult = await pool.query(
      `
      SELECT *
      FROM reklamation_positionen
      WHERE reklamation_id = $1
      ORDER BY lfd_nr NULLS LAST, pos_id;
      `,
      [id]
    );

    res.json({
      reklamation,
      positionen: positionenResult.rows,
    });
  } catch (error) {
    console.error('Fehler beim Abrufen der Detailreklamation:', error);
    res.status(500).json({ message: 'Serverfehler beim Abrufen' });
  }
});

/**
 * POST /api/reklamationen
 * Neu anlegen (Reklamation + Positionen)
 */
router.post('/', verifyToken(), async (req, res) => {
  const user = req.user;
  const data = req.body;

  if (user.role === 'Filiale' && data.filiale && data.filiale !== user.filiale) {
    return res.status(403).json({ message: 'Nur eigene Filiale anlegbar' });
  }

  // --- SodaFixx Sonderregeln (Backend-hart) ---
  // Regel 1: SodaFixx => versand immer true + tracking_id Pflicht
  // Regel 3: SodaFixx => 1 Reklamation = 1 Karton = max 18 Zylinder (Summe aller Positionen)
  const supplierIncoming = data?.lieferant;
  const sodaFixx = isSodaFixx(supplierIncoming);

  if (sodaFixx) {
    data.versand = true;

    data.tracking_id = normalizeTrackingId(data.tracking_id);
    if (!data.tracking_id) {
      return res.status(400).json({
        message: 'SodaFixx: Tracking-ID ist Pflicht (GLS Rücksendung).',
      });
    }

    const v = validateSodaFixxCartonMax18(data.positionen);
    if (!v.ok) {
      return res.status(400).json({ message: v.message });
    }
  } else {
    // Non-SodaFixx: tracking_id sauber trimmen (optional), aber nicht erzwingen
    data.tracking_id = normalizeTrackingId(data.tracking_id);
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const filialeFinal = (data.filiale || user.filiale || '').toString();

    const reklaQuery = `
      INSERT INTO reklamationen (
        datum, letzte_aenderung, art, rekla_nr, lieferant, filiale, status,
        ls_nummer_grund, versand, tracking_id
      )
      VALUES (
        $1, CURRENT_DATE, $2, $3, $4, $5, $6,
        $7, $8, $9
      )
      RETURNING id, datum;
    `;

    const reklaValues = [
      data.datum || null,
      data.art || null,
      data.rekla_nr || null,
      data.lieferant || null,
      filialeFinal || null,
      data.status || 'Angelegt',
      data.ls_nummer_grund || null,
      data.versand || false,
      data.tracking_id || null,
    ];

    const reklaResult = await client.query(reklaQuery, reklaValues);
    const reklamationId = reklaResult.rows[0].id;

    const storedDatum = reklaResult.rows[0].datum;
    const jahr = getYearFromDateValue(storedDatum) || getYearFromDateValue(data.datum) || getCurrentYear();

    const positionen = Array.isArray(data.positionen) ? data.positionen : [];

    // lfd_nr Vergabe nur, wenn Positionen vorhanden
    if (positionen.length > 0) {
      const startLfd = await allocateLfdNrBlock(client, filialeFinal, jahr, positionen.length);

      const posQuery = `
        INSERT INTO reklamation_positionen (
          reklamation_id,
          artikelnummer,
          ean,
          bestell_menge,
          bestell_einheit,
          rekla_menge,
          rekla_einheit,
          lfd_nr
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8);
      `;

      for (let i = 0; i < positionen.length; i++) {
        const pos = positionen[i] || {};
        const lfdNrToUse = startLfd + i;

        const posValues = [
          reklamationId,
          pos.artikelnummer || null,
          pos.ean || null,
          pos.bestell_menge || null,
          pos.bestell_einheit || null,
          pos.rekla_menge || null,
          pos.rekla_einheit || null,
          lfdNrToUse,
        ];

        await client.query(posQuery, posValues);
      }
    }

    await client.query('COMMIT');

    console.log(`Reklamation angelegt – ID: ${reklamationId} von ${user.name} (${user.role})`);
    res.status(201).json({ message: 'Reklamation erfolgreich angelegt', id: reklamationId });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Anlegen:', err);

    // Unique Tracking-ID (Regel 2) – DB wirft 23505
    if (err && err.code === '23505') {
      return res.status(409).json({
        message: 'Tracking-ID bereits vorhanden (muss global eindeutig sein).',
      });
    }

    res.status(500).json({ message: 'Serverfehler beim Speichern' });
  } finally {
    client.release();
  }
});

/**
 * PUT /api/reklamationen/:id
 * Komplett bearbeiten (Reklamation + Positionen ersetzen)
 * Nur Admin/Supervisor
 *
 * Fall B: lfd_nr bleibt stabil.
 */
router.put('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;
  const data = req.body;

  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({
      message: 'Zugriff verweigert: Nur Admin oder Supervisor dürfen bearbeiten',
    });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // --- SodaFixx Sonderregeln (Backend-hart) ---
    // Regel 1: SodaFixx => versand immer true + tracking_id Pflicht
    // Regel 3: SodaFixx => 1 Reklamation = 1 Karton = max 18 Zylinder (Summe aller Positionen)
    const existingReklaRes = await client.query(
      'SELECT lieferant, tracking_id FROM reklamationen WHERE id = $1',
      [id]
    );

    if (existingReklaRes.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    const existingLieferant = existingReklaRes.rows[0].lieferant;
    const existingTracking = existingReklaRes.rows[0].tracking_id;

    const supplierFinal =
      normText(data?.lieferant).length > 0 ? data.lieferant : existingLieferant;

    const sodaFixx = isSodaFixx(supplierFinal);

    let versandFinal = data.versand ?? false;
    let trackingFinal = normalizeTrackingId(
      data.tracking_id !== undefined ? data.tracking_id : existingTracking
    );

    if (sodaFixx) {
      versandFinal = true;

      if (!trackingFinal) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          message: 'SodaFixx: Tracking-ID ist Pflicht (GLS Rücksendung).',
        });
      }

      const v = validateSodaFixxCartonMax18(data.positionen);
      if (!v.ok) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: v.message });
      }
    }

    const updateQuery = `
      UPDATE reklamationen SET
        datum = $1,
        letzte_aenderung = CURRENT_DATE,
        art = $2,
        rekla_nr = $3,
        lieferant = $4,
        filiale = $5,
        status = $6,
        ls_nummer_grund = $7,
        versand = $8,
        tracking_id = $9
      WHERE id = $10
      RETURNING filiale, datum;
    `;

    const updateValues = [
      data.datum || null,
      data.art || null,
      data.rekla_nr || null,
      supplierFinal || null,
      data.filiale || null,
      data.status || null,
      data.ls_nummer_grund || null,
      versandFinal,
      trackingFinal,
      id,
    ];

    const result = await client.query(updateQuery, updateValues);

    if (result.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    const filialeFinal = (result.rows[0].filiale || '').toString();

    const storedDatum = result.rows[0].datum;
    const jahr =
      getYearFromDateValue(storedDatum) ||
      getYearFromDateValue(data.datum) ||
      getCurrentYear();

    const existingLfdRes = await client.query(
      `
      SELECT lfd_nr
      FROM reklamation_positionen
      WHERE reklamation_id = $1 AND lfd_nr IS NOT NULL;
      `,
      [id]
    );
    const existingLfdSet = new Set(
      existingLfdRes.rows.map((r) => Number(r.lfd_nr)).filter(Number.isFinite)
    );

    await client.query('DELETE FROM reklamation_positionen WHERE reklamation_id = $1', [id]);

    const positionen = Array.isArray(data.positionen) ? data.positionen : [];
    if (positionen.length > 0) {
      let newNeededCount = 0;

      for (const pos of positionen) {
        const incoming =
          pos && pos.lfd_nr !== undefined && pos.lfd_nr !== null ? Number(pos.lfd_nr) : null;

        if (!(Number.isFinite(incoming) && existingLfdSet.has(incoming))) {
          newNeededCount++;
        }
      }

      const startLfd = await allocateLfdNrBlock(client, filialeFinal, jahr, newNeededCount);

      let allocCursor = startLfd;

      const posQuery = `
        INSERT INTO reklamation_positionen (
          reklamation_id,
          artikelnummer,
          ean,
          bestell_menge,
          bestell_einheit,
          rekla_menge,
          rekla_einheit,
          lfd_nr
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8);
      `;

      for (const pos of positionen) {
        const incoming =
          pos && pos.lfd_nr !== undefined && pos.lfd_nr !== null ? Number(pos.lfd_nr) : null;

        let lfdNrToUse = null;

        if (Number.isFinite(incoming) && existingLfdSet.has(incoming)) {
          lfdNrToUse = incoming;
        } else {
          lfdNrToUse = allocCursor;
          allocCursor++;
        }

        const posValues = [
          id,
          pos.artikelnummer || null,
          pos.ean || null,
          pos.bestell_menge || null,
          pos.bestell_einheit || null,
          pos.rekla_menge || null,
          pos.rekla_einheit || null,
          lfdNrToUse,
        ];

        await client.query(posQuery, posValues);
      }
    }

    await client.query('COMMIT');

    console.log(`Reklamation vollständig bearbeitet – ID: ${id} von ${user.name} (${user.role})`);
    res.json({ message: 'Reklamation erfolgreich aktualisiert' });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Bearbeiten:', err);

    // Unique Tracking-ID (Regel 2) – DB wirft 23505
    if (err && err.code === '23505') {
      return res.status(409).json({
        message: 'Tracking-ID bereits vorhanden (muss global eindeutig sein).',
      });
    }

    res.status(500).json({ message: 'Serverfehler beim Aktualisieren' });
  } finally {
    client.release();
  }
});

/**
 * PATCH /api/reklamationen/:id
 * Teil-Update: status, versand, tracking_id, ls_nummer_grund, notiz
 * Nur Admin/Supervisor
 */
router.patch('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;
  const updates = req.body || {};

  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({
      message: 'Zugriff verweigert: Nur Admin oder Supervisor dürfen bearbeiten',
    });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const checkResult = await client.query('SELECT id, lieferant, tracking_id, versand FROM reklamationen WHERE id = $1', [id]);
    if (checkResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    const existing = checkResult.rows[0];
    const sodaFixx = isSodaFixx(existing?.lieferant);
    const existingTrackingNorm = normalizeTrackingId(existing?.tracking_id);

    // SodaFixx: tracking_id darf nie leer sein (Regel 1)
    if (sodaFixx && updates.tracking_id === undefined && !existingTrackingNorm) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        message: 'SodaFixx: Tracking-ID ist Pflicht (bitte zuerst Tracking-ID setzen).',
      });
    }

    // SodaFixx: versand ist immer true (Regel 1)
    if (sodaFixx && updates.versand !== undefined) {
      updates.versand = true;
    }

    // Normalize tracking id (trim) und SodaFixx-Pflicht prüfen
    if (updates.tracking_id !== undefined) {
      const norm = normalizeTrackingId(updates.tracking_id);

      if (sodaFixx && !norm) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          message: 'SodaFixx: Tracking-ID ist Pflicht (GLS Rücksendung).',
        });
      }

      updates.tracking_id = norm;
    }

    const allowedFields = ['status', 'versand', 'tracking_id', 'ls_nummer_grund', 'notiz'];
    const setClauses = [];
    const values = [];
    let paramIndex = 1;

    let touchedNote = false;

    for (const field of allowedFields) {
      if (updates[field] !== undefined) {
        if (field === 'notiz') {
          touchedNote = true;
          const trimmed = normText(updates.notiz);
          const finalNote = trimmed.length > 0 ? trimmed : null;

          setClauses.push(`notiz = $${paramIndex}`);
          values.push(finalNote);
          paramIndex++;

          setClauses.push(`notiz_von = $${paramIndex}`);
          values.push(user.name || user.username || 'Unbekannt');
          paramIndex++;

          setClauses.push(`notiz_am = NOW()`);
        } else {
          setClauses.push(`${field} = $${paramIndex}`);
          values.push(updates[field]);
          paramIndex++;
        }
      }
    }

    if (setClauses.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Keine gültigen Felder zum Updaten' });
    }

    // Audit + letzte_aenderung immer setzen
    setClauses.push(`letzte_aenderung = CURRENT_DATE`);

    values.push(id);

    const query = `
      UPDATE reklamationen
      SET ${setClauses.join(', ')}
      WHERE id = $${paramIndex}
      RETURNING id;
    `;

    const updRes = await client.query(query, values);

    await client.query('COMMIT');

    console.log(
      `Reklamation PATCH – ID: ${id} von ${user.name} (${user.role}): ${JSON.stringify(updates)}`
    );

    res.json({ message: 'Reklamation erfolgreich teilweise aktualisiert' });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Fehler beim PATCH:', err);

    // Unique Tracking-ID (Regel 2) – DB wirft 23505
    if (err && err.code === '23505') {
      return res.status(409).json({
        message: 'Tracking-ID bereits vorhanden (muss global eindeutig sein).',
      });
    }

    res.status(500).json({ message: 'Serverfehler beim Aktualisieren' });
  } finally {
    client.release();
  }
});

/**
 * DELETE /api/reklamationen/:id
 * Nur Admin/Supervisor
 */
router.delete('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;

  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({
      message: 'Zugriff verweigert: Nur Admin oder Supervisor dürfen löschen',
    });
  }

  try {
    const result = await pool.query('DELETE FROM reklamationen WHERE id = $1', [id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    console.log(`Reklamation gelöscht – ID: ${id} von ${user.name} (${user.role})`);
    res.json({ message: 'Reklamation erfolgreich gelöscht' });
  } catch (err) {
    console.error('Fehler beim Löschen:', err.message);
    res.status(500).json({ message: 'Serverfehler beim Löschen' });
  }
});

module.exports = router;
