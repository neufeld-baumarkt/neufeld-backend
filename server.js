// server.js – produktiv: Fingerprint auf jeder Response + klare Public/Private Trennung
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

const pool = require('./db'); // EIN DB-Pool
const verifyToken = require('./middleware/verifyToken'); // ✅ korrekt (bei dir: backend\middleware)

// ──────────────────────────────────────────────────────────────────────────────
// ENV / Basics
// ──────────────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 5000;

// JWT_SECRET ausschließlich aus ENV (Render)
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
  console.error('❌ JWT_SECRET fehlt in der ENV! Server wird beendet.');
  process.exit(1);
}

// Fingerprint-Header (zur Verifikation, dass wirklich diese Version läuft)
const BUILD_FINGERPRINT = process.env.BUILD_FINGERPRINT || 'local-dev';

// ──────────────────────────────────────────────────────────────────────────────
// App
// ──────────────────────────────────────────────────────────────────────────────

const app = express();

app.use(cors());
app.use(express.json());

// Fingerprint auf jede Response
app.use((req, res, next) => {
  res.setHeader('X-Build-Fingerprint', BUILD_FINGERPRINT);
  next();
});

// Healthcheck
app.get('/', (req, res) => {
  res.json({ status: 'ok', fingerprint: BUILD_FINGERPRINT });
});

// ──────────────────────────────────────────────────────────────────────────────
// LOGIN (PUBLIC)
// ──────────────────────────────────────────────────────────────────────────────

app.post('/api/login', async (req, res) => {
  const { name, password } = req.body;

  if (!name || !password) {
    return res.status(400).json({ message: 'Name und Passwort erforderlich' });
  }

  try {
    const result = await pool.query('SELECT * FROM users WHERE name = $1', [name]);
    if (result.rows.length === 0) {
      return res.status(401).json({ message: 'Benutzer nicht gefunden' });
    }

    const user = result.rows[0];
    const isMatch = await bcrypt.compare(password, user.password);

    if (!isMatch) {
      return res.status(401).json({ message: 'Falsches Passwort' });
    }

    const token = jwt.sign(
      {
        id: user.id,
        name: user.name,
        role: user.role,
        filiale: user.filiale,
        email: user.email,
        force_password_change: user.force_password_change,
      },
      JWT_SECRET,
      { expiresIn: '8h' }
    );

    res.json({
      token,
      name: user.name,
      role: user.role,
      filiale: user.filiale,
      email: user.email,
      force_password_change: user.force_password_change,
    });
  } catch (err) {
    console.error('Login-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

// ──────────────────────────────────────────────────────────────────────────────
// USERS (PRIVATE)
// ──────────────────────────────────────────────────────────────────────────────

// Passwort ändern (z. B. Erstlogin bei force_password_change=true)
app.post('/api/users/me/change-password', verifyToken, async (req, res) => {
  const { oldPassword, newPassword } = req.body;

  if (!oldPassword || !newPassword) {
    return res.status(400).json({ message: 'oldPassword und newPassword sind erforderlich' });
  }

  if (String(newPassword).length < 8) {
    return res.status(400).json({ message: 'Neues Passwort muss mindestens 8 Zeichen haben' });
  }

  try {
    // Aktuellen User laden
    const userResult = await pool.query('SELECT * FROM users WHERE id = $1', [req.user.id]);
    if (userResult.rows.length === 0) {
      return res.status(404).json({ message: 'Benutzer nicht gefunden' });
    }

    const user = userResult.rows[0];

    // Altes Passwort prüfen
    const isMatch = await bcrypt.compare(oldPassword, user.password);
    if (!isMatch) {
      return res.status(401).json({ message: 'Altes Passwort ist falsch' });
    }

    // Neues Passwort hashen und speichern + force_password_change zurücksetzen
    const hashed = await bcrypt.hash(newPassword, 10);

    await pool.query(
      `
      UPDATE users
      SET password = $1,
          force_password_change = false
      WHERE id = $2
    `,
      [hashed, req.user.id]
    );

    res.json({ ok: true });
  } catch (err) {
    console.error('Change-Password-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

// ──────────────────────────────────────────────────────────────────────────────
// BUDGET (PRIVATE)
// ──────────────────────────────────────────────────────────────────────────────

app.get('/api/budget/health', verifyToken, async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW() AS now');
    res.json({ ok: true, now: result.rows[0].now });
  } catch (err) {
    console.error('Budget-Health-Fehler:', err);
    res.status(500).json({ ok: false, message: 'Serverfehler' });
  }
});

// Haupt-Budget
app.get('/api/budget', verifyToken, async (req, res) => {
  try {
    const { filiale } = req.query;

    const isCentral =
      req.user?.role === 'Admin' ||
      req.user?.role === 'Supervisor' ||
      req.user?.role === 'Geschäftsführer' ||
      req.user?.role === 'Manager-1' ||
      req.user?.role === 'Manager-2';

    // Filial-User dürfen nur ihre eigene Filiale sehen
    let targetFiliale = filiale;
    if (!isCentral) {
      targetFiliale = req.user?.filiale;
    }

    if (!targetFiliale) {
      return res.status(400).json({ message: 'filiale ist erforderlich' });
    }

    const result = await pool.query(
      `
      SELECT *
      FROM budget.budgets
      WHERE filiale = $1
      ORDER BY jahr DESC, monat DESC
      LIMIT 1
    `,
      [targetFiliale]
    );

    res.json(result.rows[0] || null);
  } catch (err) {
    console.error('Budget-Fetch-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

app.get('/api/budget/budgets', verifyToken, async (req, res) => {
  try {
    const { filiale, jahr } = req.query;

    const isCentral =
      req.user?.role === 'Admin' ||
      req.user?.role === 'Supervisor' ||
      req.user?.role === 'Geschäftsführer' ||
      req.user?.role === 'Manager-1' ||
      req.user?.role === 'Manager-2';

    let targetFiliale = filiale;
    if (!isCentral) {
      targetFiliale = req.user?.filiale;
    }

    if (!targetFiliale) {
      return res.status(400).json({ message: 'filiale ist erforderlich' });
    }

    const params = [targetFiliale];
    let where = 'WHERE filiale = $1';

    if (jahr) {
      params.push(Number(jahr));
      where += ` AND jahr = $${params.length}`;
    }

    const result = await pool.query(
      `
      SELECT *
      FROM budget.budgets
      ${where}
      ORDER BY jahr DESC, monat DESC
    `,
      params
    );

    res.json(result.rows);
  } catch (err) {
    console.error('Budgets-Fetch-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

app.get('/api/budget/bookings', verifyToken, async (req, res) => {
  try {
    const { budget_id } = req.query;

    if (!budget_id) {
      return res.status(400).json({ message: 'budget_id ist erforderlich' });
    }

    const result = await pool.query(
      `
      SELECT *
      FROM budget.bookings
      WHERE budget_id = $1
      ORDER BY datum DESC, id DESC
    `,
      [budget_id]
    );

    res.json(result.rows);
  } catch (err) {
    console.error('Bookings-Fetch-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

app.post('/api/budget/bookings', verifyToken, async (req, res) => {
  try {
    const { budget_id, datum, text, betrag } = req.body;

    if (!budget_id || !datum || !text || betrag === undefined || betrag === null) {
      return res.status(400).json({ message: 'budget_id, datum, text, betrag sind erforderlich' });
    }

    const result = await pool.query(
      `
      INSERT INTO budget.bookings (budget_id, datum, text, betrag)
      VALUES ($1, $2, $3, $4)
      RETURNING *
    `,
      [budget_id, datum, text, betrag]
    );

    res.json(result.rows[0]);
  } catch (err) {
    console.error('Booking-Create-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

app.delete('/api/budget/bookings/:id', verifyToken, async (req, res) => {
  try {
    const bookingId = Number(req.params.id);
    if (!Number.isFinite(bookingId)) {
      return res.status(400).json({ message: 'Ungültige Booking-ID' });
    }

    const result = await pool.query('DELETE FROM budget.bookings WHERE id = $1 RETURNING id', [
      bookingId,
    ]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Buchung nicht gefunden' });
    }

    res.json({ ok: true, id: result.rows[0].id });
  } catch (err) {
    console.error('Booking-Delete-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

// ──────────────────────────────────────────────────────────────────────────────
// Start
// ──────────────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`✅ Server läuft auf Port ${PORT} | Fingerprint: ${BUILD_FINGERPRINT}`);
});