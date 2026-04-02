// server.js – produktiv: Fingerprint auf jeder Response + klare Public/Private Trennung
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

const pool = require('./db'); // EIN DB-Pool
const verifyToken = require('./middleware/verifyToken'); // ✅ korrekt (bei dir: backend\middleware)

// ✅ FINAL: JWT_SECRET kommt ausschließlich aus ENV (Render)
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
  throw new Error('JWT_SECRET is not set in environment variables');
}

const app = express();

// ---- Fingerprint (damit wir nie wieder raten) ----
const BUILD_TAG = process.env.BUILD_TAG || 'local-unknown';
const START_TS = new Date().toISOString();

// -----------------------------
// CORS (HARDENED: Whitelist)
// -----------------------------
const ALLOWED_ORIGINS = new Set([
  'https://app.neufeldbaumarkt.de',
  'http://localhost:3000',
]);

const corsOptions = {
  origin: (origin, callback) => {
    // Requests ohne Origin (z.B. curl/Postman/Server-to-Server) zulassen
    if (!origin) return callback(null, true);

    if (ALLOWED_ORIGINS.has(origin)) return callback(null, true);

    // Nicht erlaubte Origins: CORS Header werden nicht gesetzt -> Browser blockt
    return callback(null, false);
  },
  methods: ['GET', 'HEAD', 'PUT', 'PATCH', 'POST', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-filiale'],
  credentials: false,
  optionsSuccessStatus: 204,
};

// Middleware
app.use(cors(corsOptions));
app.options(/.*/, cors(corsOptions)); // Preflight sauber beantworten
app.use(express.json());

// Fingerprint-Header auf JEDER Response (auch Fehler)
app.use((req, res, next) => {
  res.setHeader('x-neufeld-service', 'neufeld-backend');
  res.setHeader('x-neufeld-build-tag', BUILD_TAG);
  res.setHeader('x-neufeld-started-at', START_TS);
  next();
});

// ✅ Public: Health (ohne Token)
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'neufeld-backend',
    buildTag: BUILD_TAG,
    startedAt: START_TS,
  });
});

// ✅ Public: Ping (ohne Token)
app.get('/api/ping', (req, res) => {
  res.json({ message: 'pong', timestamp: new Date().toISOString() });
});

// ✅ Public: Login (ohne Token)
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
      { id: user.id, name: user.name, role: user.role, filiale: user.filiale },
      JWT_SECRET,
      { expiresIn: '8h' }
    );

    res.json({ token, name: user.name, role: user.role, filiale: user.filiale });
  } catch (err) {
    console.error('Login-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

// ──────────────────────────────────────────────────────────────────────────────
// USERS (PRIVATE) – aus Backup übernommen (robust umgesetzt)
// ──────────────────────────────────────────────────────────────────────────────
// Passwort ändern (z. B. Erstlogin bei force_password_change=true)
app.post('/api/users/me/change-password', verifyToken(), async (req, res) => {
  const { oldPassword, newPassword } = req.body || {};

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
    const isMatch = await bcrypt.compare(String(oldPassword), user.password);
    if (!isMatch) {
      return res.status(401).json({ message: 'Altes Passwort ist falsch' });
    }

    // Neues Passwort hashen
    const hashed = await bcrypt.hash(String(newPassword), 10);

    // Versuch 1: inkl. force_password_change (wenn Spalte existiert)
    try {
      await pool.query(
        `
        UPDATE users
        SET password = $1,
            force_password_change = false
        WHERE id = $2
      `,
        [hashed, req.user.id]
      );
    } catch (e) {
      // Fallback: wenn force_password_change-Spalte nicht existiert
      const msg = String(e?.message || '');
      const code = String(e?.code || '');

      // Postgres: undefined_column => 42703
      if (code === '42703' || msg.toLowerCase().includes('force_password_change')) {
        await pool.query(
          `
          UPDATE users
          SET password = $1
          WHERE id = $2
        `,
          [hashed, req.user.id]
        );
      } else {
        throw e;
      }
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error('Change-Password-Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

// Routes mounten (bestehend + ausgelagert)
const reklamationenRoutes = require('./routes/reklamationen');
const stammdatenRoutes = require('./routes/stammdaten');
const budgetRoutes = require('./routes/budget');
const debugRoutes = require('./routes/debug');
const tasksRoutes = require('./routes/tasks');
const filialePinsRoutes = require('./routes/filialePins');
const bestellungenRoutes = require('./routes/bestellungen');

app.use('/api/reklamationen', reklamationenRoutes);
app.use('/api/budget', budgetRoutes);
app.use('/api/debug', debugRoutes);
app.use('/api/tasks', tasksRoutes);
app.use('/api/filiale-pins', filialePinsRoutes);
app.use('/api/bestellungen', bestellungenRoutes);
app.use('/api', stammdatenRoutes);

// 404 Fallback – als JSON + Fingerprint
app.use((req, res) => {
  res.status(404).json({
    error: 'not_found',
    message: `Cannot ${req.method} ${req.originalUrl}`,
    buildTag: BUILD_TAG,
    startedAt: START_TS,
  });
});

// Port / Binding für Render
const PORT = process.env.PORT || 3001;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ Backend läuft auf Port ${PORT} (0.0.0.0)`);
  console.log(`🧩 BUILD_TAG=${BUILD_TAG} START_TS=${START_TS}`);
});