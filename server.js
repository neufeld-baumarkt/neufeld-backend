// server.js – produktiv: Fingerprint auf jeder Response + klare Public/Private Trennung
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

const app = express();

// ---- Fingerprint (damit wir nie wieder raten) ----
const BUILD_TAG = process.env.BUILD_TAG || 'local-unknown';
const START_TS = new Date().toISOString();

// Middleware
app.use(cors());
app.use(express.json());

// Fingerprint-Header auf JEDER Response (auch Fehler)
app.use((req, res, next) => {
  res.setHeader('x-neufeld-service', 'neufeld-backend');
  res.setHeader('x-neufeld-build-tag', BUILD_TAG);
  res.setHeader('x-neufeld-started-at', START_TS);
  next();
});

// DB Pool
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
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
      process.env.JWT_SECRET || 'supersecretkey123',
      { expiresIn: '8h' }
    );

    res.json({ token, name: user.name, role: user.role, filiale: user.filiale });
  } catch (err) {
    console.error('Login-Fehler:', err);
    res.status(500).json({ message: 'Serverfehler' });
  }
});

// Routes mounten
const reklamationenRoutes = require('./routes/reklamationen');
const stammdatenRoutes = require('./routes/stammdaten');

app.use('/api/reklamationen', reklamationenRoutes);
app.use('/api', stammdatenRoutes);

// 404 Fallback – als JSON + Fingerprint (damit wir sehen WER antwortet)
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
