// server.js – produktiv: Fingerprint auf jeder Response + klare Public/Private Trennung
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

const pool = require('./db'); // EIN DB-Pool
const verifyToken = require('./middleware/verifyToken'); // ✅ korrekt (bei dir: backend\middleware)

// ✅ HOTFIX: JWT Secret mit Fallback (bewusst, temporär)
const JWT_SECRET = process.env.JWT_SECRET || 'supersecretkey123';

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

/**
 * ✅ Private: Tasks (Read-only) – STEP 2.1
 * GET /api/tasks
 * - Filiale: nur eigene Tasks (owner=me)
 * - Andere Rollen: aktuell 403 (bewusst minimal)
 */
app.get('/api/tasks', verifyToken(), async (req, res) => {
  try {
    const { role, filiale } = req.user || {};

    if (role !== 'Filiale') {
      return res.status(403).json({ message: 'Zugriff verweigert (nur Filiale in STEP 2.1).' });
    }

    if (!filiale) {
      return res.status(400).json({ message: 'Filiale im Token fehlt. Bitte erneut anmelden.' });
    }

    const fRes = await pool.query(
      'SELECT id, name FROM public.filialen WHERE name = $1 LIMIT 1',
      [filiale]
    );

    if (fRes.rows.length === 0) {
      return res.status(404).json({ message: `Filiale '${filiale}' nicht in public.filialen gefunden.` });
    }

    const filialeId = fRes.rows[0].id;

    const q = `
      SELECT
        t.id,
        t.owner_type,
        t.owner_id,
        t.title,
        t.body,
        t.status,
        t.created_by_user_id,
        t.created_at,
        t.updated_at,
        t.ack_at,
        t.admin_closed_at,
        t.admin_closed_by_user_id,
        t.admin_note,
        t.executed_at,
        t.executed_by_user_id,
        t.due_at,
        t.source_type,
        t.source_id,
        le.event_type AS last_event_type,
        le.event_at   AS last_event_at
      FROM core.tasks t
      LEFT JOIN LATERAL (
        SELECT event_type, event_at
        FROM core.task_events
        WHERE task_id = t.id
        ORDER BY event_at DESC
        LIMIT 1
      ) le ON true
      WHERE t.owner_type = 'filiale'
        AND t.owner_id = $1
        AND t.status = ANY($2::text[])
      ORDER BY t.created_at DESC
      LIMIT 200
    `;

    const statuses = ['open', 'ack', 'admin_closed'];
    const tRes = await pool.query(q, [filialeId, statuses]);

    return res.json({
      owner: { owner_type: 'filiale', owner_id: filialeId, filiale_name: filiale },
      tasks: tRes.rows,
    });
  } catch (err) {
    console.error('GET /api/tasks Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

// --- alles darunter UNVERÄNDERT ---
// (Tasks Create, Ack, Execute, Admin-Close, PINs, Routes, 404, Listen)

// Routes mounten (bestehend)
const reklamationenRoutes = require('./routes/reklamationen');
const stammdatenRoutes = require('./routes/stammdaten');
const budgetRoutes = require('./routes/budget');
const debugRoutes = require('./routes/debug');

app.use('/api/reklamationen', reklamationenRoutes);
app.use('/api/budget', budgetRoutes);
app.use('/api/debug', debugRoutes);
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
