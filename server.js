// server.js – produktiv: Fingerprint auf jeder Response + klare Public/Private Trennung
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

const pool = require('./db'); // EIN DB-Pool
const verifyToken = require('./middleware/verifyToken'); // ✅ korrekt (bei dir: backend\middleware)

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
      process.env.JWT_SECRET || 'supersecretkey123',
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

    // Filiale-Name → filialen.id
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

/**
 * ✅ Private: Tasks Create – STEP 2.2
 * POST /api/tasks
 * - Erlaubte Rollen: Admin, Supervisor, Geschäftsführer, Manager-1
 * - Body: owner_type ('filiale'), owner_id (int), title, body, optional due_at, source_type, source_id
 * - Erzeugt: core.tasks (status=open) + core.task_events (created)
 */
app.post('/api/tasks', verifyToken(), async (req, res) => {
  try {
    const { id: actorUserId, role } = req.user || {};

    const ALLOWED = new Set(['Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1']);
    if (!ALLOWED.has(role)) {
      return res.status(403).json({ message: 'Zugriff verweigert (Task erstellen nur Zentrale-Rollen).' });
    }

    const owner_type = String(req.body?.owner_type || '').trim();
    const owner_id_raw = req.body?.owner_id;
    const title = String(req.body?.title || '').trim();
    const body = String(req.body?.body || '').trim();

    const due_at_raw = req.body?.due_at ?? null;
    const source_type = req.body?.source_type != null ? String(req.body.source_type).trim() : null;
    const source_id = req.body?.source_id != null ? String(req.body.source_id).trim() : null;

    if (owner_type !== 'filiale') {
      return res.status(400).json({ message: "owner_type muss 'filiale' sein (Startphase)." });
    }

    const owner_id = Number(owner_id_raw);
    if (!Number.isInteger(owner_id) || owner_id <= 0) {
      return res.status(400).json({ message: 'owner_id muss eine gültige Filial-ID (int) sein.' });
    }

    if (!title) return res.status(400).json({ message: 'title ist Pflicht.' });
    if (!body) return res.status(400).json({ message: 'body ist Pflicht.' });

    let due_at = null;
    if (due_at_raw !== null && due_at_raw !== '') {
      const d = new Date(due_at_raw);
      if (Number.isNaN(d.getTime())) {
        return res.status(400).json({ message: 'due_at ist kein gültiges Datum.' });
      }
      due_at = d.toISOString();
    }

    // Owner existiert?
    const fRes = await pool.query('SELECT id, name FROM public.filialen WHERE id = $1 LIMIT 1', [owner_id]);
    if (fRes.rows.length === 0) {
      return res.status(404).json({ message: `Filiale mit id=${owner_id} nicht gefunden.` });
    }

    // Transaction: Task + Event
    await pool.query('BEGIN');

    const insertTaskQ = `
      INSERT INTO core.tasks (
        owner_type, owner_id, title, body, status, created_by_user_id,
        due_at, source_type, source_id
      )
      VALUES ($1,$2,$3,$4,'open',$5,$6,$7,$8)
      RETURNING
        id, owner_type, owner_id, title, body, status,
        created_by_user_id, created_at, updated_at,
        ack_at, admin_closed_at, admin_closed_by_user_id, admin_note,
        executed_at, executed_by_user_id,
        due_at, source_type, source_id
    `;
    const tIns = await pool.query(insertTaskQ, [
      owner_type,
      owner_id,
      title,
      body,
      actorUserId,
      due_at,
      source_type,
      source_id,
    ]);

    const task = tIns.rows[0];

    const insertEventQ = `
      INSERT INTO core.task_events (task_id, event_type, actor_user_id, meta)
      VALUES ($1,'created',$2,$3)
      RETURNING event_type, event_at
    `;
    const meta = { source: 'api', role };
    const eIns = await pool.query(insertEventQ, [task.id, actorUserId, meta]);

    await pool.query('COMMIT');

    return res.status(201).json({
      task: {
        ...task,
        last_event_type: eIns.rows[0]?.event_type || 'created',
        last_event_at: eIns.rows[0]?.event_at || null,
      },
    });
  } catch (err) {
    try {
      await pool.query('ROLLBACK');
    } catch (_) {
      // ignore rollback errors
    }
    console.error('POST /api/tasks Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * ✅ Private: Task Ack (ohne PIN) – STEP 2.3
 * POST /api/tasks/:id/ack
 * - Erlaubt: nur Filiale (Owner)
 * - open -> ack (+ ack_at) + Event 'ack'
 * - Idempotent: wenn nicht open, dann 200 ohne Änderung (kein neues Event)
 */
app.post('/api/tasks/:id/ack', verifyToken(), async (req, res) => {
  const taskId = String(req.params?.id || '').trim();

  try {
    const { role, filiale, id: actorUserId } = req.user || {};

    if (role !== 'Filiale') {
      return res.status(403).json({ message: 'Zugriff verweigert (ack nur Filiale).' });
    }
    if (!filiale) {
      return res.status(400).json({ message: 'Filiale im Token fehlt. Bitte erneut anmelden.' });
    }
    if (!taskId) {
      return res.status(400).json({ message: 'Task-ID fehlt.' });
    }

    // Filiale-Name → filialen.id
    const fRes = await pool.query(
      'SELECT id, name FROM public.filialen WHERE name = $1 LIMIT 1',
      [filiale]
    );
    if (fRes.rows.length === 0) {
      return res.status(404).json({ message: `Filiale '${filiale}' nicht in public.filialen gefunden.` });
    }
    const filialeId = fRes.rows[0].id;

    // 1) Versuche echten Statuswechsel (nur wenn open)
    await pool.query('BEGIN');

    const updQ = `
      UPDATE core.tasks
      SET status = 'ack',
          ack_at = now(),
          updated_at = now()
      WHERE id = $1
        AND owner_type = 'filiale'
        AND owner_id = $2
        AND status = 'open'
      RETURNING
        id, owner_type, owner_id, title, body, status,
        created_by_user_id, created_at, updated_at,
        ack_at, admin_closed_at, admin_closed_by_user_id, admin_note,
        executed_at, executed_by_user_id,
        due_at, source_type, source_id
    `;
    const updRes = await pool.query(updQ, [taskId, filialeId]);

    if (updRes.rows.length === 1) {
      const task = updRes.rows[0];

      const evQ = `
        INSERT INTO core.task_events (task_id, event_type, actor_user_id, meta)
        VALUES ($1,'ack',$2,$3)
        RETURNING event_type, event_at
      `;
      const meta = { source: 'api', owner_type: 'filiale', owner_id: filialeId };
      const evRes = await pool.query(evQ, [task.id, actorUserId, meta]);

      await pool.query('COMMIT');

      return res.json({
        task: {
          ...task,
          last_event_type: evRes.rows[0]?.event_type || 'ack',
          last_event_at: evRes.rows[0]?.event_at || null,
        },
      });
    }

    // Kein Update: Task entweder nicht existent, gehört nicht zur Filiale, oder ist nicht open.
    await pool.query('ROLLBACK');

    // 2) Idempotent: wenn Task existiert & gehört zur Filiale -> 200 mit aktuellem Zustand
    const getQ = `
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
      WHERE t.id = $1
        AND t.owner_type = 'filiale'
        AND t.owner_id = $2
      LIMIT 1
    `;
    const getRes = await pool.query(getQ, [taskId, filialeId]);

    if (getRes.rows.length === 0) {
      // nicht gefunden ODER gehört nicht zur Filiale
      return res.status(404).json({ message: 'Task nicht gefunden.' });
    }

    return res.json({ task: getRes.rows[0] });
  } catch (err) {
    try {
      await pool.query('ROLLBACK');
    } catch (_) {
      // ignore rollback errors
    }
    console.error('POST /api/tasks/:id/ack Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

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
