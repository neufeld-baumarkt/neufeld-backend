// server.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

const app = express();

// Middleware
app.use(cors());
app.use(express.json());  // statt bodyParser.json()

// DB Pool
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// 🔐 Login Route
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

// Routes importieren
const reklamationenRoutes = require('./routes/reklamationen');
const stammdatenRoutes = require('./routes/stammdaten');

// Routes mounten – ALLE unter /api
app.use('/api/reklamationen', reklamationenRoutes);
app.use('/api', stammdatenRoutes);  // <-- Jetzt korrekt mit /api Prefix!

// Temporäre Ping/Test-Route (kann später entfernt werden)
app.get('/api/ping', (req, res) => {
  console.log('🏓 Ping received – Backend ist wach und erreichbar');
  res.json({ message: 'pong', timestamp: new Date().toISOString() });
});

// Port und Binding – WICHTIG für Render!
const PORT = process.env.PORT || 3001;

app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ Backend läuft auf Port ${PORT} und bindet an 0.0.0.0`);
});