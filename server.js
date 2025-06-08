
// server.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const app = express();
const port = process.env.PORT || 3001;

// 🔓 CORS-Freigabe (für alle Domains – später kannst du das einschränken)
app.use(cors());

// JSON-Parsing
app.use(bodyParser.json());

// PostgreSQL-Verbindung
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// 🔐 Login-Endpoint mit bcrypt-Vergleich
app.post('/api/login', async (req, res) => {
  const { name, password } = req.body;

  if (!name || !password) {
    return res.status(400).json({ success: false, message: 'Name und Passwort erforderlich' });
  }

  try {
    const query = 'SELECT * FROM users WHERE name = $1';
    const result = await pool.query(query, [name]);

    if (result.rows.length === 0) {
      return res.status(401).json({ success: false, message: 'Benutzer nicht gefunden' });
    }

    const user = result.rows[0];

    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(401).json({ success: false, message: 'Falsches Passwort' });
    }

    return res.json({ success: true, role: user.role, name: user.name });
  } catch (err) {
    console.error('Login-Fehler:', err);
    return res.status(500).json({ success: false, message: 'Serverfehler' });
  }
});

// ✅ Serverstart
app.listen(port, () => {
  console.log(`✅ Backend läuft auf Port ${port}`);
});
