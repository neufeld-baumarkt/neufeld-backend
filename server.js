// server.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Pool } = require('pg');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

const app = express();
const port = process.env.PORT || 3001;
const JWT_SECRET = process.env.JWT_SECRET || 'supersecretkey123';

app.use(cors());
app.use(bodyParser.json());

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// 🔐 Login mit Token-Ausgabe
app.post('/api/login', async (req, res) => {
  const { name, password } = req.body;

  if (!name || !password) {
    return res.status(400).json({ message: 'Name und Passwort erforderlich' });
  }

  try {
    const query = 'SELECT * FROM users WHERE name = $1';
    const result = await pool.query(query, [name]);

    if (result.rows.length === 0) {
      return res.status(401).json({ message: 'Benutzer nicht gefunden' });
    }

    const user = result.rows[0];
    const isMatch = await bcrypt.compare(password, user.password);

    if (!isMatch) {
      return res.status(401).json({ message: 'Falsches Passwort' });
    }

    const token = jwt.sign(
      { id: user.id, name: user.name, role: user.role },
      JWT_SECRET,
      { expiresIn: '8h' }
    );

    // ✅ WICHTIG: Jetzt auch "filiale" zurückgeben
    return res.json({
      token,
      name: user.name,
      role: user.role,
      filiale: user.filiale  // 👈 diese Zeile ist neu!
    });

  } catch (err) {
    console.error('Login-Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

// ✅ Serverstart
app.listen(port, () => {
  console.log(`✅ Backend läuft auf Port ${port}`);
});
