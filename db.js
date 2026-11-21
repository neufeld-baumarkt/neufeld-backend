// db.js
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// Idle connections can fail independently of a query. pg removes the failed
// client automatically; handling the event keeps the server running.
pool.on('error', (error) => {
  console.error('PostgreSQL-Pool: Fehler einer ungenutzten Verbindung', {
    code: error.code,
    message: error.message,
  });
});

module.exports = pool;
