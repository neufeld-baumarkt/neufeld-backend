// hash.js
const bcrypt = require('bcrypt');

const password = 'k92#3Cy6'; // ⬅ Ersetze durch das echte Passwort
bcrypt.hash(password, 10).then(hash => {
  console.log('Hash:', hash);
});
