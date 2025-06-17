// middleware/verifyToken.js
const jwt = require('jsonwebtoken');
const JWT_SECRET = process.env.JWT_SECRET || 'fallback-secret';

function verifyToken(requiredRole = null) {
  return (req, res, next) => {
    const authHeader = req.headers['authorization'];

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ message: 'Token fehlt oder ungültig' });
    }

    const token = authHeader.split(' ')[1];

    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      req.user = decoded;

      if (requiredRole && decoded.role !== requiredRole) {
        return res.status(403).json({ message: 'Zugriff verweigert – Rolle unzureichend' });
      }

      next();
    } catch (err) {
      return res.status(401).json({ message: 'Token ungültig oder abgelaufen' });
    }
  };
}

module.exports = verifyToken;
