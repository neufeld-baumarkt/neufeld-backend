// middleware/verifyToken.js – ROBUSTE Version mit besserem Error-Handling
const jwt = require('jsonwebtoken');

// ✅ HOTFIX: muss EXAKT zum server.js passen, sonst sind Tokens immer "gefälscht"
const JWT_SECRET = process.env.JWT_SECRET || 'supersecretkey123';

function verifyToken(requiredRole = null) {
  return (req, res, next) => {
    const authHeader = req.headers['authorization'];

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        message: 'Kein Zugriffstoken gefunden. Bitte erneut anmelden.',
      });
    }

    const token = authHeader.split(' ')[1];
    if (!token) {
      return res.status(401).json({
        message: 'Token-Format ungültig (erwartet: Bearer <token>)',
      });
    }

    try {
      const decoded = jwt.verify(token, JWT_SECRET);

      // req.user erweitern für einfache Nutzung in Routen
      req.user = {
        id: decoded.id,
        name: decoded.name,
        role: decoded.role,
        filiale: decoded.filiale || null, // explizit null für Zentrale-User
      };

      // Rollenprüfung (falls requiredRole angegeben)
      if (requiredRole && decoded.role !== requiredRole) {
        return res.status(403).json({
          message: `Zugriff verweigert. Erforderliche Rolle: ${requiredRole}`,
        });
      }

      next();
    } catch (err) {
      console.error('JWT-Fehler:', err.name, err.message);

      if (err.name === 'TokenExpiredError') {
        return res.status(401).json({
          message: 'Token abgelaufen. Bitte erneut anmelden.',
        });
      }
      if (err.name === 'JsonWebTokenError') {
        return res.status(401).json({
          message: 'Token beschädigt oder gefälscht.',
        });
      }

      return res.status(401).json({
        message: 'Token ungültig. Bitte erneut anmelden.',
      });
    }
  };
}

module.exports = verifyToken;
