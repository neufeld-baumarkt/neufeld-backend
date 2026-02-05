const jwt = require('jsonwebtoken');

// ✅ FINAL: JWT_SECRET nur aus ENV
const JWT_SECRET = process.env.JWT_SECRET;

if (!JWT_SECRET) {
  throw new Error('JWT_SECRET is not set in environment variables');
}

function verifyToken(requiredRole = null) {
  return (req, res, next) => {
    const authHeader = req.headers['authorization'];

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        message: 'Kein Zugriffstoken gefunden. Bitte erneut anmelden.',
      });
    }

    const token = authHeader.split(' ')[1];

    try {
      const decoded = jwt.verify(token, JWT_SECRET);

      req.user = {
        id: decoded.id,
        name: decoded.name,
        role: decoded.role,
        filiale: decoded.filiale || null,
      };

      if (requiredRole && decoded.role !== requiredRole) {
        return res.status(403).json({
          message: `Zugriff verweigert. Erforderliche Rolle: ${requiredRole}`,
        });
      }

      next();
    } catch (err) {
      if (err.name === 'TokenExpiredError') {
        return res.status(401).json({ message: 'Token abgelaufen. Bitte erneut anmelden.' });
      }
      return res.status(401).json({ message: 'Token ungültig.' });
    }
  };
}

module.exports = verifyToken;
