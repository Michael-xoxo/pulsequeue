const jwt = require('jsonwebtoken');

const authGuard = (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ message: 'Authentication error: No token provided' });
    }

    const token = authHeader.split(' ')[1];
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    const userId = decoded._id || decoded.id;

    req.user = {
      ...decoded,
      _id: userId,
      id: userId,
    };

    return next();
  } catch (error) {
    return res.status(401).json({ message: 'Authentication error: Invalid or expired token' });
  }
};

authGuard.authGuard = authGuard;
authGuard.protect = authGuard;

module.exports = authGuard;
