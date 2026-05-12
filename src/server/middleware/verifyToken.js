const { UserModel } = require('../models');

function extractToken(req) {
  const header = req.headers.authorization || '';
  if (header.startsWith('Bearer ')) {
    return header.slice('Bearer '.length).trim();
  }
  const apiTokenHeader = req.headers['x-api-token'];
  if (typeof apiTokenHeader === 'string' && apiTokenHeader.trim()) {
    return apiTokenHeader.trim();
  }
  return null;
}

async function verifyToken(req, res, next) {
  try {
    const token = extractToken(req);
    if (!token) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const user = await UserModel.findOne({ apiToken: token }).lean();
    if (!user) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    req.userId = user.userId;
    req.user = user;
    return next();
  } catch (error) {
    return next(error);
  }
}

module.exports = verifyToken;
