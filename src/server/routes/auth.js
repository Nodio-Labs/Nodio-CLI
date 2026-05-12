const express = require('express');
const crypto = require('crypto');
const bcrypt = require('bcrypt');
const { v4: uuidv4 } = require('uuid');
const { UserModel } = require('../models');
const verifyToken = require('../middleware/verifyToken');

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

const router = express.Router();

router.post('/register', async (req, res, next) => {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    const password = String(req.body?.password || '');

    if (!email || !isValidEmail(email)) {
      return res.status(400).json({ error: 'Invalid email format' });
    }
    if (!password) {
      return res.status(400).json({ error: 'Password is required' });
    }

    const existing = await UserModel.findOne({ email }).lean();
    if (existing) {
      return res.status(400).json({ error: 'Email already exists' });
    }

    const passwordHash = await bcrypt.hash(password, 12);
    const argon2Salt = crypto.randomBytes(32).toString('hex');
    const apiToken = crypto.randomBytes(64).toString('hex');
    const userId = uuidv4();

    const user = await UserModel.create({
      userId,
      email,
      passwordHash,
      argon2Salt,
      apiToken
    });

    res.json({
      userId: user.userId,
      apiToken: user.apiToken,
      argon2Salt: user.argon2Salt
    });
  } catch (error) {
    next(error);
  }
});

router.post('/login', async (req, res, next) => {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    const password = String(req.body?.password || '');

    if (!email || !password) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    const user = await UserModel.findOne({ email });
    if (!user) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    const matches = await bcrypt.compare(password, user.passwordHash);
    if (!matches) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    res.json({
      userId: user.userId,
      email: user.email,
      apiToken: user.apiToken,
      argon2Salt: user.argon2Salt
    });
  } catch (error) {
    next(error);
  }
});

router.get('/me', verifyToken, async (req, res, next) => {
  try {
    res.json({
      userId: req.user?.userId,
      email: req.user?.email,
      argon2Salt: req.user?.argon2Salt
    });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
