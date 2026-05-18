#!/usr/bin/env node
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const { getServerConfig } = require('./config');
const { buildRoutes } = require('./routes');
const authRoutes = require('./routes/auth');
const { NodeModel, FileModel } = require('./models');
const verifyToken = require('./middleware/verifyToken');
const { markNodeOfflineAndRecover } = require('./services');
const { processPendingFilecoinBackupJobs } = require('./filecoinBackup');
const { getWalletBalance } = require('../../services/filecoin');

async function startServer() {
  const config = getServerConfig();

  await mongoose.connect(config.mongoUri);

  const app = express();
  const corsAllowlist = new Set([
    'https://nodio.me',
    'http://nodio.me',
    'https://drive.nodio.me',
    'https://effective-space-rotary-phone-wrv6xg64p7w72wj-3000.app.github.dev',
    'https://cautious-sniffle-wrv6xg64pg7jhq7x-5173.app.github.dev',
    'http://localhost:5173',
    'http://127.0.0.1:5173'
  ]);

  const corsOptions = {
    origin(origin, callback) {
      if (!origin || corsAllowlist.has(origin)) {
        callback(null, true);
        return;
      }
      callback(new Error('Not allowed by CORS'));
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'x-api-token'],
    optionsSuccessStatus: 200
  };

  app.use(cors(corsOptions));
  app.options(/.*/, cors(corsOptions));
  app.use(express.json({ limit: '50mb' }));
  app.use('/api/auth', authRoutes);
  app.use('/api', buildRoutes(config));

  app.get('/api/files', verifyToken, async (req, res, next) => {
    try {
      const files = await FileModel.find({ userId: req.userId })
        .sort({ createdAt: -1 })
        .select('fileId originalName sizeBytes createdAt filecoinBackedUp filecoinCid')
        .lean();

      res.json({ files });
    } catch (error) {
      next(error);
    }
  });

  app.use((error, _req, res, _next) => {
    const status = error.statusCode || 500;
    const message = error.message || 'internal server error';
    if (status >= 500) {
      console.error('[server-error]', error);
    }
    res.status(status).json({ error: message });
  });

  const offlineThresholdMs = config.heartbeatIntervalMs * config.offlineAfterMisses;

  setInterval(async () => {
    try {
      const staleBefore = new Date(Date.now() - offlineThresholdMs);
      const staleNodes = await NodeModel.find({
        status: 'online',
        lastHeartbeatAt: { $lt: staleBefore }
      })
        .select('nodeId')
        .lean();

      for (const node of staleNodes) {
        const changed = await markNodeOfflineAndRecover(node.nodeId, config.emergencyReplicaFloor);
        if (changed) {
          console.warn(`[heartbeat] node ${node.nodeId} marked offline`);
        }
      }
    } catch (error) {
      console.error('[offline-monitor]', error);
    }
  }, config.heartbeatIntervalMs);

  setInterval(() => {
    processPendingFilecoinBackupJobs(1).catch((error) => {
      console.error('[filecoin-backup]', error);
    });
  }, config.filecoinBackupIntervalMs);

  app.listen(config.port, () => {
    console.log(`Nodio central server listening on port ${config.port}`);
  });

  const dailyMs = 24 * 60 * 60 * 1000;
  const checkBalance = async () => {
    const balance = await getWalletBalance();
    if (balance !== null && balance < 2) {
      console.warn(`[filecoin] USDFC balance low: ${balance}`);
    }
  };

  setTimeout(() => {
    checkBalance().catch((error) => {
      console.error('[filecoin] balance check failed', error.message);
    });
  }, 5000);

  setInterval(() => {
    checkBalance().catch((error) => {
      console.error('[filecoin] balance check failed', error.message);
    });
  }, dailyMs);
}

startServer().catch((error) => {
  console.error('[startup]', error);
  process.exit(1);
});
