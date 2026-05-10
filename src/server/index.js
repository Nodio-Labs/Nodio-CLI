#!/usr/bin/env node
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const { getServerConfig } = require('./config');
const { buildRoutes } = require('./routes');
const { NodeModel } = require('./models');
const { markNodeOfflineAndRecover } = require('./services');

async function startServer() {
  const config = getServerConfig();

  await mongoose.connect(config.mongoUri);

  const app = express();
  const corsAllowlist = new Set([
    'https://nodio.me',
    'https://drive.nodio.me',
    'https://effective-space-rotary-phone-wrv6xg64p7w72wj-3000.app.github.dev'
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
    allowedHeaders: ['Content-Type', 'Authorization'],
    optionsSuccessStatus: 200
  };

  app.use(cors(corsOptions));
  app.options(/.*/, cors(corsOptions));
  app.use(express.json({ limit: '10mb' }));
  app.use('/api', buildRoutes(config));

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

  app.listen(config.port, () => {
    console.log(`Nodio central server listening on port ${config.port}`);
  });
}

startServer().catch((error) => {
  console.error('[startup]', error);
  process.exit(1);
});
