const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const {
  NodeModel,
  FileModel,
  ShardModel,
  ShardPlacementModel,
  ReplicationTaskModel,
  RelayTaskModel
} = require('./models');
const verifyToken = require('./middleware/verifyToken');
const { uploadToFilecoin, retrieveFromFilecoin } = require('../../services/filecoin');
const {
  chooseDistinctOnlineNodes,
  ensureEmergencyReplicasForShard
} = require('./services');
const {
  queueFilecoinBackupJob,
  processPendingFilecoinBackupJobs,
  recordFilecoinBackupMessage
} = require('./filecoinBackup');

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function normalizeUrl(url) {
  return String(url || '').replace(/\/+$/, '');
}

async function claimPendingRelayTasks(nodeId, limit = 10) {
  const pendingRelayTasks = await RelayTaskModel.find({
    nodeId,
    status: 'pending'
  })
    .sort({ createdAt: 1 })
    .limit(limit)
    .lean();

  if (pendingRelayTasks.length > 0) {
    await RelayTaskModel.updateMany(
      { _id: { $in: pendingRelayTasks.map((task) => task._id) } },
      { $set: { status: 'in_progress' }, $inc: { attempts: 1 } }
    );
  }

  return pendingRelayTasks.map((task) => ({
    taskId: task._id.toString(),
    taskType: task.taskType,
    shardId: task.shardId,
    fileId: task.fileId,
    dataBase64: task.dataBase64
  }));
}

function buildRoutes(config) {
  const router = express.Router();

  router.get('/health', async (_req, res) => {
    res.json({ ok: true, service: 'nodio-server' });
  });

  router.get('/files', verifyToken, async (req, res, next) => {
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

  router.post('/nodes/register', async (req, res, next) => {
    try {
      const { nodeId, deviceKey, nodeKey, nodeSecret: providedNodeSecret, knownNodeIds, url, capacityBytes, freeBytes } = req.body;

      if (!url) {
        return res.status(400).json({ error: 'url is required' });
      }

      const capacity = Number(capacityBytes);
      const free = Number(freeBytes);
      if (!Number.isFinite(capacity) || capacity <= 0 || !Number.isFinite(free) || free < 0) {
        return res.status(400).json({ error: 'capacityBytes and freeBytes must be valid numbers' });
      }
      if (deviceKey && typeof deviceKey !== 'string') {
        return res.status(400).json({ error: 'deviceKey must be a string when provided' });
      }
      if (knownNodeIds !== undefined && !Array.isArray(knownNodeIds)) {
        return res.status(400).json({ error: 'knownNodeIds must be an array when provided' });
      }

      const normalizedKnownNodeIds = Array.isArray(knownNodeIds)
        ? [...new Set(knownNodeIds.filter((value) => typeof value === 'string' && value.length > 0))]
        : [];

      let existingByNodeKey = null;
      if (nodeKey) {
        existingByNodeKey = await NodeModel.findOne({ nodeKey }).lean();
      }

      if (existingByNodeKey && deviceKey && existingByNodeKey.deviceKey && existingByNodeKey.deviceKey !== deviceKey) {
        return res.status(409).json({ error: 'nodeKey belongs to a different device' });
      }

      if (existingByNodeKey && nodeId && nodeId !== existingByNodeKey.nodeId) {
        return res.status(409).json({ error: 'nodeKey is already associated with a different nodeId' });
      }

      let effectiveNodeId = nodeId || existingByNodeKey?.nodeId || null;
      let claimedKnownNode = null;

      if (effectiveNodeId && deviceKey) {
        const existingByNodeId = await NodeModel.findOne({ nodeId: effectiveNodeId }).lean();
        if (
          existingByNodeId
          && existingByNodeId.deviceKey
          && existingByNodeId.deviceKey !== deviceKey
        ) {
          return res.status(409).json({ error: 'nodeId belongs to a different device' });
        }
      }

      if (!effectiveNodeId && deviceKey && normalizedKnownNodeIds.length > 0) {
        claimedKnownNode = await NodeModel.findOneAndUpdate(
          {
            deviceKey,
            status: 'offline',
            nodeId: { $in: normalizedKnownNodeIds }
          },
          {
            $set: {
              url,
              capacityBytes: capacity,
              freeBytes: free,
              status: 'online',
              lastHeartbeatAt: new Date(),
              ...(nodeKey ? { nodeKey } : {})
            }
          },
          {
            new: true,
            sort: { createdAt: 1 }
          }
        );

        if (claimedKnownNode) {
          effectiveNodeId = claimedKnownNode.nodeId;
        }
      }

      if (!effectiveNodeId) {
        effectiveNodeId = `donor-${uuidv4().slice(0, 8)}`;
      }

      const node = claimedKnownNode
        || await NodeModel.findOneAndUpdate(
          { nodeId: effectiveNodeId },
          {
            $set: {
              nodeId: effectiveNodeId,
              ...(deviceKey ? { deviceKey } : {}),
              ...(nodeKey ? { nodeKey } : {}),
              url,
              capacityBytes: capacity,
              freeBytes: free,
              status: 'online',
              lastHeartbeatAt: new Date()
            }
          },
          { upsert: true, new: true, setDefaultsOnInsert: true }
        );

      const nodeSecret = node.nodeSecret || (typeof providedNodeSecret === 'string' && providedNodeSecret.trim()) || crypto.randomBytes(64).toString('hex');
      if (!node.nodeSecret || node.nodeSecret !== nodeSecret) {
        await NodeModel.updateOne(
          { nodeId: node.nodeId },
          { $set: { nodeSecret } }
        );
      }

      res.json({
        nodeId: node.nodeId,
        status: node.status,
        heartbeatIntervalMs: config.heartbeatIntervalMs,
        relayPollIntervalMs: config.relayPollIntervalMs,
        minReplicas: config.minReplicas,
        emergencyReplicaFloor: config.emergencyReplicaFloor,
        nodeSecret
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/nodes/heartbeat', async (req, res, next) => {
    try {
      const { nodeId, freeBytes, shardIds } = req.body;
      if (!nodeId) {
        return res.status(400).json({ error: 'nodeId is required' });
      }

      const node = await NodeModel.findOne({ nodeId });
      if (!node) {
        return res.status(404).json({ error: 'node not found' });
      }

      node.status = 'online';
      node.lastHeartbeatAt = new Date();
      if (Number.isFinite(Number(freeBytes)) && Number(freeBytes) >= 0) {
        node.freeBytes = Number(freeBytes);
      }
      await node.save();

      const nodeLockedForFilecoin = Boolean(
        node.filecoinUploadLockUntil && new Date(node.filecoinUploadLockUntil).getTime() > Date.now()
      );

      if (Array.isArray(shardIds) && shardIds.length > 0) {
        try {
          const normalizedShardIds = [...new Set(shardIds.filter(Boolean))];
          const knownShards = await ShardModel.find({ shardId: { $in: normalizedShardIds } })
            .select('shardId fileId')
            .lean();

          for (const shard of knownShards) {
            if (!shard.fileId) {
              continue;
            }

            await ShardPlacementModel.updateOne(
              { shardId: shard.shardId, nodeId },
              {
                $set: {
                  status: 'available',
                  fileId: shard.fileId
                },
                $setOnInsert: {
                  shardId: shard.shardId,
                  nodeId
                }
              },
              { upsert: true }
            );
          }
        } catch (syncError) {
          console.warn('[heartbeat-sync]', nodeId, syncError.message);
        }
      }

      let tasksWithSourceUrl = [];
      let relayTasks = [];

      if (!nodeLockedForFilecoin) {
        const pendingTasks = await ReplicationTaskModel.find({
          targetNodeId: nodeId,
          status: 'pending'
        })
          .sort({ createdAt: 1 })
          .limit(5)
          .lean();

        if (pendingTasks.length > 0) {
          await ReplicationTaskModel.updateMany(
            { _id: { $in: pendingTasks.map((task) => task._id) } },
            { $set: { status: 'in_progress' }, $inc: { attempts: 1 } }
          );
        }

        tasksWithSourceUrl = [];
        for (const task of pendingTasks) {
          const source = await NodeModel.findOne({ nodeId: task.sourceNodeId, status: 'online' })
            .select('url nodeId nodeSecret')
            .lean();

          if (!source) {
            await ReplicationTaskModel.updateOne(
              { _id: task._id },
              {
                $set: {
                  status: 'failed',
                  errorMessage: 'source node is offline'
                }
              }
            );
            continue;
          }

          tasksWithSourceUrl.push({
            taskId: task._id.toString(),
            shardId: task.shardId,
            fileId: task.fileId,
            sourceNodeId: source.nodeId,
            sourceUrl: source.url,
            sourceNodeSecret: source.nodeSecret || null
          });
        }

        relayTasks = await claimPendingRelayTasks(nodeId, 10);
      }

      res.json({
        ok: true,
        now: new Date().toISOString(),
        nodeLockedForFilecoin,
        replicationTasks: tasksWithSourceUrl,
        relayTasks
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/nodes/relay-pull', async (req, res, next) => {
    try {
      const { nodeId } = req.body;
      if (!nodeId) {
        return res.status(400).json({ error: 'nodeId is required' });
      }

      const node = await NodeModel.findOne({ nodeId });
      if (!node) {
        return res.status(404).json({ error: 'node not found' });
      }

      const nodeLockedForFilecoin = Boolean(
        node.filecoinUploadLockUntil && new Date(node.filecoinUploadLockUntil).getTime() > Date.now()
      );

      if (nodeLockedForFilecoin) {
        return res.json({
          ok: true,
          now: new Date().toISOString(),
          nodeLockedForFilecoin: true,
          relayTasks: []
        });
      }

      const relayTasks = await claimPendingRelayTasks(nodeId, 10);

      if (relayTasks.length > 0 && node.pendingRelayAlert) {
        node.pendingRelayAlert = false;
        node.pendingRelayAlertAt = null;
        await node.save();
      }

      res.json({
        ok: true,
        now: new Date().toISOString(),
        relayTasks
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/nodes/:nodeId/secret', verifyToken, async (req, res, next) => {
    try {
      const { nodeId } = req.params;
      if (!nodeId) {
        return res.status(400).json({ error: 'nodeId is required' });
      }

      const node = await NodeModel.findOne({ nodeId }).select('nodeId nodeSecret').lean();
      if (!node) {
        return res.status(404).json({ error: 'node not found' });
      }

      return res.json({ nodeSecret: node.nodeSecret || null });
    } catch (error) {
      next(error);
    }
  });

  router.post('/nodes/:nodeId/alert-relay-pending', async (req, res, next) => {
    try {
      const { nodeId } = req.params;
      const node = await NodeModel.findOne({ nodeId });
      if (!node) {
        return res.status(404).json({ error: 'node not found' });
      }
      // Set pending relay flag so donor knows to check urgently
      node.pendingRelayAlert = true;
      node.pendingRelayAlertAt = new Date();
      await node.save();
      res.json({ ok: true, message: 'relay pending alert sent' });
    } catch (error) {
      next(error);
    }
  });

  router.post('/replication-tasks/:taskId/complete', async (req, res, next) => {
    try {
      const { taskId } = req.params;
      const { nodeId, success, errorMessage } = req.body;

      const task = await ReplicationTaskModel.findById(taskId);
      if (!task) {
        return res.status(404).json({ error: 'replication task not found' });
      }

      if (task.targetNodeId !== nodeId) {
        return res.status(403).json({ error: 'nodeId does not match task target' });
      }

      if (success) {
        task.status = 'completed';
        task.errorMessage = null;
        await task.save();

        await ShardPlacementModel.updateOne(
          { shardId: task.shardId, nodeId: task.targetNodeId },
          {
            $set: {
              fileId: task.fileId,
              status: 'available'
            },
            $setOnInsert: {
              shardId: task.shardId,
              nodeId: task.targetNodeId
            }
          },
          { upsert: true }
        );

        await ensureEmergencyReplicasForShard(task.shardId, config.emergencyReplicaFloor);
      } else {
        task.status = 'failed';
        task.errorMessage = errorMessage || 'replication failed';
        await task.save();
      }

      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  router.post('/relay-tasks/:taskId/complete', async (req, res, next) => {
    try {
      const { taskId } = req.params;
      const { nodeId, success, errorMessage, resultDataBase64 } = req.body;

      const task = await RelayTaskModel.findById(taskId);
      if (!task) {
        return res.status(404).json({ error: 'relay task not found' });
      }

      if (task.nodeId !== nodeId) {
        return res.status(403).json({ error: 'nodeId does not match relay task target' });
      }

      if (success) {
        task.status = 'completed';
        task.errorMessage = null;
        task.resultDataBase64 = typeof resultDataBase64 === 'string' ? resultDataBase64 : null;
      } else {
        task.status = 'failed';
        task.errorMessage = errorMessage || 'relay task failed';
      }

      await task.save();
      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  router.post('/relay/shards/store', async (req, res, next) => {
    try {
      const { opId, shardId, fileId, nodeIds, dataBase64 } = req.body;
      if (!shardId || !Array.isArray(nodeIds) || nodeIds.length === 0 || typeof dataBase64 !== 'string') {
        return res.status(400).json({ error: 'shardId, nodeIds and dataBase64 are required' });
      }

      const normalizedNodeIds = [...new Set(nodeIds.filter((value) => typeof value === 'string' && value.length > 0))];
      if (normalizedNodeIds.length === 0) {
        return res.status(400).json({ error: 'nodeIds must contain at least one valid nodeId' });
      }

      const operationId = opId || uuidv4();
      const docs = normalizedNodeIds.map((nodeId) => ({
        opId: operationId,
        taskType: 'store',
        nodeId,
        shardId,
        fileId: fileId || null,
        dataBase64,
        status: 'pending'
      }));

      await RelayTaskModel.insertMany(docs);
      res.json({ ok: true, opId: operationId, queued: docs.length });
    } catch (error) {
      next(error);
    }
  });

  router.get('/relay/shards/store/:opId', async (req, res, next) => {
    try {
      const { opId } = req.params;
      const tasks = await RelayTaskModel.find({ opId, taskType: 'store' }).lean();
      if (tasks.length === 0) {
        return res.status(404).json({ error: 'relay store operation not found' });
      }

      const successfulNodeIds = tasks.filter((task) => task.status === 'completed').map((task) => task.nodeId);
      const failed = tasks
        .filter((task) => task.status === 'failed')
        .map((task) => ({ nodeId: task.nodeId, errorMessage: task.errorMessage || 'failed' }));

      res.json({
        ok: true,
        opId,
        pendingCount: tasks.filter((task) => task.status === 'pending' || task.status === 'in_progress').length,
        successfulNodeIds,
        failed
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/relay/shards/fetch', async (req, res, next) => {
    try {
      const { opId, shardId, nodeIds } = req.body;
      if (!shardId || !Array.isArray(nodeIds) || nodeIds.length === 0) {
        return res.status(400).json({ error: 'shardId and nodeIds are required' });
      }

      const normalizedNodeIds = [...new Set(nodeIds.filter((value) => typeof value === 'string' && value.length > 0))];
      if (normalizedNodeIds.length === 0) {
        return res.status(400).json({ error: 'nodeIds must contain at least one valid nodeId' });
      }

      const operationId = opId || uuidv4();
      const docs = normalizedNodeIds.map((nodeId) => ({
        opId: operationId,
        taskType: 'fetch',
        nodeId,
        shardId,
        status: 'pending'
      }));

      await RelayTaskModel.insertMany(docs);
      res.json({ ok: true, opId: operationId, queued: docs.length });
    } catch (error) {
      next(error);
    }
  });

  router.get('/relay/shards/fetch/:opId', async (req, res, next) => {
    try {
      const { opId } = req.params;
      const tasks = await RelayTaskModel.find({ opId, taskType: 'fetch' }).lean();
      if (tasks.length === 0) {
        return res.status(404).json({ error: 'relay fetch operation not found' });
      }

      const completed = tasks.find((task) => task.status === 'completed' && task.resultDataBase64);
      const failed = tasks
        .filter((task) => task.status === 'failed')
        .map((task) => ({ nodeId: task.nodeId, errorMessage: task.errorMessage || 'failed' }));

      res.json({
        ok: true,
        opId,
        pendingCount: tasks.filter((task) => task.status === 'pending' || task.status === 'in_progress').length,
        hasResult: Boolean(completed),
        nodeId: completed?.nodeId || null,
        resultDataBase64: completed?.resultDataBase64 || null,
        failed
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/files/register', verifyToken, async (req, res, next) => {
    try {
      const { fileId, originalName, sizeBytes, shardCount, cipher, metadata, encryptedAESKey } = req.body;
      if (!originalName || !Number.isFinite(Number(sizeBytes)) || Number(sizeBytes) < 0) {
        return res.status(400).json({ error: 'originalName and sizeBytes are required' });
      }

      const normalizedShardCount = parsePositiveInt(shardCount, 1);
      const actualFileId = fileId || uuidv4();

      const existing = await FileModel.findOne({ fileId: actualFileId }).lean();
      if (existing?.userId && existing.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      const nextMetadata = metadata === undefined ? existing?.metadata || {} : metadata;
      const update = {
        fileId: actualFileId,
        originalName,
        sizeBytes: Number(sizeBytes),
        shardCount: normalizedShardCount,
        cipher: cipher || 'aes-256-gcm',
        metadata: nextMetadata
      };

      if (!existing?.userId) {
        update.userId = req.userId;
      }

      if (typeof encryptedAESKey === 'string' && encryptedAESKey.trim()) {
        update.encryptedAESKey = encryptedAESKey.trim();
      }

      const file = await FileModel.findOneAndUpdate(
        { fileId: actualFileId },
        {
          $set: update
        },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );

      res.json({ fileId: file.fileId, shardCount: file.shardCount });
    } catch (error) {
      next(error);
    }
  });

  router.post('/files/:fileId/store-key', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const encryptedAESKey = String(req.body?.encryptedAESKey || '').trim();

      if (!fileId) {
        return res.status(400).json({ error: 'fileId is required' });
      }
      if (!encryptedAESKey) {
        return res.status(400).json({ error: 'encryptedAESKey is required' });
      }

      const file = await FileModel.findOne({ fileId });
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      await FileModel.findOneAndUpdate(
        { fileId },
        {
          $set: {
            encryptedAESKey
          }
        },
        { new: true }
      );

      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  router.get('/files/:fileId/key', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;

      if (!fileId) {
        return res.status(400).json({ error: 'fileId is required' });
      }

      const file = await FileModel.findOne({ fileId }).lean();
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }
      if (!file.encryptedAESKey) {
        return res.status(404).json({ error: 'encrypted key not found' });
      }

      res.json({ encryptedAESKey: file.encryptedAESKey });
    } catch (error) {
      next(error);
    }
  });

  router.post('/files/:fileId/filecoin', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const { filecoinCid, filecoinBackedUp } = req.body;

      if (!fileId) {
        return res.status(400).json({ error: 'fileId is required' });
      }
      if (!filecoinCid) {
        return res.status(400).json({ error: 'filecoinCid is required' });
      }

      const file = await FileModel.findOne({ fileId });
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId && file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      const updated = await FileModel.findOneAndUpdate(
        { fileId },
        {
          $set: {
            filecoinCid,
            filecoinBackedUp: typeof filecoinBackedUp === 'boolean' ? filecoinBackedUp : true
          }
        },
        { new: true }
      );

      res.json({ ok: true, fileId: updated.fileId, filecoinCid: updated.filecoinCid });
    } catch (error) {
      next(error);
    }
  });

  router.post('/files/:fileId/filecoin/upload', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const { dataBase64 } = req.body;

      if (!fileId) {
        return res.status(400).json({ error: 'fileId is required' });
      }
      if (!dataBase64 || typeof dataBase64 !== 'string') {
        return res.status(400).json({ error: 'dataBase64 is required' });
      }

      const payload = Buffer.from(dataBase64, 'base64');
      if (!payload || payload.length === 0) {
        return res.status(400).json({ error: 'dataBase64 decoded to empty payload' });
      }

      const cid = await uploadToFilecoin(payload, fileId);
      if (!cid) {
        return res.status(502).json({ error: 'filecoin upload failed' });
      }

      const file = await FileModel.findOne({ fileId });
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId && file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      const updated = await FileModel.findOneAndUpdate(
        { fileId },
        {
          $set: {
            filecoinCid: cid,
            filecoinBackedUp: true
          }
        },
        { new: true }
      );

      res.json({ ok: true, fileId: updated.fileId, filecoinCid: cid });
    } catch (error) {
      next(error);
    }
  });

  router.post('/files/:fileId/filecoin/message', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const { filecoinCid, nodeId, sourceNodeId } = req.body;

      if (!fileId) {
        return res.status(400).json({ error: 'fileId is required' });
      }
      if (!filecoinCid || typeof filecoinCid !== 'string') {
        return res.status(400).json({ error: 'filecoinCid is required' });
      }
      if (!nodeId || typeof nodeId !== 'string') {
        return res.status(400).json({ error: 'nodeId is required' });
      }

      const file = await FileModel.findOne({ fileId }).lean();
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId && file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      const allowedNodeIds = new Set();
      if (sourceNodeId && typeof sourceNodeId === 'string') {
        allowedNodeIds.add(sourceNodeId);
      }

      const placementRows = await ShardPlacementModel.find({ fileId, status: 'available' }).select('nodeId').lean();
      for (const placement of placementRows) {
        allowedNodeIds.add(placement.nodeId);
      }

      if (!allowedNodeIds.has(nodeId)) {
        return res.status(403).json({ error: 'node is not allowed to report this file' });
      }

      await recordFilecoinBackupMessage({
        fileId,
        filecoinCid,
        reportedByNodeId: nodeId,
        sourceNodeId: sourceNodeId || nodeId
      });

      res.json({ ok: true, fileId, filecoinCid, nodeId });
    } catch (error) {
      next(error);
    }
  });

  router.post('/files/:fileId/filecoin/queue', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;

      if (!fileId) {
        return res.status(400).json({ error: 'fileId is required' });
      }

      const file = await FileModel.findOne({ fileId });
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId && file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      if (file.filecoinBackedUp && file.filecoinCid) {
        return res.json({ ok: true, fileId, queued: false, filecoinCid: file.filecoinCid });
      }

      const job = await queueFilecoinBackupJob(fileId);
      void processPendingFilecoinBackupJobs(1).catch((error) => {
        console.error('[filecoin-backup] immediate queue kick failed', error);
      });

      res.json({ ok: true, fileId, queued: true, jobId: job._id.toString() });
    } catch (error) {
      next(error);
    }
  });

  router.post('/shards/register', async (req, res, next) => {
    try {
      const { shardId, fileId, order, sizeBytes, checksum, nodeIds } = req.body;
      if (!shardId || !fileId || !checksum) {
        return res.status(400).json({ error: 'shardId, fileId, and checksum are required' });
      }

      if (nodeIds !== undefined && !Array.isArray(nodeIds)) {
        return res.status(400).json({ error: 'nodeIds must be an array when provided' });
      }

      await ShardModel.findOneAndUpdate(
        { shardId },
        {
          $set: {
            shardId,
            fileId,
            order: Number(order) || 0,
            sizeBytes: Number(sizeBytes) || 0,
            checksum
          }
        },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );

      const uniqueNodeIds = [...new Set(Array.isArray(nodeIds) ? nodeIds : [])];
      for (const nodeId of uniqueNodeIds) {
        await ShardPlacementModel.updateOne(
          { shardId, nodeId },
          {
            $set: {
              fileId,
              status: 'available'
            },
            $setOnInsert: {
              shardId,
              nodeId
            }
          },
          { upsert: true }
        );
      }

      if (uniqueNodeIds.length > 0) {
        await ensureEmergencyReplicasForShard(shardId, config.emergencyReplicaFloor);
      }

      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  router.post('/shards/placement-plan', verifyToken, async (req, res, next) => {
    try {
      const { shardId, sizeBytes, replicas } = req.body;
      if (!shardId || !Number.isFinite(Number(sizeBytes)) || Number(sizeBytes) < 0) {
        return res.status(400).json({ error: 'shardId and sizeBytes are required' });
      }

      const replicaCount = parsePositiveInt(replicas, config.minReplicas);
      const nodes = await chooseDistinctOnlineNodes(replicaCount, Number(sizeBytes));

      res.json({
        shardId,
        replicas: nodes.map((node) => ({
          nodeId: node.nodeId,
          url: node.url,
          nodeSecret: node.nodeSecret || null
        }))
      });
    } catch (error) {
      if (String(error.message).startsWith('insufficient_online_nodes')) {
        return res.status(409).json({ error: error.message });
      }
      next(error);
    }
  });

  router.delete('/files/:fileId', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const file = await FileModel.findOne({ fileId }).lean();
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId && file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      const shards = await ShardModel.find({ fileId }).select('shardId').lean();
      const shardIds = shards.map((shard) => shard.shardId);

      const placements = await ShardPlacementModel.find({ fileId }).lean();
      const nodeIds = [...new Set(placements.map((placement) => placement.nodeId))];

      const onlineNodes = await NodeModel.find({ nodeId: { $in: nodeIds }, status: 'online' })
        .select('nodeId url nodeSecret')
        .lean();
      const nodeUrlMap = new Map(onlineNodes.map((node) => [node.nodeId, { url: node.url, nodeSecret: node.nodeSecret || null }]));

      const shardDeleteFailures = [];
      let deleteAttempts = 0;
      let deleteSuccesses = 0;
      let deleteSkippedOffline = 0;
      for (const placement of placements) {
        const nodeInfo = nodeUrlMap.get(placement.nodeId);
        if (!nodeInfo?.url) {
          deleteSkippedOffline += 1;
          continue;
        }

        const deleteUrl = `${normalizeUrl(nodeInfo.url)}/shards/${placement.shardId}`;
        deleteAttempts += 1;
        try {
          await axios.delete(deleteUrl, {
            timeout: 15000,
            headers: {
              Authorization: `Bearer ${nodeInfo.nodeSecret}`
            }
          });
          deleteSuccesses += 1;
        } catch (error) {
          shardDeleteFailures.push({
            shardId: placement.shardId,
            nodeId: placement.nodeId,
            error: error.response?.data?.error || error.message
          });
        }
      }

      await ReplicationTaskModel.deleteMany({
        $or: [
          { fileId },
          { shardId: { $in: shardIds } }
        ]
      });
      await RelayTaskModel.deleteMany({
        $or: [
          { fileId },
          { shardId: { $in: shardIds } }
        ]
      });
      await ShardPlacementModel.deleteMany({ fileId });
      await ShardModel.deleteMany({ fileId });
      await FileModel.deleteOne({ fileId });

      res.json({
        ok: true,
        fileId,
        deletedShards: shardIds.length,
        deletedPlacements: placements.length,
        donorDeleteAttempts: deleteAttempts,
        donorDeleteSuccesses: deleteSuccesses,
        donorDeleteSkippedOffline: deleteSkippedOffline,
        shardDeleteFailures
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/files/:fileId/manifest', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const file = await FileModel.findOne({ fileId }).lean();
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId && file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      const shards = await ShardModel.find({ fileId }).sort({ order: 1 }).lean();
      const placements = await ShardPlacementModel.find({ fileId, status: 'available' }).lean();

      const nodeIds = [...new Set(placements.map((placement) => placement.nodeId))];
      const nodes = await NodeModel.find({ nodeId: { $in: nodeIds }, status: 'online' })
        .select('nodeId url nodeSecret')
        .lean();
      const nodeMap = new Map(nodes.map((node) => [node.nodeId, { url: node.url, nodeSecret: node.nodeSecret || null }]));

      const shardManifests = shards.map((shard) => {
        const shardPlacements = placements
          .filter((placement) => placement.shardId === shard.shardId)
          .map((placement) => ({
            nodeId: placement.nodeId,
            url: nodeMap.get(placement.nodeId)?.url,
            nodeSecret: nodeMap.get(placement.nodeId)?.nodeSecret || null
          }))
          .filter((placement) => Boolean(placement.url));

        return {
          shardId: shard.shardId,
          order: shard.order,
          sizeBytes: shard.sizeBytes,
          checksum: shard.checksum,
          replicas: shardPlacements
        };
      });

      res.json({
        file: {
          fileId: file.fileId,
          originalName: file.originalName,
          sizeBytes: file.sizeBytes,
          shardCount: file.shardCount,
          cipher: file.cipher,
          userId: file.userId || null,
          encryptedAESKey: file.encryptedAESKey || null,
          metadata: file.metadata,
          filecoinCid: file.filecoinCid || null,
          filecoinBackedUp: Boolean(file.filecoinBackedUp)
        },
        shards: shardManifests
      });
    } catch (error) {
      next(error);
    }
  });

  // Download assembled file (Filecoin-backed) when no donor shards available.
  // Streams the decoded payload from Filecoin to the client.
  router.get('/files/:fileId/download', verifyToken, async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const file = await FileModel.findOne({ fileId }).lean();
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }
      if (file.userId && file.userId !== req.userId) {
        return res.status(403).json({ error: 'Forbidden' });
      }

      const shardCount = await ShardModel.countDocuments({ fileId });
      if (shardCount > 0) {
        return res.status(409).json({ error: 'file has donor shards; use shard download flow (manifest + replicas)' });
      }

      if (!file.filecoinCid) {
        return res.status(404).json({ error: 'no filecoin backup available' });
      }

      // retrieve from Filecoin (may take some time)
      const payload = await retrieveFromFilecoin(file.filecoinCid);
      if (!payload) {
        return res.status(502).json({ error: 'filecoin retrieval failed' });
      }

      const name = file.originalName || 'download.bin';
      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('Content-Length', String(payload.length));
      res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(name)}"`);
      res.send(payload);
    } catch (error) {
      next(error);
    }
  });

  return router;
}

module.exports = {
  buildRoutes
};
