const express = require('express');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const {
  NodeModel,
  FileModel,
  ShardModel,
  ShardPlacementModel,
  ReplicationTaskModel
} = require('./models');
const {
  chooseDistinctOnlineNodes,
  ensureEmergencyReplicasForShard
} = require('./services');

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function normalizeUrl(url) {
  return String(url || '').replace(/\/+$/, '');
}

function buildRoutes(config) {
  const router = express.Router();

  router.get('/health', async (_req, res) => {
    res.json({ ok: true, service: 'nodio-server' });
  });

  router.post('/nodes/register', async (req, res, next) => {
    try {
      const { nodeId, deviceKey, nodeKey, url, capacityBytes, freeBytes } = req.body;

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

      let existingByNodeKey = null;
      if (nodeKey) {
        existingByNodeKey = await NodeModel.findOne({ nodeKey }).lean();
      }

      if (existingByNodeKey && nodeId && nodeId !== existingByNodeKey.nodeId) {
        return res.status(409).json({ error: 'nodeKey is already associated with a different nodeId' });
      }

      const effectiveNodeId = nodeId || existingByNodeKey?.nodeId || `donor-${uuidv4().slice(0, 8)}`;

      const node = await NodeModel.findOneAndUpdate(
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

      res.json({
        nodeId: node.nodeId,
        status: node.status,
        heartbeatIntervalMs: config.heartbeatIntervalMs,
        minReplicas: config.minReplicas,
        emergencyReplicaFloor: config.emergencyReplicaFloor
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

      const tasksWithSourceUrl = [];
      for (const task of pendingTasks) {
        const source = await NodeModel.findOne({ nodeId: task.sourceNodeId, status: 'online' })
          .select('url nodeId')
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
          sourceUrl: source.url
        });
      }

      res.json({
        ok: true,
        now: new Date().toISOString(),
        replicationTasks: tasksWithSourceUrl
      });
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

  router.post('/files/register', async (req, res, next) => {
    try {
      const { fileId, originalName, sizeBytes, shardCount, cipher, metadata } = req.body;
      if (!originalName || !Number.isFinite(Number(sizeBytes)) || Number(sizeBytes) < 0) {
        return res.status(400).json({ error: 'originalName and sizeBytes are required' });
      }

      const normalizedShardCount = parsePositiveInt(shardCount, 1);
      const actualFileId = fileId || uuidv4();

      const file = await FileModel.findOneAndUpdate(
        { fileId: actualFileId },
        {
          $set: {
            fileId: actualFileId,
            originalName,
            sizeBytes: Number(sizeBytes),
            shardCount: normalizedShardCount,
            cipher: cipher || 'aes-256-gcm',
            metadata: metadata || {}
          }
        },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );

      res.json({ fileId: file.fileId, shardCount: file.shardCount });
    } catch (error) {
      next(error);
    }
  });

  router.post('/shards/register', async (req, res, next) => {
    try {
      const { shardId, fileId, order, sizeBytes, checksum, nodeIds } = req.body;
      if (!shardId || !fileId || !checksum || !Array.isArray(nodeIds) || nodeIds.length === 0) {
        return res.status(400).json({ error: 'shardId, fileId, checksum and nodeIds are required' });
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

      const uniqueNodeIds = [...new Set(nodeIds)];
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

      await ensureEmergencyReplicasForShard(shardId, config.emergencyReplicaFloor);

      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  router.post('/shards/placement-plan', async (req, res, next) => {
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
          url: node.url
        }))
      });
    } catch (error) {
      if (String(error.message).startsWith('insufficient_online_nodes')) {
        return res.status(409).json({ error: error.message });
      }
      next(error);
    }
  });

  router.delete('/files/:fileId', async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const file = await FileModel.findOne({ fileId }).lean();
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }

      const shards = await ShardModel.find({ fileId }).select('shardId').lean();
      const shardIds = shards.map((shard) => shard.shardId);

      const placements = await ShardPlacementModel.find({ fileId }).lean();
      const nodeIds = [...new Set(placements.map((placement) => placement.nodeId))];

      const onlineNodes = await NodeModel.find({ nodeId: { $in: nodeIds }, status: 'online' })
        .select('nodeId url')
        .lean();
      const nodeUrlMap = new Map(onlineNodes.map((node) => [node.nodeId, node.url]));

      const shardDeleteFailures = [];
      let deleteAttempts = 0;
      let deleteSuccesses = 0;
      let deleteSkippedOffline = 0;
      for (const placement of placements) {
        const nodeUrl = nodeUrlMap.get(placement.nodeId);
        if (!nodeUrl) {
          deleteSkippedOffline += 1;
          continue;
        }

        const deleteUrl = `${normalizeUrl(nodeUrl)}/shards/${placement.shardId}`;
        deleteAttempts += 1;
        try {
          await axios.delete(deleteUrl, { timeout: 15000 });
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

  router.get('/files/:fileId/manifest', async (req, res, next) => {
    try {
      const { fileId } = req.params;
      const file = await FileModel.findOne({ fileId }).lean();
      if (!file) {
        return res.status(404).json({ error: 'file not found' });
      }

      const shards = await ShardModel.find({ fileId }).sort({ order: 1 }).lean();
      const placements = await ShardPlacementModel.find({ fileId, status: 'available' }).lean();

      const nodeIds = [...new Set(placements.map((placement) => placement.nodeId))];
      const nodes = await NodeModel.find({ nodeId: { $in: nodeIds }, status: 'online' })
        .select('nodeId url')
        .lean();
      const nodeMap = new Map(nodes.map((node) => [node.nodeId, node.url]));

      const shardManifests = shards.map((shard) => {
        const shardPlacements = placements
          .filter((placement) => placement.shardId === shard.shardId)
          .map((placement) => ({
            nodeId: placement.nodeId,
            url: nodeMap.get(placement.nodeId)
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
          metadata: file.metadata
        },
        shards: shardManifests
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}

module.exports = {
  buildRoutes
};
