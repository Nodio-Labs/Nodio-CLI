const {
  NodeModel,
  ShardModel,
  ShardPlacementModel,
  ReplicationTaskModel
} = require('./models');

async function chooseDistinctOnlineNodes(requiredCount, minFreeBytes = 0, excludedNodeIds = []) {
  const excluded = new Set(excludedNodeIds);
  const now = new Date();

  const candidates = await NodeModel.find({
    status: 'online',
    $or: [
      { filecoinUploadLockUntil: null },
      { filecoinUploadLockUntil: { $lte: now } }
    ],
    freeBytes: { $gte: minFreeBytes },
    nodeId: { $nin: [...excluded] }
  })
    .sort({ freeBytes: -1, lastHeartbeatAt: -1 })
    .lean();

  if (candidates.length < requiredCount) {
    throw new Error(`insufficient_online_nodes: required ${requiredCount}, found ${candidates.length}`);
  }

  return candidates.slice(0, requiredCount);
}

async function ensureEmergencyReplicasForShard(shardId, emergencyReplicaFloor) {
  const shard = await ShardModel.findOne({ shardId }).lean();
  if (!shard) {
    return;
  }

  const existingPlacements = await ShardPlacementModel.find({
    shardId,
    status: 'available'
  }).lean();

  const onlineMap = new Map();
  const onlineNodes = await NodeModel.find({ status: 'online' }).lean();
  for (const node of onlineNodes) {
    onlineMap.set(node.nodeId, node);
  }

  const livePlacements = existingPlacements.filter((p) => onlineMap.has(p.nodeId));
  const currentReplicaCount = livePlacements.length;

  if (currentReplicaCount >= emergencyReplicaFloor) {
    return;
  }

  const sourcePlacement = livePlacements[0];
  if (!sourcePlacement) {
    return;
  }

  const missing = emergencyReplicaFloor - currentReplicaCount;
  const excluded = livePlacements.map((p) => p.nodeId);
  const targets = await chooseDistinctOnlineNodes(missing, shard.sizeBytes, excluded);

  for (const target of targets) {
    await ReplicationTaskModel.updateOne(
      {
        shardId,
        targetNodeId: target.nodeId,
        status: { $in: ['pending', 'in_progress'] }
      },
      {
        $setOnInsert: {
          shardId,
          fileId: shard.fileId,
          sourceNodeId: sourcePlacement.nodeId,
          targetNodeId: target.nodeId,
          status: 'pending',
          attempts: 0
        }
      },
      { upsert: true }
    );
  }
}

async function markNodeOfflineAndRecover(nodeId, emergencyReplicaFloor) {
  const node = await NodeModel.findOne({ nodeId });
  if (!node || node.status === 'offline') {
    return false;
  }

  node.status = 'offline';
  await node.save();

  await ShardPlacementModel.updateMany(
    { nodeId, status: 'available' },
    { $set: { status: 'lost' } }
  );

  const affectedShardRows = await ShardPlacementModel.find({ nodeId }).select('shardId').lean();
  const shardIds = [...new Set(affectedShardRows.map((row) => row.shardId))];

  for (const shardId of shardIds) {
    await ensureEmergencyReplicasForShard(shardId, emergencyReplicaFloor);
  }

  return true;
}

module.exports = {
  chooseDistinctOnlineNodes,
  ensureEmergencyReplicasForShard,
  markNodeOfflineAndRecover
};
