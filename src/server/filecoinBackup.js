const axios = require('axios');
const {
  FileModel,
  ShardModel,
  ShardPlacementModel,
  NodeModel,
  FilecoinBackupJobModel
} = require('./models');
const { uploadToFilecoin } = require('../../services/filecoin');

function normalizeUrl(url) {
  return String(url || '').replace(/\/+$/, '');
}

function randomPick(items) {
  if (!Array.isArray(items) || items.length === 0) {
    return null;
  }

  const idx = Math.floor(Math.random() * items.length);
  return items[idx];
}

async function lockNodeForFilecoinUpload(nodeId, fileId, lockMs = 30 * 60 * 1000) {
  const lockUntil = new Date(Date.now() + lockMs);
  await NodeModel.updateOne(
    { nodeId },
    {
      $set: {
        filecoinUploadLockUntil: lockUntil,
        filecoinUploadLockFileId: fileId
      }
    }
  );

  return lockUntil;
}

async function unlockNodeForFilecoinUpload(nodeId, fileId) {
  await NodeModel.updateOne(
    { nodeId, filecoinUploadLockFileId: fileId },
    {
      $set: {
        filecoinUploadLockUntil: null,
        filecoinUploadLockFileId: null
      }
    }
  );
}

async function recordFilecoinBackupMessage({ fileId, filecoinCid, reportedByNodeId, sourceNodeId }) {
  const now = new Date();
  const file = await FileModel.findOne({ fileId });
  if (!file) {
    throw new Error('file not found');
  }

  const updated = await FileModel.findOneAndUpdate(
    { fileId },
    {
      $set: {
        filecoinCid,
        filecoinBackedUp: true,
        filecoinReportedByNodeId: reportedByNodeId || null,
        filecoinReportedAt: now
      }
    },
    { new: true }
  );

  await FilecoinBackupJobModel.updateOne(
    { fileId },
    {
      $set: {
        status: 'completed',
        sourceNodeId: sourceNodeId || null,
        reportedByNodeId: reportedByNodeId || null,
        reportedAt: now,
        filecoinCid,
        completedAt: now,
        errorMessage: null
      }
    }
  );

  return updated;
}

async function queueFilecoinBackupJob(fileId) {
  return FilecoinBackupJobModel.findOneAndUpdate(
    { fileId },
    {
      $set: {
        status: 'pending',
        attempts: 0,
        sourceNodeId: null,
        filecoinCid: null,
        errorMessage: null,
        lastAttemptAt: null,
        completedAt: null
      },
      $setOnInsert: {
        fileId
      }
    },
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );
}

async function claimPendingFilecoinBackupJob() {
  return FilecoinBackupJobModel.findOneAndUpdate(
    { status: 'pending' },
    {
      $set: {
        status: 'in_progress',
        lastAttemptAt: new Date(),
        errorMessage: null
      },
      $inc: { attempts: 1 }
    },
    { sort: { createdAt: 1 }, new: true }
  );
}

async function fetchShardFromNode(node, shardId) {
  const response = await axios.get(`${normalizeUrl(node.url)}/shards/${shardId}`, {
    responseType: 'arraybuffer',
    timeout: 15000,
    headers: {
      Authorization: `Bearer ${node.nodeSecret}`
    }
  });

  return Buffer.from(response.data);
}

async function pickSingleSourceNodeForFile(fileId, shards) {
  const shardIds = shards.map((shard) => shard.shardId);
  const placements = await ShardPlacementModel.find({
    fileId,
    shardId: { $in: shardIds },
    status: 'available'
  }).lean();

  const placementMap = new Map();
  for (const placement of placements) {
    const count = placementMap.get(placement.nodeId) || 0;
    placementMap.set(placement.nodeId, count + 1);
  }

  const fullReplicaNodeIds = [...placementMap.entries()]
    .filter(([, count]) => count >= shards.length)
    .map(([nodeId]) => nodeId);

  if (fullReplicaNodeIds.length === 0) {
    throw new Error('no single private node has full file replica set');
  }

  const now = new Date();
  const candidates = await NodeModel.find({
    nodeId: { $in: fullReplicaNodeIds },
    status: 'online',
    $or: [
      { filecoinUploadLockUntil: null },
      { filecoinUploadLockUntil: { $lte: now } }
    ]
  })
    .select('nodeId url nodeSecret')
    .lean();

  const selected = randomPick(candidates);
  if (!selected) {
    throw new Error('no online private node available to send file to filecoin');
  }

  return selected;
}

async function buildEncryptedFileBuffer(fileId, sourceNode) {
  const file = await FileModel.findOne({ fileId }).lean();
  if (!file) {
    throw new Error('file not found');
  }

  const shards = await ShardModel.find({ fileId }).sort({ order: 1 }).lean();
  if (shards.length === 0) {
    throw new Error('shards not found for file');
  }

  const shardBuffers = [];

  for (const shard of shards) {
    let shardBuffer = null;
    try {
      // eslint-disable-next-line no-await-in-loop
      shardBuffer = await fetchShardFromNode(sourceNode, shard.shardId);
    } catch (error) {
      throw new Error(
        `unable to fetch shard ${shard.shardId} from source node ${sourceNode.nodeId}: ${error.message}`
      );
    }

    shardBuffers.push(shardBuffer);
  }

  return Buffer.concat(shardBuffers);
}

async function processFilecoinBackupJob(job) {
  const fileId = job.fileId;
  const file = await FileModel.findOne({ fileId }).lean();

  if (!file) {
    throw new Error('file not found');
  }

  if (file.filecoinBackedUp && file.filecoinCid) {
    await FilecoinBackupJobModel.updateOne(
      { _id: job._id },
      {
        $set: {
          status: 'completed',
          filecoinCid: file.filecoinCid,
          completedAt: new Date(),
          errorMessage: null
        }
      }
    );

    return { fileId, filecoinCid: file.filecoinCid, alreadyBackedUp: true };
  }

  const shards = await ShardModel.find({ fileId }).sort({ order: 1 }).lean();
  if (shards.length === 0) {
    throw new Error('shards not found for file');
  }

  const sourceNode = await pickSingleSourceNodeForFile(fileId, shards);
  await lockNodeForFilecoinUpload(sourceNode.nodeId, fileId);
  await FilecoinBackupJobModel.updateOne(
    { _id: job._id },
    {
      $set: {
        sourceNodeId: sourceNode.nodeId
      }
    }
  );

  console.log(`[filecoin-backup] file ${fileId}: selected source node ${sourceNode.nodeId}`);
  console.log(`[filecoin-backup] file ${fileId}: sending to filecoin...`);

  try {
    const encryptedFileBuffer = await buildEncryptedFileBuffer(fileId, sourceNode);
    const filecoinCid = await uploadToFilecoin(encryptedFileBuffer, fileId);

    if (!filecoinCid) {
      throw new Error('filecoin upload failed');
    }

    console.log(`[filecoin-backup] file ${fileId}: reporting completion to central server`);
    await recordFilecoinBackupMessage({
      fileId,
      filecoinCid,
      reportedByNodeId: sourceNode.nodeId,
      sourceNodeId: sourceNode.nodeId
    });

    console.log(`[filecoin-backup] file ${fileId}: completed with cid ${filecoinCid}`);

    return { fileId, filecoinCid, sourceNodeId: sourceNode.nodeId };
  } finally {
    await unlockNodeForFilecoinUpload(sourceNode.nodeId, fileId);
  }
}

async function processPendingFilecoinBackupJobs(limit = 1) {
  const results = [];

  for (let index = 0; index < limit; index += 1) {
    const job = await claimPendingFilecoinBackupJob();
    if (!job) {
      break;
    }

    try {
      // eslint-disable-next-line no-await-in-loop
      results.push(await processFilecoinBackupJob(job));
    } catch (error) {
      await FilecoinBackupJobModel.updateOne(
        { _id: job._id },
        {
          $set: {
            status: 'failed',
            errorMessage: error.message || 'filecoin backup failed'
          }
        }
      );

      console.error(`[filecoin-backup] job failed for ${job.fileId}: ${error.message}`);
      results.push({ fileId: job.fileId, error: error.message });
    }
  }

  return results;
}

module.exports = {
  queueFilecoinBackupJob,
  processPendingFilecoinBackupJobs,
  recordFilecoinBackupMessage
};