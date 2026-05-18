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

async function queueFilecoinBackupJob(fileId) {
  return FilecoinBackupJobModel.findOneAndUpdate(
    { fileId },
    {
      $set: {
        status: 'pending',
        attempts: 0,
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
    timeout: 15000
  });

  return Buffer.from(response.data);
}

async function buildEncryptedFileBuffer(fileId) {
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
    const placements = await ShardPlacementModel.find({ shardId: shard.shardId, status: 'available' }).lean();
    if (placements.length === 0) {
      throw new Error(`no private-node placements found for shard ${shard.shardId}`);
    }

    const candidateNodeIds = [...new Set(placements.map((placement) => placement.nodeId))];
    const onlineNodes = await NodeModel.find({ nodeId: { $in: candidateNodeIds }, status: 'online' })
      .select('nodeId url')
      .lean();

    const onlineNodeMap = new Map(onlineNodes.map((node) => [node.nodeId, node]));
    let shardBuffer = null;

    for (const placement of placements) {
      const node = onlineNodeMap.get(placement.nodeId);
      if (!node) {
        continue;
      }

      try {
        // eslint-disable-next-line no-await-in-loop
        shardBuffer = await fetchShardFromNode(node, shard.shardId);
        break;
      } catch (error) {
        console.warn(
          `[filecoin-backup] fetch failed for shard ${shard.shardId} from ${node.nodeId}: ${error.message}`
        );
      }
    }

    if (!shardBuffer) {
      throw new Error(`unable to fetch shard ${shard.shardId} from private nodes`);
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

  const encryptedFileBuffer = await buildEncryptedFileBuffer(fileId);
  const filecoinCid = await uploadToFilecoin(encryptedFileBuffer, fileId);

  if (!filecoinCid) {
    throw new Error('filecoin upload failed');
  }

  await FileModel.findOneAndUpdate(
    { fileId },
    {
      $set: {
        filecoinCid,
        filecoinBackedUp: true
      }
    },
    { new: true }
  );

  await FilecoinBackupJobModel.updateOne(
    { _id: job._id },
    {
      $set: {
        status: 'completed',
        filecoinCid,
        completedAt: new Date(),
        errorMessage: null
      }
    }
  );

  return { fileId, filecoinCid };
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
  processPendingFilecoinBackupJobs
};