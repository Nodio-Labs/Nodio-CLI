const fs = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const { createApiClient, normalizeUrl } = require('./client');
const { encryptAes256Gcm, decryptAes256Gcm, sha256Hex } = require('../common/crypto');

function splitBuffer(buffer, shardSizeBytes) {
  if (shardSizeBytes <= 0) {
    throw new Error('shard size must be greater than zero');
  }

  const chunks = [];
  for (let offset = 0; offset < buffer.length; offset += shardSizeBytes) {
    chunks.push(buffer.subarray(offset, Math.min(offset + shardSizeBytes, buffer.length)));
  }

  return chunks.length > 0 ? chunks : [Buffer.alloc(0)];
}

function parseAesKey(keyBase64) {
  const key = Buffer.from(keyBase64, 'base64');
  if (key.length !== 32) {
    throw new Error('key must decode to exactly 32 bytes for AES-256');
  }
  return key;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function relayStoreShard(api, { shardId, fileId, nodeIds, dataBuffer, timeoutMs = 45000, pollMs = 200 }) {
  const opId = uuidv4();
  await api.post('/relay/shards/store', {
    opId,
    shardId,
    fileId,
    nodeIds,
    dataBase64: dataBuffer.toString('base64')
  });

  // Alert server that relay tasks are pending so donors check urgently
  await Promise.allSettled(nodeIds.map((nodeId) => api.post(`/nodes/${nodeId}/alert-relay-pending`)));

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const response = await api.get(`/relay/shards/store/${opId}`);
    const payload = response.data || {};
    if (Number(payload.pendingCount || 0) === 0) {
      return {
        successfulNodeIds: payload.successfulNodeIds || [],
        failed: payload.failed || []
      };
    }
    // Polls every 500ms; with 10s heartbeat, donor should respond within 10s+network latency
    // eslint-disable-next-line no-await-in-loop
    await sleep(pollMs);
  }

  throw new Error(`relay store timed out for shard ${shardId}`);
}

async function relayFetchShard(api, { shardId, nodeIds, timeoutMs = 45000, pollMs = 200 }) {
  const opId = uuidv4();
  await api.post('/relay/shards/fetch', {
    opId,
    shardId,
    nodeIds
  });

  // Alert server that relay tasks are pending so donors check urgently
  await Promise.allSettled(nodeIds.map((nodeId) => api.post(`/nodes/${nodeId}/alert-relay-pending`)));

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const response = await api.get(`/relay/shards/fetch/${opId}`);
    const payload = response.data || {};
    if (payload.hasResult && payload.resultDataBase64) {
      return {
        nodeId: payload.nodeId,
        data: Buffer.from(payload.resultDataBase64, 'base64'),
        failed: payload.failed || []
      };
    }

    if (Number(payload.pendingCount || 0) === 0) {
      return {
        nodeId: null,
        data: null,
        failed: payload.failed || []
      };
    }

    // Wait for donor heartbeats to pull relay tasks.
    // eslint-disable-next-line no-await-in-loop
    await sleep(pollMs);
  }

  throw new Error(`relay fetch timed out for shard ${shardId}`);
}

async function uploadFile(options) {
  const filePath = path.resolve(options.file);
  const serverUrl = options.server;
  const shardSizeMb = Number(options.shardSizeMb || 1);
  const replicas = Number(options.replicas || 5);
  const directTimeoutMs = Number(options.directTimeoutMs || 1200);
  const relayFirst = Boolean(options.relayFirst);

  if (!Number.isFinite(shardSizeMb) || shardSizeMb <= 0) {
    throw new Error('shard-size-mb must be greater than 0');
  }
  if (!Number.isInteger(replicas) || replicas < 1) {
    throw new Error('replicas must be an integer >= 1');
  }
  if (!Number.isFinite(directTimeoutMs) || directTimeoutMs <= 0) {
    throw new Error('direct-timeout-ms must be greater than 0');
  }

  const shardSizeBytes = Math.floor(shardSizeMb * 1024 * 1024);
  const plainBuffer = await fs.readFile(filePath);
  const chunks = splitBuffer(plainBuffer, shardSizeBytes);

  const keyBuffer = options.keyBase64 ? parseAesKey(options.keyBase64) : crypto.randomBytes(32);
  const keyBase64 = keyBuffer.toString('base64');

  const api = createApiClient(serverUrl);
  const fileId = options.fileId || uuidv4();
  const originalName = path.basename(filePath);

  await api.post('/files/register', {
    fileId,
    originalName,
    sizeBytes: plainBuffer.length,
    shardCount: chunks.length,
    cipher: 'aes-256-gcm',
    metadata: {
      encryption: {
        algorithm: 'aes-256-gcm',
        shardSizeBytes,
        keyFormat: 'base64',
        shards: []
      }
    }
  });

  const encryptionShardMeta = [];

  for (let order = 0; order < chunks.length; order += 1) {
    const shardId = `${fileId}-shard-${order}-${uuidv4().slice(0, 8)}`;
    const encrypted = encryptAes256Gcm(chunks[order], keyBuffer);
    const checksum = sha256Hex(encrypted.cipherText);

    const placementResponse = await api.post('/shards/placement-plan', {
      shardId,
      sizeBytes: encrypted.cipherText.length,
      replicas
    });

    const plannedReplicas = placementResponse.data.replicas || [];
    if (plannedReplicas.length < replicas) {
      throw new Error(`placement failed for ${shardId}: expected ${replicas} replicas`);
    }

    let successfulReplicas = [];
    let failedReplicas = [];

    if (relayFirst) {
      failedReplicas = plannedReplicas.map((replica) => ({
        nodeId: replica.nodeId,
        url: replica.url,
        error: 'direct write skipped (relay-first)'
      }));
    } else {
      const directWriteResults = await Promise.allSettled(
        plannedReplicas.map(async (replica) => {
          const putUrl = `${normalizeUrl(replica.url)}/shards/${shardId}`;
          await axios.put(putUrl, encrypted.cipherText, {
            headers: { 'Content-Type': 'application/octet-stream' },
            timeout: directTimeoutMs
          });
          return replica;
        })
      );

      successfulReplicas = directWriteResults
        .filter((result) => result.status === 'fulfilled')
        .map((result) => result.value);

      failedReplicas = directWriteResults
        .map((result, index) => ({ result, replica: plannedReplicas[index] }))
        .filter((entry) => entry.result.status === 'rejected')
        .map((entry) => ({
          nodeId: entry.replica.nodeId,
          url: entry.replica.url,
          error: entry.result.reason?.message || 'direct write failed'
        }));
    }

    if (failedReplicas.length > 0) {
      try {
        const relayResult = await relayStoreShard(api, {
          shardId,
          fileId,
          nodeIds: failedReplicas.map((replica) => replica.nodeId),
          dataBuffer: encrypted.cipherText
        });

        const promotedRelayReplicas = plannedReplicas.filter((replica) => relayResult.successfulNodeIds.includes(replica.nodeId));
        successfulReplicas.push(...promotedRelayReplicas.filter((replica) => !successfulReplicas.some((item) => item.nodeId === replica.nodeId)));
      } catch (relayError) {
        console.warn(`[upload] relay store failed for shard ${shardId}: ${relayError.message}`);
      }
    }

    if (successfulReplicas.length === 0) {
      throw new Error(`failed to write shard ${shardId} to any replica: ${failedReplicas.map((r) => `${r.url} (${r.error})`).join(', ')}`);
    }

    if (failedReplicas.length > 0) {
      console.warn(`[upload] shard ${shardId} failed on ${failedReplicas.length} direct replica(s), final successes=${successfulReplicas.length}`);
    }

    await api.post('/shards/register', {
      shardId,
      fileId,
      order,
      sizeBytes: encrypted.cipherText.length,
      checksum,
      nodeIds: successfulReplicas.map((replica) => replica.nodeId)
    });

    encryptionShardMeta.push({
      shardId,
      order,
      iv: encrypted.iv,
      authTag: encrypted.authTag
    });
  }

  await api.post('/files/register', {
    fileId,
    originalName,
    sizeBytes: plainBuffer.length,
    shardCount: chunks.length,
    cipher: 'aes-256-gcm',
    metadata: {
      encryption: {
        algorithm: 'aes-256-gcm',
        shardSizeBytes,
        keyFormat: 'base64',
        shards: encryptionShardMeta
      }
    }
  });

  console.log(`Upload complete`);
  console.log(`fileId: ${fileId}`);
  console.log(`originalName: ${originalName}`);
  console.log(`sizeBytes: ${plainBuffer.length}`);
  console.log(`shardCount: ${chunks.length}`);
  console.log(`replicasPerShard: ${replicas}`);
  console.log(`aes256KeyBase64: ${keyBase64}`);
}

async function downloadFile(options) {
  const fileId = options.fileId;
  const serverUrl = options.server;
  const output = options.output ? path.resolve(options.output) : path.resolve(`./${fileId}.downloaded`);
  const keyBuffer = parseAesKey(options.keyBase64);
  const directTimeoutMs = Number(options.directTimeoutMs || 1200);
  const relayFirst = Boolean(options.relayFirst);

  if (!Number.isFinite(directTimeoutMs) || directTimeoutMs <= 0) {
    throw new Error('direct-timeout-ms must be greater than 0');
  }

  const api = createApiClient(serverUrl);
  const manifestResponse = await api.get(`/files/${fileId}/manifest`);
  const manifest = manifestResponse.data;
  const metadata = manifest.file?.metadata || {};
  const encryption = metadata.encryption;

  if (!encryption || encryption.algorithm !== 'aes-256-gcm') {
    throw new Error('missing or unsupported encryption metadata');
  }

  const shardMetaMap = new Map((encryption.shards || []).map((entry) => [entry.shardId, entry]));

  const orderedShards = [...(manifest.shards || [])].sort((a, b) => a.order - b.order);
  const plainParts = [];

  for (const shard of orderedShards) {
    const shardMeta = shardMetaMap.get(shard.shardId);
    if (!shardMeta) {
      throw new Error(`missing encryption metadata for shard ${shard.shardId}`);
    }

    let encryptedBuffer = null;
    if (!relayFirst) {
      const directFetchResults = await Promise.allSettled(
        (shard.replicas || []).map(async (replica) => {
          const getUrl = `${normalizeUrl(replica.url)}/shards/${shard.shardId}`;
          const response = await axios.get(getUrl, {
            responseType: 'arraybuffer',
            timeout: directTimeoutMs
          });
          const candidate = Buffer.from(response.data);
          if (sha256Hex(candidate) !== shard.checksum) {
            throw new Error('checksum mismatch');
          }
          return candidate;
        })
      );

      const successfulDirectFetch = directFetchResults.find((result) => result.status === 'fulfilled');
      if (successfulDirectFetch) {
        encryptedBuffer = successfulDirectFetch.value;
      }
    }

    if (!encryptedBuffer && Array.isArray(shard.replicas) && shard.replicas.length > 0) {
      try {
        const relayResult = await relayFetchShard(api, {
          shardId: shard.shardId,
          nodeIds: shard.replicas.map((replica) => replica.nodeId)
        });

        if (relayResult.data && sha256Hex(relayResult.data) === shard.checksum) {
          encryptedBuffer = relayResult.data;
        }
      } catch (relayError) {
        console.warn(`[download] relay fetch failed for shard ${shard.shardId}: ${relayError.message}`);
      }
    }

    if (!encryptedBuffer) {
      throw new Error(`failed to fetch valid replica for shard ${shard.shardId}`);
    }

    try {
      const plain = decryptAes256Gcm(encryptedBuffer, keyBuffer, shardMeta.iv, shardMeta.authTag);
      plainParts.push(plain);
    } catch (error) {
      if (String(error.message || '').includes('unsupported state or unable to authenticate data')) {
        throw new Error(
          `decryption failed for shard ${shard.shardId}: key is incorrect or metadata/key mismatch (double-check key-base64 copy)`
        );
      }
      throw error;
    }
  }

  const reconstructed = Buffer.concat(plainParts);
  await fs.mkdir(path.dirname(output), { recursive: true });
  await fs.writeFile(output, reconstructed);

  console.log(`Download complete`);
  console.log(`fileId: ${fileId}`);
  console.log(`output: ${output}`);
  console.log(`sizeBytes: ${reconstructed.length}`);
}

async function deleteFile(options) {
  const fileId = options.fileId;
  const serverUrl = options.server;

  const api = createApiClient(serverUrl);
  const response = await api.delete(`/files/${fileId}`);
  const payload = response.data || {};

  console.log('Delete complete');
  console.log(`fileId: ${payload.fileId || fileId}`);
  console.log(`deletedShards: ${payload.deletedShards || 0}`);
  console.log(`deletedPlacementsMetadata: ${payload.deletedPlacements || 0}`);
  console.log(`donorDeleteAttempts: ${payload.donorDeleteAttempts || 0}`);
  console.log(`donorDeleteSuccesses: ${payload.donorDeleteSuccesses || 0}`);
  console.log(`donorDeleteSkippedOffline: ${payload.donorDeleteSkippedOffline || 0}`);
  console.log(`shardDeleteFailures: ${(payload.shardDeleteFailures || []).length}`);
}

module.exports = {
  uploadFile,
  downloadFile,
  deleteFile
};
