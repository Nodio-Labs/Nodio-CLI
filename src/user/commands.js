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

async function uploadFile(options) {
  const filePath = path.resolve(options.file);
  const serverUrl = options.server;
  const shardSizeMb = Number(options.shardSizeMb || 1);
  const replicas = Number(options.replicas || 5);

  if (!Number.isFinite(shardSizeMb) || shardSizeMb <= 0) {
    throw new Error('shard-size-mb must be greater than 0');
  }
  if (!Number.isInteger(replicas) || replicas < 5) {
    throw new Error('replicas must be an integer >= 5');
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

    for (const replica of plannedReplicas) {
      const putUrl = `${normalizeUrl(replica.url)}/shards/${shardId}`;
      await axios.put(putUrl, encrypted.cipherText, {
        headers: { 'Content-Type': 'application/octet-stream' },
        timeout: 30000
      });
    }

    await api.post('/shards/register', {
      shardId,
      fileId,
      order,
      sizeBytes: encrypted.cipherText.length,
      checksum,
      nodeIds: plannedReplicas.map((replica) => replica.nodeId)
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
    for (const replica of shard.replicas || []) {
      try {
        const getUrl = `${normalizeUrl(replica.url)}/shards/${shard.shardId}`;
        const response = await axios.get(getUrl, {
          responseType: 'arraybuffer',
          timeout: 30000
        });
        const candidate = Buffer.from(response.data);
        if (sha256Hex(candidate) !== shard.checksum) {
          continue;
        }
        encryptedBuffer = candidate;
        break;
      } catch {
        continue;
      }
    }

    if (!encryptedBuffer) {
      throw new Error(`failed to fetch valid replica for shard ${shard.shardId}`);
    }

    const plain = decryptAes256Gcm(encryptedBuffer, keyBuffer, shardMeta.iv, shardMeta.authTag);
    plainParts.push(plain);
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
  console.log(`deletedPlacements: ${payload.deletedPlacements || 0}`);
  console.log(`shardDeleteFailures: ${(payload.shardDeleteFailures || []).length}`);
}

module.exports = {
  uploadFile,
  downloadFile,
  deleteFile
};
