const fs = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const axios = require('axios');
const readline = require('readline');
const { v4: uuidv4 } = require('uuid');
const { createApiClient, normalizeUrl } = require('./client');
const { encryptAes256Gcm, decryptAes256Gcm, sha256Hex } = require('../common/crypto');
const { retrieveFromFilecoin } = require('../../services/filecoin');
const {
  loadSession,
  saveSession,
  clearSession,
  deriveMasterKey,
  encryptMasterKey,
  decryptMasterKey
} = require('../cli/session');

function promptInput(promptText) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: true });
    rl.question(promptText, (answer) => {
      rl.close();
      resolve(String(answer || '').trim());
    });
  });
}

function promptHiddenInput(promptText) {
  return new Promise((resolve) => {
    process.stdout.write(promptText);
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: true });
    rl.stdoutMuted = true;
    rl._writeToOutput = function _writeToOutput(stringToWrite) {
      if (rl.stdoutMuted) {
        rl.output.write('*');
      } else {
        rl.output.write(stringToWrite);
      }
    };
    rl.question('', (answer) => {
      rl.history = rl.history.slice(1);
      rl.close();
      process.stdout.write('\n');
      resolve(String(answer || ''));
    });
  });
}

async function requireSession() {
  const session = await loadSession();
  if (!session) {
    console.log('Please login first using: npx nodio-cli nodio login');
    process.exit(1);
  }
  return session;
}

function attachSessionToken(api, session) {
  if (session?.apiToken) {
    api.defaults.headers['x-api-token'] = session.apiToken;
  }
}

function packEncryptedKey({ iv, authTag, cipherText }) {
  return `${iv}:${authTag}:${cipherText.toString('base64')}`;
}

function unpackEncryptedKey(payload) {
  const [iv, authTag, cipherText] = String(payload || '').split(':');
  if (!iv || !authTag || !cipherText) {
    throw new Error('invalid encrypted key format');
  }
  return {
    iv,
    authTag,
    cipherText: Buffer.from(cipherText, 'base64')
  };
}

async function buildSessionPayload(authResponse, password) {
  const masterKey = await deriveMasterKey(password, authResponse.argon2Salt);
  const encryptedMasterKey = encryptMasterKey(masterKey, authResponse.apiToken);
  return {
    apiToken: authResponse.apiToken,
    argon2Salt: authResponse.argon2Salt,
    userId: authResponse.userId,
    email: authResponse.email || null,
    encryptedMasterKey
  };
}

async function login(options) {
  const serverUrl = options.server;
  const email = await promptInput('Email: ');
  const password = await promptHiddenInput('Password: ');

  const api = createApiClient(serverUrl);
  const response = await api.post('/auth/login', { email, password });
  const session = await buildSessionPayload(response.data, password);
  await saveSession(session);
  console.log('Logged in ✅');
}

async function register(options) {
  const serverUrl = options.server;
  const email = await promptInput('Email: ');
  const password = await promptHiddenInput('Password: ');

  const api = createApiClient(serverUrl);
  const response = await api.post('/auth/register', { email, password });
  const session = await buildSessionPayload({ ...response.data, email }, password);
  await saveSession(session);
  console.log('Registered ✅');
}

async function logout() {
  await clearSession();
  console.log('Logged out ✅');
}

async function whoami(options) {
  const serverUrl = options.server;
  const session = await requireSession();
  const api = createApiClient(serverUrl);
  attachSessionToken(api, session);
  const response = await api.get('/auth/me');
  console.log(`email: ${response.data?.email || session.email || 'unknown'}`);
  console.log(`userId: ${response.data?.userId || session.userId || 'unknown'}`);
}

async function listFiles(options) {
  const serverUrl = options.server;
  const session = await requireSession();
  const api = createApiClient(serverUrl);
  attachSessionToken(api, session);
  const response = await api.get('/files');
  const files = response.data?.files || [];

  if (files.length === 0) {
    console.log('No files found for this account.');
    return;
  }

  for (const file of files) {
    console.log(`fileId: ${file.fileId}`);
    console.log(`name: ${file.originalName}`);
    console.log(`sizeBytes: ${file.sizeBytes}`);
    console.log(`createdAt: ${file.createdAt}`);
    console.log(`filecoinBackedUp: ${Boolean(file.filecoinBackedUp)}`);
    console.log(`filecoinCid: ${file.filecoinCid || ''}`);
    console.log('---');
  }
}

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

function splitEncryptedBufferBySizes(buffer, shardSizes) {
  const slices = [];
  let offset = 0;

  for (const sizeBytes of shardSizes) {
    const size = Number(sizeBytes) || 0;
    const end = offset + size;
    if (end > buffer.length) {
      throw new Error('filecoin buffer is smaller than expected shard sizes');
    }
    slices.push(buffer.subarray(offset, end));
    offset = end;
  }

  if (offset < buffer.length) {
    console.warn('[filecoin] extra bytes present after shard split; ignoring remainder');
  }

  return slices;
}

function decryptShardsFromBuffers(orderedShards, shardMetaMap, encryptedShardBuffers, keyBuffer) {
  const plainParts = [];

  for (let index = 0; index < orderedShards.length; index += 1) {
    const shard = orderedShards[index];
    const shardMeta = shardMetaMap.get(shard.shardId);
    if (!shardMeta) {
      throw new Error(`missing encryption metadata for shard ${shard.shardId}`);
    }

    const encryptedBuffer = encryptedShardBuffers[index];
    if (!encryptedBuffer) {
      throw new Error(`missing encrypted payload for shard ${shard.shardId}`);
    }

    if (sha256Hex(encryptedBuffer) !== shard.checksum) {
      throw new Error(`checksum mismatch for shard ${shard.shardId} from filecoin`);
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

  return Buffer.concat(plainParts);
}

async function requestFilecoinBackup(api, fileBuffer, fileId) {
  if (!fileBuffer || fileBuffer.length === 0) {
    return;
  }

  try {
    await api.post(
      `/files/${fileId}/filecoin/upload`,
      {
        dataBase64: fileBuffer.toString('base64')
      },
      {
        timeout: 180000
      }
    );
  } catch (error) {
    const message = error.response?.data?.error || error.message;
    console.warn(`[filecoin] server upload failed for ${fileId}: ${message}`);
  }
}

function fireAndForgetLayer1Reseed(api, fileId, orderedShards, encryptedShardBuffers, directTimeoutMs) {
  void (async () => {
    for (let index = 0; index < orderedShards.length; index += 1) {
      const shard = orderedShards[index];
      const encryptedBuffer = encryptedShardBuffers[index];
      if (!encryptedBuffer || !Array.isArray(shard.replicas) || shard.replicas.length === 0) {
        continue;
      }

      const directWriteResults = await Promise.allSettled(
        shard.replicas.map(async (replica) => {
          const putUrl = `${normalizeUrl(replica.url)}/shards/${shard.shardId}`;
          await axios.put(putUrl, encryptedBuffer, {
            headers: { 'Content-Type': 'application/octet-stream' },
            timeout: directTimeoutMs
          });
          return replica;
        })
      );

      const successfulReplicas = directWriteResults
        .filter((result) => result.status === 'fulfilled')
        .map((result) => result.value);

      const failedReplicas = directWriteResults
        .map((result, replicaIndex) => ({ result, replica: shard.replicas[replicaIndex] }))
        .filter((entry) => entry.result.status === 'rejected')
        .map((entry) => ({
          nodeId: entry.replica.nodeId,
          url: entry.replica.url,
          error: entry.result.reason?.message || 'direct write failed'
        }));

      if (failedReplicas.length > 0) {
        try {
          const relayResult = await relayStoreShard(api, {
            shardId: shard.shardId,
            fileId,
            nodeIds: failedReplicas.map((replica) => replica.nodeId),
            dataBuffer: encryptedBuffer
          });

          const promotedRelayReplicas = shard.replicas.filter((replica) =>
            relayResult.successfulNodeIds.includes(replica.nodeId)
          );

          for (const replica of promotedRelayReplicas) {
            if (!successfulReplicas.some((item) => item.nodeId === replica.nodeId)) {
              successfulReplicas.push(replica);
            }
          }
        } catch (relayError) {
          console.warn(`[filecoin] reseed relay failed for shard ${shard.shardId}: ${relayError.message}`);
        }
      }

      if (successfulReplicas.length > 0) {
        try {
          await api.post('/shards/register', {
            shardId: shard.shardId,
            fileId,
            order: shard.order,
            sizeBytes: shard.sizeBytes,
            checksum: shard.checksum,
            nodeIds: successfulReplicas.map((replica) => replica.nodeId)
          });
        } catch (registerError) {
          console.warn(`[filecoin] reseed register failed for shard ${shard.shardId}: ${registerError.message}`);
        }
      }
    }
  })();
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

  const session = await requireSession();
  const masterKey = decryptMasterKey(session.encryptedMasterKey, session.apiToken);

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
  const encryptedKeyPayload = encryptAes256Gcm(keyBuffer, masterKey);
  const encryptedAESKey = packEncryptedKey(encryptedKeyPayload);

  const api = createApiClient(serverUrl);
  attachSessionToken(api, session);
  const fileId = options.fileId || uuidv4();
  const originalName = path.basename(filePath);

  await api.post('/files/register', {
    fileId,
    originalName,
    sizeBytes: plainBuffer.length,
    shardCount: chunks.length,
    cipher: 'aes-256-gcm',
    encryptedAESKey,
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
  const encryptedShardBuffers = [];

  for (let order = 0; order < chunks.length; order += 1) {
    const shardId = `${fileId}-shard-${order}-${uuidv4().slice(0, 8)}`;
    const encrypted = encryptAes256Gcm(chunks[order], keyBuffer);
    const checksum = sha256Hex(encrypted.cipherText);
    encryptedShardBuffers[order] = encrypted.cipherText;

    let plannedReplicas = [];
    try {
      const placementResponse = await api.post('/shards/placement-plan', {
        shardId,
        sizeBytes: encrypted.cipherText.length,
        replicas
      });

      plannedReplicas = placementResponse.data.replicas || [];
      if (plannedReplicas.length < replicas) {
        console.warn(`[upload] placement returned ${plannedReplicas.length}/${replicas} replicas for ${shardId}`);
      }
    } catch (error) {
      const apiMessage = error.response?.data?.error;
      const statusCode = error.response?.status;
      if (statusCode === 409 && String(apiMessage || '').startsWith('insufficient_online_nodes')) {
        console.warn(`[upload] placement unavailable for ${shardId}: ${apiMessage}`);
        plannedReplicas = [];
      } else {
        throw error;
      }
    }

    let successfulReplicas = [];
    let failedReplicas = [];

    if (plannedReplicas.length === 0) {
      console.warn(`[upload] no donor replicas available for shard ${shardId}; continuing with Filecoin only`);
    } else if (relayFirst) {
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
      const failures = failedReplicas.map((r) => `${r.url} (${r.error})`).join(', ');
      console.warn(`[upload] shard ${shardId} stored on 0 donors${failures ? `: ${failures}` : ''}`);
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
    encryptedAESKey,
    metadata: {
      encryption: {
        algorithm: 'aes-256-gcm',
        shardSizeBytes,
        keyFormat: 'base64',
        shards: encryptionShardMeta
      }
    }
  });

  // Upload to Filecoin via the central server (uses server-side wallet key)
  const encryptedFileBuffer = Buffer.concat(encryptedShardBuffers);
  console.log(`[filecoin] uploading to Filecoin via central server (this may take a while)...`);
  await requestFilecoinBackup(api, encryptedFileBuffer, fileId);

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
  const directTimeoutMs = Number(options.directTimeoutMs || 1200);
  const relayFirst = Boolean(options.relayFirst);

  const session = await requireSession();
  const masterKey = decryptMasterKey(session.encryptedMasterKey, session.apiToken);

  if (!Number.isFinite(directTimeoutMs) || directTimeoutMs <= 0) {
    throw new Error('direct-timeout-ms must be greater than 0');
  }

  const api = createApiClient(serverUrl);
  attachSessionToken(api, session);
  const manifestResponse = await api.get(`/files/${fileId}/manifest`);
  const manifest = manifestResponse.data;
  const metadata = manifest.file?.metadata || {};
  const encryption = metadata.encryption;

  let keyBuffer = null;
  if (options.keyBase64) {
    keyBuffer = parseAesKey(options.keyBase64);
  } else if (manifest.file?.encryptedAESKey) {
    const encryptedKeyPayload = unpackEncryptedKey(manifest.file.encryptedAESKey);
    keyBuffer = decryptAes256Gcm(
      encryptedKeyPayload.cipherText,
      masterKey,
      encryptedKeyPayload.iv,
      encryptedKeyPayload.authTag
    );
    if (!Buffer.isBuffer(keyBuffer) || keyBuffer.length !== 32) {
      throw new Error('invalid decrypted AES key length');
    }
  }

  if (!keyBuffer) {
    throw new Error('missing encryption key; provide --key-base64 or login to access encrypted keys');
  }

  if (!encryption || encryption.algorithm !== 'aes-256-gcm') {
    throw new Error('missing or unsupported encryption metadata');
  }

  const shardMetaMap = new Map((encryption.shards || []).map((entry) => [entry.shardId, entry]));

  const orderedShards = [...(manifest.shards || [])].sort((a, b) => a.order - b.order);
  let reconstructed = null;

  try {
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

    reconstructed = Buffer.concat(plainParts);
  } catch (error) {
    console.warn(`[download] layer1 failed for ${fileId}: ${error.message}`);

    const filecoinCid = manifest.file?.filecoinCid;
    if (!filecoinCid) {
      throw error;
    }

    const encryptedFileBuffer = await retrieveFromFilecoin(filecoinCid);
    if (!encryptedFileBuffer) {
      throw new Error('filecoin retrieval failed');
    }

    const encryptedShardBuffers = splitEncryptedBufferBySizes(
      encryptedFileBuffer,
      orderedShards.map((shard) => shard.sizeBytes)
    );

    reconstructed = decryptShardsFromBuffers(orderedShards, shardMetaMap, encryptedShardBuffers, keyBuffer);
    fireAndForgetLayer1Reseed(api, fileId, orderedShards, encryptedShardBuffers, directTimeoutMs);
  }
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
  const session = await requireSession();
  attachSessionToken(api, session);
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
  deleteFile,
  login,
  register,
  logout,
  whoami,
  listFiles
};
