const dotenv = require('dotenv');
const { formatUnits, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { calibration: filecoinCalibration } = require('@filoz/synapse-core/chains');
let synapseSdkPromise = null;

dotenv.config();

let synapseClientPromise = null;
const FILECOIN_MIN_PAYLOAD_BYTES = 127;
function resolveSynapseFactory(sdk) {
  const candidate = sdk?.default || sdk;

  if (candidate?.createSynapse && typeof candidate.createSynapse === 'function') {
    return candidate.createSynapse;
  }

  if (candidate?.Synapse?.create && typeof candidate.Synapse.create === 'function') {
    return candidate.Synapse.create.bind(candidate.Synapse);
  }

  if (candidate?.Synapse && typeof candidate.Synapse === 'function') {
    return async (options) => new candidate.Synapse(options);
  }

  if (typeof candidate === 'function') {
    return async (options) => new candidate(options);
  }

  throw new Error('synapse sdk factory not found');
}

async function getSynapseSdk() {
  if (!synapseSdkPromise) {
    synapseSdkPromise = import('@filoz/synapse-sdk');
  }

  return synapseSdkPromise;
}

function normalizeCid(value) {
  if (!value) {
    return null;
  }

  if (typeof value === 'string') {
    return value;
  }

  // Handle CID objects (has toString() method)
  if (value && typeof value.toString === 'function' && (value.constructor?.name === 'CID' || value.asCID)) {
    return value.toString();
  }

  if (typeof value.cid === 'string') {
    return value.cid;
  }

  if (value.cid && typeof value.cid.toString === 'function') {
    return value.cid.toString();
  }

  if (typeof value.pieceCid === 'string') {
    return value.pieceCid;
  }

  if (value.pieceCid && typeof value.pieceCid.toString === 'function') {
    return value.pieceCid.toString();
  }

  if (typeof value.id === 'string') {
    return value.id;
  }

  return null;
}

function normalizeBuffer(value) {
  if (!value) {
    return null;
  }

  if (Buffer.isBuffer(value)) {
    return value;
  }

  if (value.data && Buffer.isBuffer(value.data)) {
    return value.data;
  }

  if (value instanceof ArrayBuffer) {
    return Buffer.from(value);
  }

  if (ArrayBuffer.isView(value)) {
    return Buffer.from(value.buffer);
  }

  if (typeof value === 'string') {
    return Buffer.from(value, 'base64');
  }

  if (value.data && typeof value.data === 'string') {
    return Buffer.from(value.data, 'base64');
  }

  return null;
}

function normalizeBalance(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'number') {
    return value;
  }

  if (typeof value === 'string') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  if (typeof value === 'bigint') {
    return Number(value) / 1_000_000;
  }

  if (typeof value === 'object') {
    if (value.formatted) {
      const parsed = Number(value.formatted);
      return Number.isFinite(parsed) ? parsed : null;
    }
    if (value.value !== undefined) {
      return normalizeBalance(value.value);
    }
  }

  return null;
}

function encodeFilecoinPayload(fileBuffer) {
  const payloadLength = Buffer.byteLength(fileBuffer);
  const header = Buffer.alloc(8);
  header.writeBigUInt64BE(BigInt(payloadLength));

  const wrapped = Buffer.concat([header, fileBuffer]);
  if (wrapped.length >= FILECOIN_MIN_PAYLOAD_BYTES) {
    return wrapped;
  }

  return Buffer.concat([wrapped, Buffer.alloc(FILECOIN_MIN_PAYLOAD_BYTES - wrapped.length)]);
}

function decodeFilecoinPayload(fileBuffer) {
  if (!Buffer.isBuffer(fileBuffer) || fileBuffer.length < 8) {
    return fileBuffer;
  }

  try {
    const originalLength = Number(fileBuffer.readBigUInt64BE(0));
    const start = 8;
    const end = start + originalLength;
    if (originalLength >= 0 && end <= fileBuffer.length) {
      return fileBuffer.subarray(start, end);
    }
  } catch {
    return fileBuffer;
  }

  return fileBuffer;
}

async function buildSynapseClient() {
  const privateKey = process.env.NODIO_WALLET_PRIVATE_KEY;
  if (!privateKey) {
    throw new Error('NODIO_WALLET_PRIVATE_KEY is not set');
  }

  const rpcUrl =
    process.env.FILECOIN_RPC_URL
    || 'https://api.calibration.node.glif.io/rpc/v1';

  if (!rpcUrl) {
    throw new Error('FILECOIN_RPC_URL is not set');
  }

  const account = privateKeyToAccount(privateKey);

  const synapseSdk = await getSynapseSdk();
  const Synapse = synapseSdk?.Synapse || synapseSdk?.default?.Synapse || synapseSdk?.default;

  if (!Synapse || typeof Synapse.create !== 'function') {
    throw new Error('synapse sdk Synapse.create not found');
  }

  return Synapse.create({
    chain: filecoinCalibration,
    transport: http(rpcUrl),
    account,
    source: null,
    withCDN: false
  });
}

async function getSynapseClient() {
  if (!synapseClientPromise) {
    synapseClientPromise = buildSynapseClient();
  }

  return synapseClientPromise;
}

async function uploadToFilecoin(fileBuffer, fileId) {
  if (!fileBuffer) {
    throw new Error('file buffer is required');
  }

  const client = await getSynapseClient();
  const label = fileId ? String(fileId) : undefined;
  const payload = encodeFilecoinPayload(Buffer.isBuffer(fileBuffer) ? fileBuffer : Buffer.from(fileBuffer));

  if (!client || !client.storage || typeof client.storage.upload !== 'function') {
    console.warn('[filecoin] storage.upload not available on synapse client');
    return null;
  }

  const maxAttempts = 3;
  const baseBackoffMs = 500;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const result = await client.storage.upload(payload, { copies: 2, name: label });
      return normalizeCid(result);
    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      console.warn(`[filecoin] storage.upload attempt ${attempt}/${maxAttempts} failed: ${msg}`);
      if (err && err.cause) {
        console.warn(`[filecoin]   cause: ${err.cause.message || err.cause}`);
      }
      if (attempt === maxAttempts) {
        console.error('[filecoin] storage.upload failed - max attempts reached');
        return null;
      }
      // exponential backoff
      const wait = baseBackoffMs * (2 ** (attempt - 1));
      // eslint-disable-next-line no-await-in-loop
      await new Promise((resolve) => setTimeout(resolve, wait));
    }
  }
  return null;
}

async function retrieveFromFilecoin(cid) {
  try {
    if (!cid) {
      throw new Error('cid is required');
    }

    const client = await getSynapseClient();

    if (typeof client.retrieve === 'function') {
      const result = await client.retrieve(cid);
      return decodeFilecoinPayload(normalizeBuffer(result));
    }

    if (client.storage?.retrieve && typeof client.storage.retrieve === 'function') {
      const result = await client.storage.retrieve(cid);
      return decodeFilecoinPayload(normalizeBuffer(result));
    }

    if (client.storage?.download && typeof client.storage.download === 'function') {
      const result = await client.storage.download({ pieceCid: cid });
      return decodeFilecoinPayload(normalizeBuffer(result));
    }

    if (client.file?.retrieve && typeof client.file.retrieve === 'function') {
      const result = await client.file.retrieve(cid);
      return decodeFilecoinPayload(normalizeBuffer(result));
    }

    if (typeof client.fetch === 'function') {
      const result = await client.fetch(cid);
      return decodeFilecoinPayload(normalizeBuffer(result));
    }

    console.warn('[filecoin] retrieve method not found on synapse client');
    return null;
  } catch (error) {
    console.error('[filecoin] retrieve failed', error.message);
    return null;
  }
}

async function getWalletBalance() {
  try {
    const client = await getSynapseClient();

    if (client?.payments && typeof client.payments.walletBalance === 'function') {
      const balance = await client.payments.walletBalance({ token: 'USDFC' });
      return Number.parseFloat(formatUnits(balance, 18));
    }

    console.warn('[filecoin] balance method not found on synapse client');
    return null;
  } catch (error) {
    console.error('[filecoin] balance check failed', error.message);
    return null;
  }
}

module.exports = {
  uploadToFilecoin,
  retrieveFromFilecoin,
  getWalletBalance
};
