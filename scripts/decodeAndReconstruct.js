(async () => {
  try {
    require('dotenv').config();
    const axios = require('axios');
    const fs = require('fs/promises');
    const path = require('path');
    const { sha256Hex, decryptAes256Gcm } = require('../src/common/crypto');

    const server = process.argv[2] || 'http://127.0.0.1:4000';
    const fileId = process.argv[3];
    const keyBase64 = process.argv[4];

    if (!fileId) {
      console.error('Usage: node scripts/decodeAndReconstruct.js <serverUrl> <fileId> [keyBase64]');
      process.exit(2);
    }

    console.log('Fetching manifest...');
    const resp = await axios.get(`${server}/api/files/${fileId}/manifest`);
    const manifest = resp.data || {};
    const orderedShards = [...(manifest.shards || [])].sort((a, b) => a.order - b.order);

    const rawPath = path.resolve(`./${fileId}.filecoin.raw`);
    console.log('Reading raw payload from', rawPath);
    const raw = await fs.readFile(rawPath);

    let decoded = raw;
    if (raw.length >= 8) {
      const header = raw.subarray(0, 8);
      const originalLength = Number(header.readBigUInt64BE(0));
      const start = 8;
      const end = start + originalLength;
      if (originalLength >= 0 && end <= raw.length) {
        decoded = raw.subarray(start, end);
        console.log('Found 8-byte header; extracted original payload length', originalLength);
      } else {
        console.log('8-byte header present but length invalid; using full buffer');
      }
    } else {
      console.log('No 8-byte header found; using full buffer');
    }

    const decodedPath = path.resolve(`./${fileId}.filecoin.decoded.raw`);
    await fs.writeFile(decodedPath, decoded);
    console.log('Wrote decoded payload to', decodedPath);

    if (!orderedShards || orderedShards.length === 0) {
      console.warn('No shard metadata in manifest; stopping after decode');
      process.exit(0);
    }

    const sizes = orderedShards.map((s) => Number(s.sizeBytes || s.size || 0));
    const totalSizes = sizes.reduce((a, b) => a + b, 0);
    if (totalSizes > decoded.length) {
      console.warn('Sum of shard sizes > decoded payload length; attempting best-effort split');
    }

    // split by sizes
    const shards = [];
    let offset = 0;
    for (let i = 0; i < sizes.length; i++) {
      const size = sizes[i] || 0;
      const end = Math.min(offset + size, decoded.length);
      const slice = decoded.subarray(offset, end);
      shards.push(slice);
      offset = end;
    }
    if (offset < decoded.length) {
      console.warn('Extra bytes present after shard split; saved as remainder');
      await fs.writeFile(path.resolve(`./${fileId}.filecoin.remainder.raw`), decoded.subarray(offset));
    }

    for (let i = 0; i < shards.length; i++) {
      const shard = shards[i];
      const shardPath = path.resolve(`./${fileId}.shard.${i}.enc`);
      await fs.writeFile(shardPath, shard);
      const expectedChecksum = orderedShards[i]?.checksum;
      if (expectedChecksum) {
        const actual = sha256Hex(shard);
        console.log(`shard ${i}: wrote ${shardPath} size=${shard.length} checksum match=${actual === expectedChecksum}`);
      } else {
        console.log(`shard ${i}: wrote ${shardPath} size=${shard.length}`);
      }
    }

    if (!keyBase64) {
      console.log('No key provided; stopping after saving encrypted shards. To decrypt, re-run with keyBase64 as third arg.');
      process.exit(0);
    }

    const keyBuffer = Buffer.from(keyBase64, 'base64');
    if (keyBuffer.length !== 32) {
      throw new Error('Provided keyBase64 does not decode to 32 bytes');
    }

    const decryptedParts = [];

    const encryptionShards = (manifest.file && manifest.file.metadata && manifest.file.metadata.encryption && manifest.file.metadata.encryption.shards) || [];
    const shardMetaMap = new Map((encryptionShards || []).map((s) => [s.shardId, s]));

    for (let i = 0; i < shards.length; i++) {
      const topShard = orderedShards[i];
      if (!topShard) throw new Error(`missing top-level shard metadata for index ${i}`);
      const shardMeta = shardMetaMap.get(topShard.shardId);
      if (!shardMeta) throw new Error(`missing encryption metadata for shard ${topShard.shardId}`);
      const iv = shardMeta.iv;
      const authTag = shardMeta.authTag;
      const encryptedBuffer = shards[i];
      try {
        const plain = decryptAes256Gcm(encryptedBuffer, keyBuffer, iv, authTag);
        decryptedParts.push(plain);
        console.log(`shard ${i}: decrypted ok size=${plain.length}`);
      } catch (err) {
        console.error(`shard ${i}: decryption failed: ${err.message}`);
        throw err;
      }
    }

    const reconstructed = Buffer.concat(decryptedParts);
    const originalName = manifest.file?.originalName || `${fileId}.reconstructed`;
    const outPath = path.resolve(`./${originalName}`);
    await fs.writeFile(outPath, reconstructed);
    console.log('Wrote reconstructed file to', outPath, 'size=', reconstructed.length);

    process.exit(0);
  } catch (err) {
    console.error(err && err.stack ? err.stack : err);
    process.exit(1);
  }
})();
