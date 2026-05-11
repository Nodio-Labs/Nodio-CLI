(async () => {
  try {
    require('dotenv').config();
    const axios = require('axios');
    const path = require('path');
    const fs = require('fs');
    const { uploadToFilecoin } = require('../services/filecoin');

    const server = process.argv[2] || process.env.NODIO_SERVER_URL || 'http://127.0.0.1:4000';
    const fileId = process.argv[3];
    if (!fileId) {
      throw new Error('Usage: node backupFromLayer1.js <serverUrl> <fileId>');
    }

    console.log('Fetching manifest for', fileId);
    const manifestResp = await axios.get(`${server}/api/files/${fileId}/manifest`, { timeout: 20000 });
    const manifest = manifestResp.data;
    const shards = manifest.shards || [];

    const encryptedShardBuffers = [];

    for (const shard of shards) {
      const replicas = shard.replicas || [];
      let fetched = null;
      for (const replica of replicas) {
        if (!replica.url) continue;
        try {
          const url = `${replica.url.replace(/\/+$/, '')}/shards/${shard.shardId}`;
          const resp = await axios.get(url, { responseType: 'arraybuffer', timeout: 20000 });
          const buf = Buffer.from(resp.data);
          // simple checksum check
          const crypto = require('crypto');
          const sha = crypto.createHash('sha256').update(buf).digest('hex');
          if (sha === shard.checksum) {
            fetched = buf;
            break;
          }
        } catch (err) {
          console.warn('replica fetch failed', replica.nodeId, replica.url, err.message);
        }
      }

      if (!fetched) {
        throw new Error(`failed to fetch shard ${shard.shardId} from any replica`);
      }

      encryptedShardBuffers.push(fetched);
    }

    const encryptedFileBuffer = Buffer.concat(encryptedShardBuffers);
    console.log('Uploading to Filecoin (this may take a while)...');
    const cid = await uploadToFilecoin(encryptedFileBuffer, fileId);
    console.log('uploadToFilecoin returned cid=', cid);
    if (!cid) {
      throw new Error('uploadToFilecoin failed or returned null');
    }

    try {
      await axios.post(`${server}/api/files/${fileId}/filecoin`, { filecoinCid: cid, filecoinBackedUp: true });
      console.log('Persisted CID to server');
    } catch (err) {
      console.warn('Failed to persist CID to server:', err.message || err);
    }

    console.log('Done');
    process.exit(0);
  } catch (error) {
    console.error('error:', error.message || error);
    if (error.stack) console.error(error.stack);
    process.exit(1);
  }
})();
