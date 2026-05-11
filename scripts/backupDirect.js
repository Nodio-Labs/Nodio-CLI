(async () => {
  try {
    require('dotenv').config();
    const axios = require('axios');
    const { privateKeyToAccount } = require('viem/accounts');
    const { http } = require('viem');
    const { calibration } = require('@filoz/synapse-core/chains');

    const synapseSdk = await import('@filoz/synapse-sdk');
    const Synapse = synapseSdk?.Synapse || synapseSdk?.default?.Synapse || synapseSdk?.default;
    if (!Synapse || typeof Synapse.create !== 'function') {
      throw new Error('Synapse.create not available');
    }

    const server = process.argv[2] || 'http://127.0.0.1:4000';
    const fileId = process.argv[3];
    if (!fileId) throw new Error('Usage: node backupDirect.js <serverUrl> <fileId>');

    const privateKey = process.env.NODIO_WALLET_PRIVATE_KEY;
    if (!privateKey) throw new Error('NODIO_WALLET_PRIVATE_KEY not set');

    const rpcUrl = process.env.FILECOIN_RPC_URL || 'https://api.calibration.node.glif.io/rpc/v1';
    const account = privateKeyToAccount(privateKey);

    console.log('Init Synapse...');
    const synapse = await Synapse.create({ chain: calibration, transport: http(rpcUrl), account, source: null, withCDN: false });
    if (!synapse.storage || typeof synapse.storage.upload !== 'function') throw new Error('storage.upload not available');

    console.log('Fetching manifest...');
    const manifestResp = await axios.get(`${server}/api/files/${fileId}/manifest`);
    const manifest = manifestResp.data;
    const shards = manifest.shards || [];

    const encryptedShardBuffers = [];
    for (const shard of shards) {
      const replica = (shard.replicas || [])[0];
      if (!replica || !replica.url) throw new Error('no replica url');
      const url = `${replica.url.replace(/\/+$/, '')}/shards/${shard.shardId}`;
      const resp = await axios.get(url, { responseType: 'arraybuffer' });
      encryptedShardBuffers.push(Buffer.from(resp.data));
    }

    const payload = Buffer.concat(encryptedShardBuffers);
    console.log('Calling synapse.storage.upload (this may take a while)...');
    try {
      const result = await synapse.storage.upload(payload, { copies: 2 });
      console.log('upload result:', result);
    } catch (err) {
      console.error('synapse.storage.upload error:', err);
      if (err && err.cause) console.error('cause:', err.cause);
    }

    process.exit(0);
  } catch (error) {
    console.error(error && error.stack ? error.stack : error);
    process.exit(1);
  }
})();
