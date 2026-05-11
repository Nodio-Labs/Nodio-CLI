(async () => {
  try {
    require('dotenv').config();
    const axios = require('axios');
    const fs = require('fs/promises');
    const path = require('path');

    const server = process.argv[2] || 'http://127.0.0.1:4000';
    const fileId = process.argv[3];
    if (!fileId) {
      console.error('Usage: node scripts/downloadFromFilecoin.js <serverUrl> <fileId>');
      process.exit(2);
    }

    const { retrieveFromFilecoin } = require('../services/filecoin');

    console.log('Fetching manifest...');
    const resp = await axios.get(`${server}/api/files/${fileId}/manifest`);
    const manifest = resp.data || {};
    const cid = manifest.file && manifest.file.filecoinCid;
    if (!cid) {
      throw new Error('filecoinCid not present on manifest');
    }

    console.log('Retrieving from Filecoin...', cid);
    const buf = await retrieveFromFilecoin(cid);
    if (!buf) throw new Error('retrieveFromFilecoin returned no data');

    const outPath = path.resolve(`./${fileId}.filecoin.raw`);
    await fs.writeFile(outPath, buf);
    console.log('Saved filecoin payload to', outPath);
    process.exit(0);
  } catch (err) {
    console.error(err && err.stack ? err.stack : err);
    process.exit(1);
  }
})();
