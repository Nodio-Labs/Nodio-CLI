const fs = require('fs/promises');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

const DEVICE_DIR = path.join(os.homedir(), '.nodio');
const DEVICE_FILE = path.join(DEVICE_DIR, 'device-identity.json');

async function readDeviceIdentity() {
  try {
    const raw = await fs.readFile(DEVICE_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

async function getOrCreateDeviceKey() {
  const identity = await readDeviceIdentity();
  if (identity.deviceKey) {
    return identity.deviceKey;
  }

  const deviceKey = crypto.randomUUID();
  await fs.mkdir(DEVICE_DIR, { recursive: true });
  await fs.writeFile(
    DEVICE_FILE,
    JSON.stringify(
      {
        deviceKey,
        createdAt: new Date().toISOString()
      },
      null,
      2
    ),
    'utf-8'
  );

  return deviceKey;
}

module.exports = {
  getOrCreateDeviceKey
};
