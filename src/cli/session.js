const fs = require('fs/promises');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const argon2 = require('argon2');
const { encryptAes256Gcm, decryptAes256Gcm } = require('../common/crypto');

const SESSION_DIR = '.nodio';
const SESSION_FILE = 'session.json';

function getSessionPath() {
  return path.join(os.homedir(), SESSION_DIR, SESSION_FILE);
}

async function loadSession() {
  try {
    const raw = await fs.readFile(getSessionPath(), 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

async function saveSession(session) {
  const dir = path.join(os.homedir(), SESSION_DIR);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(getSessionPath(), JSON.stringify(session, null, 2));
}

async function clearSession() {
  try {
    await fs.unlink(getSessionPath());
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error;
    }
  }
}

async function deriveMasterKey(password, argon2Salt) {
  const saltBuffer = Buffer.from(argon2Salt, 'hex');
  return argon2.hash(password, {
    type: argon2.argon2id,
    salt: saltBuffer,
    hashLength: 32,
    raw: true
  });
}

function deriveSessionKey(apiToken) {
  return crypto.createHash('sha256').update(apiToken).digest();
}

function packEncryptedKey({ iv, authTag, cipherText }) {
  return `${iv}:${authTag}:${cipherText.toString('base64')}`;
}

function unpackEncryptedKey(payload) {
  const [iv, authTag, cipherText] = String(payload || '').split(':');
  if (!iv || !authTag || !cipherText) {
    throw new Error('invalid encryptedMasterKey format');
  }
  return {
    iv,
    authTag,
    cipherText: Buffer.from(cipherText, 'base64')
  };
}

function encryptMasterKey(masterKeyBuffer, apiToken) {
  const sessionKey = deriveSessionKey(apiToken);
  const encrypted = encryptAes256Gcm(masterKeyBuffer, sessionKey);
  return packEncryptedKey(encrypted);
}

function decryptMasterKey(encryptedMasterKey, apiToken) {
  const sessionKey = deriveSessionKey(apiToken);
  const payload = unpackEncryptedKey(encryptedMasterKey);
  return decryptAes256Gcm(payload.cipherText, sessionKey, payload.iv, payload.authTag);
}

module.exports = {
  getSessionPath,
  loadSession,
  saveSession,
  clearSession,
  deriveMasterKey,
  encryptMasterKey,
  decryptMasterKey
};
