const crypto = require('crypto');

function sha256Hex(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

function encryptAes256Gcm(plainBuffer, keyBuffer) {
  if (!Buffer.isBuffer(keyBuffer) || keyBuffer.length !== 32) {
    throw new Error('keyBuffer must be a 32-byte buffer for AES-256');
  }

  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', keyBuffer, iv);
  const encrypted = Buffer.concat([cipher.update(plainBuffer), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return {
    cipherText: encrypted,
    iv: iv.toString('base64'),
    authTag: authTag.toString('base64')
  };
}

function decryptAes256Gcm(cipherBuffer, keyBuffer, ivBase64, authTagBase64) {
  if (!Buffer.isBuffer(keyBuffer) || keyBuffer.length !== 32) {
    throw new Error('keyBuffer must be a 32-byte buffer for AES-256');
  }

  const decipher = crypto.createDecipheriv(
    'aes-256-gcm',
    keyBuffer,
    Buffer.from(ivBase64, 'base64')
  );
  decipher.setAuthTag(Buffer.from(authTagBase64, 'base64'));
  return Buffer.concat([decipher.update(cipherBuffer), decipher.final()]);
}

module.exports = {
  sha256Hex,
  encryptAes256Gcm,
  decryptAes256Gcm
};
