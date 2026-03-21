const axios = require('axios');

function normalizeUrl(url) {
  return String(url || '').replace(/\/+$/, '');
}

function createApiClient(serverUrl) {
  return axios.create({
    baseURL: `${normalizeUrl(serverUrl)}/api`,
    timeout: 30000,
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

module.exports = {
  createApiClient,
  normalizeUrl
};
