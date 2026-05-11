(async () => {
  try {
    require('dotenv').config();
    const { http } = require('viem');
    const { privateKeyToAccount } = require('viem/accounts');
    const { calibration } = require('@filoz/synapse-core/chains');

    const synapseSdk = await import('@filoz/synapse-sdk');
    const Synapse = synapseSdk?.Synapse || synapseSdk?.default?.Synapse || synapseSdk?.default;
    if (!Synapse || typeof Synapse.create !== 'function') {
      throw new Error('Synapse.create not available from @filoz/synapse-sdk');
    }

    const privateKey = process.env.NODIO_WALLET_PRIVATE_KEY;
    if (!privateKey) {
      throw new Error('NODIO_WALLET_PRIVATE_KEY must be set in environment');
    }

    const rpcUrl = process.env.FILECOIN_RPC_URL || 'https://api.calibration.node.glif.io/rpc/v1';

    const account = privateKeyToAccount(privateKey);

    console.log('Initializing Synapse client (calibration)...');
    const synapse = await Synapse.create({
      chain: calibration,
      transport: http(rpcUrl),
      account,
      source: null,
      withCDN: false
    });

    if (!synapse.payments || typeof synapse.payments.fundSync !== 'function') {
      throw new Error('Payments service not available on Synapse client');
    }

    // 3 USDFC with 18 decimals
    const amount = BigInt('3000000000000000000');

    console.log(`Submitting fund request for ${amount.toString()} (3 USDFC)...`);

    const result = await synapse.payments.fundSync({ amount });

    console.log('Fund transaction submitted.');
    console.log('Hash:', result.hash);
    console.log('Receipt:', result.receipt ? {
      blockNumber: result.receipt.blockNumber,
      transactionHash: result.receipt.transactionHash,
      status: result.receipt.status
    } : null);

    process.exit(0);
  } catch (error) {
    console.error('[fundSynapse] error:', error && error.message ? error.message : error);
    if (error && error.stack) console.error(error.stack);
    process.exit(1);
  }
})();
