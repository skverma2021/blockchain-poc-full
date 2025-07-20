// routes/transactions.js
const express = require('express');
const router = express.Router();
const db = require('../db'); // Import your database functions
const network = require('./network');
const axios = require('axios');
const crypto = require('crypto'); // Make sure crypto is imported

// This endpoint is for RECEIVING a transaction that has been broadcast from a peer node.
// The transaction object received here is expected to be complete and fully formed.
router.post('/receive', async (req, res) => {
    console.log(`Node ${network.myNodeUrl}: Received transaction for processing...`);
    try {
        const transactionData = req.body;

        // Validation for a received transaction: It should already have an ID and a hash.
        if (!transactionData.transactionId || !transactionData.timestamp || !transactionData.rowHash || !transactionData.rawDataJson || transactionData.projId === undefined) {
            console.error(`Node ${network.myNodeUrl}: Received transaction is missing mandatory fields (transactionId, timestamp, rowHash, rawDataJson, or projId). Rejecting.`);
            return res.status(400).json({ error: 'Missing mandatory transaction fields for reception.' });
        }
        console.log(`Node ${network.myNodeUrl}: Passed Check-1 (Mandatory fields present)`);

        // Re-validate the hash: Use the exact rawDataJson string that was originally hashed.
        const dataToHash = transactionData.transactionId + transactionData.timestamp + transactionData.rawDataJson;
        const reCalculatedHash = crypto.createHash('sha256').update(dataToHash).digest('hex');

        console.log(`Node ${network.myNodeUrl}: Original rowHash: ${transactionData.rowHash}`);
        console.log(`Node ${network.myNodeUrl}: Recalculated Hash: ${reCalculatedHash}`);

        if (reCalculatedHash !== transactionData.rowHash) {
            console.error(`Node ${network.myNodeUrl}: Received transaction hash mismatch. Rejecting.`);
            return res.status(400).json({ error: 'Transaction hash mismatch. Data may be corrupted.' });
        }
        console.log(`Node ${network.myNodeUrl}: Passed Check-2 (Hash validation successful)`);

        // Add the received transaction to this node's mempool.
        // createTransaction is designed to use provided IDs/hashes if they exist.
        await db.createTransaction(transactionData);

        res.status(201).json({
            message: 'Transaction received and accepted into mempool.',
            transaction: transactionData
        });
    } catch (error) {
        console.error(`Node ${network.myNodeUrl}: Error in POST /api/transactions/receive:`, error.message);
        if (error.message.includes('SQLITE_CONSTRAINT: UNIQUE constraint failed: mempool_transactions.transaction_id')) {
            return res.status(409).json({ error: 'Transaction ID already exists in mempool.' });
        }
        res.status(500).json({ error: 'Failed to accept received transaction.' });
    }
});


// This endpoint is for a client (your local API or a user interface) to SUBMIT a new, raw transaction.
// This route is the starting point for a new transaction on the network.
router.post('/submit', async function (req, res) {
    console.log(`Node ${network.myNodeUrl}: Received new transaction for submission...`);
    const rawTransactionData = req.body;
    try {
        // 1. Add the transaction to this node's own mempool.
        // db.createTransaction() will handle the generation of ID, timestamp, hash, and projId.
        const newTransaction = await db.createTransaction(rawTransactionData);

        // 2. Prepare POST requests to all other known nodes to their '/receive' endpoint.
        const broadcastPromises = network.networkNodes.map(networkNodeUrl => {
            console.log(`Node ${network.myNodeUrl}: Broadcasting to ${networkNodeUrl}/api/transactions/receive`);
            // return axios.post(`${networkNodeUrl}/api/transactions/receive`, newTransaction);
            return axios.post(`${networkNodeUrl}/transactions/receive`, newTransaction);
        });

        // 3. Wait for all broadcast requests to finish.
        await Promise.all(broadcastPromises);

        // 4. Respond to the client after the local creation and broadcast are successful.
        res.status(201).json({
            note: 'Transaction created locally and broadcast successfully.',
            transaction: newTransaction
        });

    } catch (error) {
        console.error(`Node ${network.myNodeUrl}: Transaction submission and broadcast failed:`, error.message);
        res.status(500).json({
            note: 'Transaction broadcast failed.',
            error: error.message
        });
    }
});

// A route to get all confirmed transactions from the confirmed_transactions table.
router.get('/', async (req, res) => {
    console.log(`Node ${network.myNodeUrl}: Fetching all confirmed transactions...`);
    try {
        const transactions = await db.readAllTransactions();
        res.status(200).json({ transactions });
    } catch (error) {
        console.error(`Node ${network.myNodeUrl}: Error fetching confirmed transactions:`, error.message);
        res.status(500).json({ error: 'Failed to retrieve transactions.' });
    }
});

module.exports = router;