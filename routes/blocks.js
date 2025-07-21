// routes/blocks.js
const express = require('express');
const router = express.Router();
const db = require('../db');
const network = require('./network');
const axios = require('axios');
const crypto = require('crypto');

// --- Helper function for Merkle Tree ---
function calculateMerkleRoot(transactions) {
    if (transactions.length === 0) {
        // Return a consistent hash for an empty set of transactions
        return crypto.createHash('sha256').update('empty_merkle_root_placeholder').digest('hex');
    }

    let hashes = transactions.map(tx => {
        // Ensure you're hashing the consistent representation of the transaction
        // The rowHash generated in db.js is ideal for this if it's based on rawDataJson
        // If not, you'd stringify the relevant parts of the tx object here.
        // For consistency, let's use the rowHash from the transaction object
        return tx.rowHash;
    });

    while (hashes.length > 1) {
        if (hashes.length % 2 !== 0) {
            hashes.push(hashes[hashes.length - 1]); // Duplicate last hash if odd number
        }
        let newHashes = [];
        for (let i = 0; i < hashes.length; i += 2) {
            newHashes.push(crypto.createHash('sha256').update(hashes[i] + hashes[i+1]).digest('hex'));
        }
        hashes = newHashes;
    }
    return hashes[0];
}

// --- The actual mining function (exclusive to RegAuth) ---
async function mineBlockInternal() {
    const BLOCK_SIZE = 5; // Define how many transactions per block

    try {
        const mempoolCount = await db.getMempoolCount();
        if (mempoolCount < BLOCK_SIZE) {
            console.log(`Node ${network.myNodeUrl}: Mempool has only ${mempoolCount} transactions, less than ${BLOCK_SIZE}. Not mining yet.`);
            return { note: `Mempool has only ${mempoolCount} transactions, less than ${BLOCK_SIZE}. Not mining yet.` };
        }

        console.log(`Node ${network.myNodeUrl}: Mempool count reached ${mempoolCount}. Triggering block mine...`);

        // 1. Get transactions from mempool
        const pendingTransactions = await db.getTransactionsForBlock(BLOCK_SIZE);

        // 2. Get the last block from this node's chain
        const lastBlock = await db.getLastBlock(); // This returns the full block object
        const previousBlockHash = lastBlock ? lastBlock.hash : '0'; // '0' for genesis block
        const lastBlockIndex = lastBlock ? lastBlock.blockIndex : -1;

        // 3. Create a new block object
        const newBlock = {
            blockIndex: lastBlockIndex + 1, // Corrected index calculation
            timestamp: new Date().toISOString(),
            transactions: pendingTransactions, // Full transaction objects
            merkleRoot: calculateMerkleRoot(pendingTransactions),
            previousBlockHash: previousBlockHash,
            nonce: 0, // PoA doesn't require a nonce race, can be 0 or a timestamp
            hash: '' // Will be calculated after all other fields are set
        };

        // 4. Calculate the block hash
        // The block hash should be calculated from all *relevant* block header fields
        // Stringify transactions based on their unique IDs and hashes for consistent hashing
        const transactionsForBlockHash = newBlock.transactions.map(tx => ({ id: tx.transactionId, hash: tx.rowHash }));
        const blockHashInput = newBlock.blockIndex + newBlock.timestamp + newBlock.merkleRoot + newBlock.previousBlockHash + newBlock.nonce + JSON.stringify(transactionsForBlockHash);
        newBlock.hash = crypto.createHash('sha256').update(blockHashInput).digest('hex');

        // 5. Add block to RegAuth's own blockchain (db.addBlockToBlockchain)
        await db.addBlockToBlockchain(newBlock);
        console.log(`Node ${network.myNodeUrl}: Block ${newBlock.blockIndex} mined and added to local blockchain.`);

        // 6. Remove confirmed transactions from mempool
        const confirmedTransactionIds = pendingTransactions.map(tx => tx.transactionId);
        await db.removeTransactionsFromMempool(confirmedTransactionIds);
        console.log(`Node ${network.myNodeUrl}: Confirmed transactions removed from mempool.`);

        // 7. Broadcast the new block to all other nodes (ProjA, ProjB)
        const broadcastPromises = network.networkNodes.map(networkNodeUrl => {
            console.log(`Node ${network.myNodeUrl}: Broadcasting block ${newBlock.blockIndex} to ${networkNodeUrl}/blocks/receive`);
            // return axios.post(`${networkNodeUrl}/api/blocks/receive`, { newBlock });
            return axios.post(`${networkNodeUrl}/blocks/receive`, { newBlock });
        });
        await Promise.all(broadcastPromises);

        console.log(`Node ${network.myNodeUrl}: Block ${newBlock.blockIndex} mined and broadcast successfully.`);
        return { note: 'Block mined and broadcast successfully.', block: newBlock };

    } catch (error) {
        console.error(`Node ${network.myNodeUrl}: Error mining block:`, error.message);
        throw error; // Re-throw for the caller (index.js) to catch
    }
}

// --- Express Routes ---

// POST /api/blocks/mine
router.post('/mine', async (req, res) => {
    try {
        const result = await mineBlockInternal();
        res.status(200).json(result);
    } catch (error) {
        // Error already logged in mineBlockInternal
        res.status(500).json({ error: 'Failed to mine block.', details: error.message });
    }
});

// POST /api/blocks/receive
router.post('/receive', async (req, res) => {
    const { newBlock } = req.body;
    console.log(`Node ${network.myNodeUrl}: Received block for processing (Index: ${newBlock.blockIndex})...`);

    // 1. Basic validation (check if block structure is valid)
    if (!newBlock || newBlock.blockIndex === undefined || !newBlock.timestamp || !newBlock.transactions || !newBlock.merkleRoot || !newBlock.previousBlockHash || !newBlock.hash) {
        console.error(`Node ${network.myNodeUrl}: Received block is missing required fields. Rejecting.`);
        return res.status(400).json({ note: 'Received block is missing required fields.' });
    }
    console.log(`Node ${network.myNodeUrl}: Passed Check-1 (Mandatory block fields present)`);

    try {
        // 2. Get the last block from THIS node's chain
        const lastBlockOnThisChain = await db.getLastBlock(); // This returns the full block object
        const lastBlockOnThisChainHash = lastBlockOnThisChain ? lastBlockOnThisChain.hash : '0';
        const lastBlockOnThisChainIndex = lastBlockOnThisChain ? lastBlockOnThisChain.blockIndex : -1;

        // --- Block Validation Checks ---

        // Check 2a: Is this block already on our chain or an older block?
        if (newBlock.blockIndex <= lastBlockOnThisChainIndex) {
            console.log(`Node ${network.myNodeUrl}: Received block (Index: ${newBlock.blockIndex}) is older or already on chain (Current last index: ${lastBlockOnThisChainIndex}). Ignoring.`);
            return res.status(200).json({ note: 'Block already on chain or older, ignoring.' });
        }

        // Check 2b: Is the block index exactly one greater than our last block?
        if (newBlock.blockIndex !== lastBlockOnThisChainIndex + 1) {
             console.error(`Node ${network.myNodeUrl}: Received block index mismatch. Expected ${lastBlockOnThisChainIndex + 1}, Got ${newBlock.blockIndex}. Rejecting.`);
             return res.status(400).json({ note: 'Block index is not sequential. Rejecting.' });
        }
        console.log(`Node ${network.myNodeUrl}: Passed Check-2 (Block index is sequential)`);


        // 3. Validate previous hash: Does it link correctly to our chain?
        if (newBlock.previousBlockHash !== lastBlockOnThisChainHash) {
            console.error(`Node ${network.myNodeUrl}: Received block previous hash mismatch. Expected ${lastBlockOnThisChainHash}, Got ${newBlock.previousBlockHash}. Rejecting.`);
            return res.status(400).json({ note: 'Received block does not link correctly to our chain (previous hash mismatch).' });
        }
        console.log(`Node ${network.myNodeUrl}: Passed Check-3 (Previous hash matches)`);


        // 4. Re-calculate hash: Does the block hash match its content?
        const transactionsForBlockHash = newBlock.transactions.map(tx => ({ id: tx.transactionId, hash: tx.rowHash }));
        const reCalculatedHash = crypto.createHash('sha256').update(newBlock.blockIndex + newBlock.timestamp + newBlock.merkleRoot + newBlock.previousBlockHash + newBlock.nonce + JSON.stringify(transactionsForBlockHash)).digest('hex');

        if (reCalculatedHash !== newBlock.hash) {
            console.error(`Node ${network.myNodeUrl}: Block hash mismatch for index ${newBlock.blockIndex}. Expected ${newBlock.hash}, Recalculated ${reCalculatedHash}. Rejecting.`);
            return res.status(400).json({ note: 'Block hash mismatch, block rejected.' });
        }
        console.log(`Node ${network.myNodeUrl}: Passed Check-4 (Block hash validation successful)`);

        // 5. Validate Merkle Root
        const receivedMerkleRoot = calculateMerkleRoot(newBlock.transactions);
        if (receivedMerkleRoot !== newBlock.merkleRoot) {
            console.warn(`Node ${network.myNodeUrl}: Merkle Root mismatch for block ${newBlock.blockIndex}. Potential data tampering. Expected ${newBlock.merkleRoot}, Recalculated ${receivedMerkleRoot}.`);
            return res.status(400).json({ note: 'Merkle Root mismatch, block rejected.' });
        }
        console.log(`Node ${network.myNodeUrl}: Passed Check-5 (Merkle Root validation successful)`);


        // 6. Validate each transaction's compliance data (e.g., SO2 limits)
        // This is where you'd integrate your `validateTransactionData` function.
        // For each transaction in newBlock.transactions:
        //   if (!validateTransactionData(tx)) {
        //     console.warn(`Node ${network.myNodeUrl}: Transaction ${tx.transactionId} in block ${newBlock.blockIndex} is non-compliant.`);
        //     // Decide: reject block or just flag transaction. For compliance, usually flag.
        //   }
        console.log(`Node ${network.myNodeUrl}: Passed Check-6 (Transaction compliance validation - placeholder)`);


        // 7. Add block to this node's blockchain
        await db.addBlockToBlockchain(newBlock);
        console.log(`Node ${network.myNodeUrl}: Block ${newBlock.blockIndex} added to local blockchain.`);


        // 8. Remove confirmed transactions from this node's mempool
        const confirmedTransactionIds = newBlock.transactions.map(tx => tx.transactionId);
        await db.removeTransactionsFromMempool(confirmedTransactionIds);
        console.log(`Node ${network.myNodeUrl}: Confirmed transactions removed from mempool.`);


        res.status(200).json({ note: 'Block received and accepted.' });

    } catch (error) {
        console.error(`Node ${network.myNodeUrl}: Error processing received block:`, error.message);
        res.status(500).json({ error: 'Failed to process received block.', details: error.message });
    }
});

// GET /api/blocks/chain
// Allows other nodes to request the full blockchain from this node.
router.get('/chain', async (req, res) => {
    console.log(`Node ${network.myNodeUrl}: Received request for full blockchain.`);
    try {
        const chain = await db.getAllBlocks();
        res.status(200).json({ chain });
    } catch (error) {
        console.error(`Node ${network.myNodeUrl}: Error fetching chain:`, error.message);
        res.status(500).json({ error: 'Failed to retrieve blockchain.', details: error.message });
    }
});


// Export both the router and the internal mining function
module.exports = {
    router,
    mineBlockInternal // Export the function so index.js can call it
};