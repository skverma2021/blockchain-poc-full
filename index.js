// index.js
const express = require('express');
const db = require('./db'); // Your db.js module
const transactionsRoutes = require('./routes/transactions');
const { router: blocksRouter, mineBlockInternal } = require('./routes/blocks'); // Destructure blocksRouter and mineBlockInternal
const network = require('./routes/network'); // Import network module
const cors = require('cors');
const axios = require('axios'); // For initial chain sync

const app = express();
// Command line arguments: node index.js <port> <myNodeUrl> <projId> <dbFileName>
const PORT = process.argv[2] || 3000;
const MY_NODE_URL = process.argv[3];
const REG_AUTH_ID = process.argv[4]; // '0' for RegAuth, '1' for ProjA, '2' for ProjB etc.
const DB_FILE_NAME = process.argv[5] || 'default.db';

// Set module-level variables in db and network modules
db.setProjId(REG_AUTH_ID);
db.setDbFile(DB_FILE_NAME);
network.setMyNodeUrl(MY_NODE_URL); // Set myNodeUrl in the network module

const MINE_THRESHOLD = 5; // Transactions to trigger a mine (for RegAuth)
let mineInterval; // To store the interval timer

// Middleware
app.use(cors()); // Enable CORS for all routes (important for React frontend)
app.use(express.json()); // Enable parsing of JSON request bodies

// Routes
app.use('/api/transactions', transactionsRoutes);
app.use('/api/blocks', blocksRouter); // Use the destructured router
app.use('/api/network', network.router);

// Basic root route
app.get('/', (req, res) => {
    res.send(`Welcome to Node ${PORT}! Role: ${REG_AUTH_ID === '0' ? 'Regulator' : 'Project'}. My URL: ${MY_NODE_URL}`);
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error(`Node ${MY_NODE_URL}: Global Error Handler:`, err.stack);
    res.status(500).send('Something broke!');
});

// Start the server and initialize the database
async function startServer() {
    try {
        await db.initDb(); // Initialize database connection and tables

        // --- Initial Chain Synchronization Logic (for non-RegAuth nodes) ---
        // Only attempt to sync if this is NOT the RegAuth node and it has no blocks
        const lastBlockOnThisChain = await db.getLastBlock();
        if (!lastBlockOnThisChain && String(REG_AUTH_ID) !== '0') {
            console.log(`Node ${MY_NODE_URL}: No local blockchain found. Attempting to sync from RegAuth...`);
            const regAuthUrl = 'http://localhost:3000'; // Assuming RegAuth is always on 3000 for PoC

            try {
                const response = await axios.get(`${regAuthUrl}/api/blocks/chain`);
                const fullChain = response.data.chain;

                if (fullChain && fullChain.length > 0) {
                    console.log(`Node ${MY_NODE_URL}: Received chain of length ${fullChain.length} from RegAuth.`);
                    // Add each block to this node's blockchain (starting from genesis)
                    for (const block of fullChain) {
                        // The addBlockToBlockchain function already handles atomicity and uniqueness checks
                        await db.addBlockToBlockchain(block);
                        // Also clear any transactions from mempool that are in these synchronized blocks
                        const confirmedTransactionIds = block.transactions.map(tx => tx.transactionId);
                        await db.removeTransactionsFromMempool(confirmedTransactionIds);
                    }
                    console.log(`Node ${MY_NODE_URL}: Blockchain synchronized successfully.`);
                } else {
                    console.log(`Node ${MY_NODE_URL}: RegAuth has no chain yet. Waiting for blocks.`);
                }
            } catch (syncError) {
                console.error(`Node ${MY_NODE_URL}: Error syncing chain from RegAuth:`, syncError.message);
                // Continue running, but node might be out of sync.
            }
        }
        // --- End Initial Chain Synchronization Logic ---


        // --- RegAuth Specific Mining Logic ---
        if (String(REG_AUTH_ID) === '0') { // Only RegAuth (node with ID '0') mines
            console.log(`Node ${MY_NODE_URL}: RegAuth node. Starting mining check interval...`);
            mineInterval = setInterval(async () => {
                try {
                    const mempoolCount = await db.getMempoolCount();
                    console.log(`Node ${MY_NODE_URL}: RegAuth Mempool Count: ${mempoolCount}`);
                    if (mempoolCount >= MINE_THRESHOLD) {
                        console.log(`Node ${MY_NODE_URL}: Mempool count reached ${MINE_THRESHOLD}. Triggering block mine...`);
                        await mineBlockInternal(); // Directly call the exported function
                    }
                } catch (error) {
                    console.error(`Node ${MY_NODE_URL}: Error during mining check:`, error.message);
                }
            }, 10 * 1000); // Check every 10 seconds (adjust as needed)
        }
        // --- End RegAuth Specific Logic ---

        app.listen(PORT, () => {
            console.log(`Node ${MY_NODE_URL}: Server running on port ${PORT}`);
            console.log(`Node ${MY_NODE_URL}: Access at ${MY_NODE_URL}`);
        });
    } catch (error) {
        console.error(`Node ${MY_NODE_URL}: Failed to start server:`, error);
        process.exit(1);
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log(`Node ${MY_NODE_URL}: Shutting down server...`);
    if (mineInterval) {
        clearInterval(mineInterval); // Clear the mining interval
    }
    await db.closeDb();
    process.exit(0);
});

startServer();