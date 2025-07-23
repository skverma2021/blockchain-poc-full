// db.js
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
const crypto = require('crypto'); // Use Node's built-in crypto module for sha256
const { v4: uuidv4 } = require('uuid');

let theProj; // Stores the project ID (0 for RegAuth, 1 for ProjA, etc.)
let fileName; // Stores the database file name (e.g., projA.db)
let DB_FILE_PATH; // Stores the full path to the SQLite database file

// Global database instance for this module
let db;

/**
 * Sets the project ID for the current node. This is used when inserting transactions.
 * @param {string|number} projId The ID of the project/node.
 */
function setProjId(projId) {
    theProj = projId;
    console.log(`DB Module: Project ID set to ${theProj}`);
}

/**
 * Sets the database file name and ensures the 'data' directory exists.
 * This must be called before initDb().
 * @param {string} fname The name of the database file (e.g., 'projA.db').
 */
function setDbFile(fname) {
    fileName = fname;
    DB_FILE_PATH = path.join(__dirname, 'data', fileName);
    console.log(`DB Module: Using database file at: ${DB_FILE_PATH}`);

    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) {
        fs.mkdirSync(dataDir);
        console.log(`DB Module: Created data directory: ${dataDir}`);
    }
}

/**
 * Initializes the database connection and creates 'bchain', 'mempool_transactions',
 * and 'confirmed_transactions' tables if they do not exist.
 * Ensures a genesis block exists in 'bchain' for Regulator (projId '0').
 * @returns {Promise<sqlite3.Database>} A promise that resolves with the database object.
 */
function initDb() {
    return new Promise((resolve, reject) => {
        if (!DB_FILE_PATH) {
            return reject(new Error("DB Module: Database file path not set. Call setDbFile() first."));
        }

        db = new sqlite3.Database(DB_FILE_PATH, (err) => {
            if (err) {
                console.error('DB Module: Error opening database:', err.message);
                return reject(err);
            }
            console.log(`DB Module: Connected to SQLite database at ${DB_FILE_PATH}`);

            // Enable foreign key constraints (important for confirmed_transactions)
            db.run('PRAGMA foreign_keys = ON;', (pragmaErr) => {
                if (pragmaErr) {
                    console.error('DB Module: Error enabling foreign keys:', pragmaErr.message);
                    return reject(pragmaErr);
                }
                console.log('DB Module: Foreign key enforcement enabled.');
            });

            // Define table schemas
            const ensureBchain = `CREATE TABLE IF NOT EXISTS bchain (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                blockIndex INTEGER NOT NULL UNIQUE,
                timestamp TEXT NOT NULL,
                transactions TEXT NOT NULL, -- JSON string of transaction IDs/hashes in the block
                nonce INTEGER NOT NULL,
                hash TEXT NOT NULL,
                previousBlockHash TEXT NOT NULL,
                merkleRoot TEXT NOT NULL
            )`;

            const ensureMempoolTransactions = `CREATE TABLE IF NOT EXISTS mempool_transactions (
                internal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT UNIQUE NOT NULL, -- This is your UUID
                projId TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                submitter_id TEXT NOT NULL,
                station_id TEXT,
                so2 REAL,
                no2 REAL,
                pm10 REAL,
                pm2_5 REAL,
                raw_data_json TEXT NOT NULL, -- Stores the original raw transaction JSON string
                rowHash TEXT NOT NULL
            )`;

            const ensureConfirmedTransactions = `CREATE TABLE IF NOT EXISTS confirmed_transactions (
                internal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT UNIQUE NOT NULL, -- This is your UUID
                block_id INTEGER NOT NULL, -- Foreign key to bchain.id
                projId TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                submitter_id TEXT NOT NULL,
                station_id TEXT,
                so2 REAL,
                no2 REAL,
                pm10 REAL,
                pm2_5 REAL,
                raw_data_json TEXT NOT NULL,
                rowHash TEXT NOT NULL,
                FOREIGN KEY (block_id) REFERENCES bchain(id)
            )`;

            // Execute all table creations sequentially
            db.serialize(() => {
                db.run(ensureBchain, (err) => {
                    if (err) { console.error('DB Module: Error creating bchain table:', err.message); return reject(err); }
                    console.log('DB Module: Table "bchain" ensured to exist.');
                });
                db.run(ensureMempoolTransactions, (err) => {
                    if (err) { console.error('DB Module: Error creating mempool_transactions table:', err.message); return reject(err); }
                    console.log('DB Module: Table "mempool_transactions" ensured to exist.');
                });
                db.run(ensureConfirmedTransactions, (err) => {
                    if (err) { console.error('DB Module: Error creating confirmed_transactions table:', err.message); return reject(err); }
                    console.log('DB Module: Table "confirmed_transactions" ensured to exist.');
                });

                // Check and create Genesis Block (only for RegAuth, projId '0')
                db.get(`SELECT COUNT(*) AS count FROM bchain`, [], (err, row) => {
                    if (err) { console.error('DB Module: Error checking genesis block:', err.message); return reject(err); }

                    if (row.count > 0) {
                        console.log('DB Module: Genesis Block already exists.');
                        resolve(db);
                    } else if (String(theProj) === '0') { // Only RegAuth (ID '0') creates the genesis block
                        console.log('DB Module: Creating Genesis Block for Regulator node...');
                        const genesisBlock = {
                            blockIndex: 0,
                            timestamp: new Date().toISOString(),
                            transactions: '[]', // No actual transactions in genesis block
                            nonce: 0,
                            hash: crypto.createHash('sha256').update('regulator_genesis_block_v1').digest('hex'), // A unique, fixed hash for genesis
                            previousBlockHash: '0', // Standard for genesis
                            merkleRoot: crypto.createHash('sha256').update('genesis_merkle_root_v1').digest('hex') // Merkle root for empty transactions
                        };

                        const insertGenesis = `
                            INSERT INTO bchain (blockIndex, timestamp, transactions, nonce, hash, previousBlockHash, merkleRoot)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        `;
                        db.run(insertGenesis, [
                            genesisBlock.blockIndex,
                            genesisBlock.timestamp,
                            genesisBlock.transactions,
                            genesisBlock.nonce,
                            genesisBlock.hash,
                            genesisBlock.previousBlockHash,
                            genesisBlock.merkleRoot
                        ], function (err) {
                            if (err) { console.error('DB Module: Error creating Genesis Block:', err.message); return reject(err); }
                            console.log('DB Module: Genesis Block created.');
                            resolve(db);
                        });
                    } else {
                        // For non-RegAuth nodes, genesis block will be received from RegAuth
                        console.log('DB Module: No genesis block found. Awaiting genesis block from RegAuth node.');
                        resolve(db);
                    }
                });
            });
        });
    });
}

/**
 * Closes the database connection.
 * @returns {Promise<void>} A promise that resolves when the database is closed.
 */
function closeDb() {
    return new Promise((resolve, reject) => {
        if (db) {
            db.close((err) => {
                if (err) {
                    console.error('DB Module: Error closing database:', err.message);
                    reject(err);
                } else {
                    console.log('DB Module: Database connection closed.');
                    resolve();
                }
            });
        } else {
            resolve(); // No database open
        }
    });
}

/**
 * Creates (Inserts) a new transaction record into the **mempool_transactions** table.
 * Uses async/await syntax for cleaner asynchronous flow.
 * It intelligently uses provided transactionId, timestamp, rowHash, projId if available (for received transactions)
 * or generates them (for new submissions).
 * @param {Object} transactionData The transaction object to insert.
 * @returns {Promise<Object>} A promise that resolves with the inserted transaction data.
 */
async function createTransaction(transactionData) {
    if (!db) {
        throw new Error("DB Module: Database not initialized. Call initDb() first.");
    }
    // Ensure projId is set either globally or provided in the transactionData
    if (theProj === undefined && transactionData.projId === undefined) {
        throw new Error("DB Module: Project ID not set for this node or provided in transaction data.");
    }

    // Prioritize received values, otherwise generate
    const finalTransactionId = transactionData.transactionId || uuidv4().split('-').join('');
    const finalTimestamp = transactionData.timestamp || new Date().toISOString();
    // Use provided projId from transactionData if available, otherwise use the global theProj
    const finalProjId = transactionData.projId !== undefined ? String(transactionData.projId) : String(theProj);

    // The rawDataJson should be the JSON.stringify of the *original* transaction data,
    // as it was submitted by the client. If it's already provided (from a broadcast), use it.
    // Otherwise, stringify the current transactionData (which is the raw input from a local submit).
    const rawDataForHash = transactionData.rawDataJson || JSON.stringify({
        submitterId: transactionData.submitterId,
        stationID: transactionData.stationID,
        SO2: transactionData.SO2,
        NO2: transactionData.NO2,
        PM10: transactionData.PM10,
        PM2_5: transactionData.PM2_5
    });

    // The rowHash is calculated from a consistent set of data
    const finalRowHash = transactionData.rowHash || crypto.createHash('sha256').update(finalTransactionId + finalTimestamp + rawDataForHash).digest('hex');

    // Destructure core fields from the original transactionData for database columns
    const { submitterId, stationID, SO2, NO2, PM10, PM2_5 } = transactionData;

    const sql = `INSERT INTO mempool_transactions
                 (projId, transaction_id, timestamp, submitter_id, station_id, so2, no2, pm10, pm2_5, raw_data_json, rowHash)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    try {
        await new Promise((resolve, reject) => {
            db.run(sql, [
                finalProjId,
                finalTransactionId,
                finalTimestamp,
                submitterId,
                stationID,
                SO2,
                NO2,
                PM10,
                PM2_5,
                rawDataForHash,
                finalRowHash
            ], function (err) {
                if (err) {
                    console.error('DB Module: Error inserting transaction into mempool:', err.message);
                    reject(err);
                } else {
                    console.log(`DB Module: Transaction inserted into mempool with ID: ${finalTransactionId}`);
                    resolve();
                }
            });
        });

        // Return the complete transaction data, including generated IDs/hashes and rawDataJson
        return {
            transactionId: finalTransactionId,
            timestamp: finalTimestamp,
            rowHash: finalRowHash,
            projId: finalProjId,
            rawDataJson: rawDataForHash, // Ensure this is always returned for broadcast
            submitterId, stationID, SO2, NO2, PM10, PM2_5 // Include original fields
        };
    } catch (error) {
        throw error; // Re-throw for the caller
    }
}

/**
 * Reads (Retrieves) all transaction records from the **confirmed_transactions** table.
 * @returns {Promise<Array<Object>>} A promise that resolves with an array of transaction objects.
 */
function readAllTransactions() {
    return new Promise((resolve, reject) => {
        if (!db) {
            return reject(new Error("DB Module: Database not initialized. Call initDb() first."));
        }
        const sql = `SELECT * FROM confirmed_transactions ORDER BY timestamp DESC`;
        db.all(sql, [], (err, rows) => {
            if (err) {
                console.error('DB Module: Error reading confirmed transactions:', err.message);
                reject(err);
            } else {
                const transactions = rows.map(row => ({
                    internal_id: row.internal_id,
                    transactionId: row.transaction_id,
                    block_id: row.block_id,
                    projId: row.projId,
                    timestamp: row.timestamp,
                    submitterId: row.submitter_id,
                    stationID: row.station_id,
                    SO2: row.so2,
                    NO2: row.no2,
                    PM10: row.pm10,
                    PM2_5: row.pm2_5,
                    fullData: JSON.parse(row.raw_data_json), // Parse original raw data
                    rowHash: row.rowHash
                }));
                resolve(transactions);
            }
        });
    });
}

/**
 * Gets the current count of transactions in the **mempool_transactions** table.
 * @returns {Promise<number>} A promise that resolves with the count.
 */
function getMempoolCount() {
    return new Promise((resolve, reject) => {
        if (!db) {
            return reject(new Error("DB Module: Database not initialized. Call initDb() first."));
        }
        db.get(`SELECT COUNT(*) AS count FROM mempool_transactions`, (err, row) => {
            if (err) {
                console.error('DB Module: Error getting mempool count:', err.message);
                reject(err);
            } else {
                resolve(row.count);
            }
        });
    });
}

/**
 * Retrieves a specified number of transactions from the **mempool_transactions** for block creation.
 * Returns the full transaction objects, parsed from raw_data_json, including generated IDs/hashes.
 * @param {number} limit The maximum number of transactions to retrieve.
 * @returns {Promise<Array<Object>>} A promise that resolves with an array of transactions.
 */
function getTransactionsForBlock(limit) {
    return new Promise((resolve, reject) => {
        if (!db) {
            return reject(new Error("DB Module: Database not initialized. Call initDb() first."));
        }
        // Order by timestamp to get the oldest transactions first (FIFO)
        db.all(`SELECT * FROM mempool_transactions ORDER BY timestamp ASC LIMIT ?`, [limit], (err, rows) => {
            if (err) {
                console.error('DB Module: Error getting transactions for block:', err.message);
                reject(err);
            } else {
                const transactions = rows.map(row => {
                    // Reconstruct the full transaction object as it was stored,
                    // ensuring all fields needed for hashing and storage are present.
                    return {
                        transactionId: row.transaction_id,
                        projId: row.projId,
                        timestamp: row.timestamp,
                        submitterId: row.submitter_id,
                        stationID: row.station_id,
                        SO2: row.so2,
                        NO2: row.no2,
                        PM10: row.pm10,
                        PM2_5: row.pm2_5,
                        rawDataJson: row.raw_data_json,
                        rowHash: row.rowHash
                    };
                });
                resolve(transactions);
            }
        });
    });
}

/**
 * Removes transactions from the **mempool_transactions** after they have been included in a block.
 * @param {Array<string>} transactionIds An array of transactionId strings to remove.
 * @returns {Promise<number>} A promise that resolves with the number of rows deleted.
 */
function removeTransactionsFromMempool(transactionIds) {
    return new Promise((resolve, reject) => {
        if (!db) {
            return reject(new Error("DB Module: Database not initialized. Call initDb() first."));
        }
        if (transactionIds.length === 0) {
            return resolve(0);
        }
        const placeholders = transactionIds.map(() => '?').join(',');
        db.run(`DELETE FROM mempool_transactions WHERE transaction_id IN (${placeholders})`, transactionIds, function(err) {
            if (err) {
                console.error('DB Module: Error removing transactions from mempool:', err.message);
                reject(err);
            } else {
                console.log(`DB Module: Removed ${this.changes} transactions from mempool.`);
                resolve(this.changes);
            }
        });
    });
}

/**
 * Adds a confirmed block to the 'bchain' table and its transactions to 'confirmed_transactions'.
 * This operation is wrapped in a database transaction for atomicity.
 * @param {Object} block The block object to add, including its transactions array.
 * @returns {Promise<Object>} A promise that resolves with the added block.
 */
function addBlockToBlockchain(block) {
    return new Promise((resolve, reject) => {
        if (!db) {
            return reject(new Error("DB Module: Database not initialized. Call initDb() first."));
        }

        const { blockIndex, timestamp, transactions, nonce, hash, previousBlockHash, merkleRoot } = block;

        // Store a lightweight representation of transactions in the block metadata (e.g., just IDs and hashes)
        const lightweightTransactions = transactions.map(tx => ({
            transactionId: tx.transactionId,
            rowHash: tx.rowHash
        }));
        const transactionsJson = JSON.stringify(lightweightTransactions);

        db.serialize(() => { // Use serialize to ensure sequential operations within this section
            db.run("BEGIN TRANSACTION;", (beginErr) => {
                if (beginErr) {
                    console.error('DB Module: Error beginning transaction:', beginErr.message);
                    return reject(beginErr);
                }
            });

            // 1. Insert block metadata into 'bchain' table
            const insertBlockSql = `INSERT INTO bchain
                                    (blockIndex, timestamp, transactions, nonce, hash, previousBlockHash, merkleRoot)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)`;
            db.run(insertBlockSql, [
                blockIndex,
                timestamp,
                transactionsJson,
                nonce,
                hash,
                previousBlockHash,
                merkleRoot
            ], function(err) {
                if (err) {
                    db.run("ROLLBACK;", () => console.error('DB Module: Transaction rolled back due to block insertion error.'));
                    console.error('DB Module: Error inserting block into bchain:', err.message);
                    return reject(err);
                }
                const block_id = this.lastID; // Get the ID of the newly inserted block in 'bchain'

                // 2. Insert each transaction into the 'confirmed_transactions' table
                const insertTxPromises = transactions.map(tx => {
                    return new Promise((res, rej) => {
                        // Ensure all necessary fields are available in the 'tx' object
                        const { transactionId, projId, timestamp, submitterId, stationID, SO2, NO2, PM10, PM2_5, rawDataJson, rowHash } = tx;
                        const sql = `INSERT INTO confirmed_transactions
                                     (transaction_id, block_id, projId, timestamp, submitter_id, station_id, so2, no2, pm10, pm2_5, raw_data_json, rowHash)
                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                        db.run(sql, [
                            transactionId, block_id, projId, timestamp, submitterId, stationID, SO2, NO2, PM10, PM2_5, rawDataJson, rowHash
                        ], function(txErr) {
                            if (txErr) rej(txErr);
                            else res();
                        });
                    });
                });

                Promise.all(insertTxPromises)
                    .then(() => {
                        // 3. Commit the database transaction
                        db.run("COMMIT;", (commitErr) => {
                            if (commitErr) {
                                console.error('DB Module: Error committing block transaction:', commitErr.message);
                                db.run("ROLLBACK;", () => console.error('DB Module: Transaction rolled back due to commit error.'));
                                reject(commitErr);
                            } else {
                                console.log(`DB Module: Block (index ${blockIndex}) added to blockchain with ${transactions.length} transactions and committed.`);
                                resolve(block);
                            }
                        });
                    })
                    .catch(insertErr => {
                        console.error('DB Module: Error inserting confirmed transactions:', insertErr.message);
                        db.run("ROLLBACK;", () => console.error('DB Module: Transaction rolled back due to confirmed transactions insertion error.'));
                        reject(insertErr);
                    });
            });
        });
    });
}

/**
 * Gets the last block from the 'bchain' table.
 * @returns {Promise<Object|null>} A promise that resolves with the last block object, or null if no blocks exist.
 */
function getLastBlock() {
    return new Promise((resolve, reject) => {
        if (!db) {
            return reject(new Error("DB Module: Database not initialized. Call initDb() first."));
        }
        db.get(`SELECT * FROM bchain ORDER BY blockIndex DESC LIMIT 1`, [], (err, row) => {
            if (err) {
                console.error('DB Module: Error getting last block:', err.message);
                reject(err);
            } else {
                if (row) {
                    // Parse the transactions JSON string back into an array of objects
                    row.transactions = JSON.parse(row.transactions);
                    resolve(row);
                } else {
                    // This case should ideally not happen if Genesis block is always created for RegAuth
                    resolve(null);
                }
            }
        });
    });
}

/**
 * Retrieves all blocks from the 'bchain' table, ordered by blockIndex.
 * @returns {Promise<Array<Object>>} A promise that resolves with an array of block objects.
 */
function getAllBlocks() {
    return new Promise((resolve, reject) => {
        if (!db) {
            return reject(new Error("DB Module: Database not initialized. Call initDb() first."));
        }
        db.all(`SELECT * FROM bchain ORDER BY blockIndex ASC`, [], (err, rows) => {
            if (err) {
                console.error('DB Module: Error getting all blocks:', err.message);
                reject(err);
            } else {
                const chain = rows.map(row => {
                    // Parse the transactions JSON string back into an array of objects
                    row.transactions = JSON.parse(row.transactions);
                    return row;
                });
                resolve(chain);
            }
        });
    });
}

module.exports = {
    initDb,
    closeDb,
    setProjId,
    setDbFile,
    createTransaction,
    readAllTransactions,
    getMempoolCount,
    getTransactionsForBlock,
    removeTransactionsFromMempool,
    addBlockToBlockchain,
    getLastBlock,
    getAllBlocks
};