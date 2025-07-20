// routes/network.js
const express = require('express');
const router = express.Router();
const axios = require('axios');

// myNodeUrl will be set from process.argv in index.js
let myNodeUrl = '';
let networkNodes = []; // All known peer nodes (excluding self)

// Setter for myNodeUrl, called from index.js
function setMyNodeUrl(url) {
    myNodeUrl = url;
    console.log(`Network Module: My Node URL set to ${myNodeUrl}`);
}

router.post('/register-and-broadcast-node', async (req, res) => {
    console.log(`Node ${myNodeUrl}: Registering and broadcasting new node...`);
    const newNodeUrl = req.body.newNodeUrl;

    if (!newNodeUrl) {
        return res.status(400).json({ error: 'newNodeUrl is required.' });
    }

    // Add the new node to this node's list if it's not already there and not self
    if (!networkNodes.includes(newNodeUrl) && newNodeUrl !== myNodeUrl) {
        networkNodes.push(newNodeUrl);
        console.log(`Node ${myNodeUrl}: Added ${newNodeUrl} to networkNodes.`);
    }

    // Broadcast this new node to all existing nodes in the networkNodes list
    const regPromises = networkNodes.map(existingNodeUrl => {
        if (existingNodeUrl !== newNodeUrl) { // Don't send back to the new node itself in this step
            console.log(`Node ${myNodeUrl}: Broadcasting ${newNodeUrl} to existing node ${existingNodeUrl}/network/register-node`);
            return axios.post(`${existingNodeUrl}/api/network/register-node`, { newNodeUrl })
                .catch(err => console.error(`Node ${myNodeUrl}: Error broadcasting to ${existingNodeUrl}: ${err.message}`));
        }
        return Promise.resolve(); // Resolve immediately if it's the new node itself
    });

    try {
        await Promise.all(regPromises);

        // Send list of all known nodes (including self) to the new node
        const allNetworkNodes = [...networkNodes, myNodeUrl];
        console.log(`Node ${myNodeUrl}: Sending bulk registration to new node ${newNodeUrl}/api/network/register-nodes-bulk with:`, allNetworkNodes);
        // await axios.post(`${newNodeUrl}/api/network/register-nodes-bulk`, {
        //     allNetworkNodes: allNetworkNodes
        // });
        await axios.post(`${newNodeUrl}/network/register-nodes-bulk`, {
            allNetworkNodes: allNetworkNodes
        });

        res.json({ note: 'New node registered with network and broadcasted.', networkNodes: networkNodes });
    } catch (err) {
        console.error(`Node ${myNodeUrl}: Error during register-and-broadcast-node: ${err.message}`);
        res.status(500).json({ error: 'Failed to register and broadcast node.', details: err.message });
    }
});


router.post('/register-node', (req, res) => {
    console.log(`Node ${myNodeUrl}: Received request to register node...`);
    const newNodeUrl = req.body.newNodeUrl;

    if (!newNodeUrl) {
        return res.status(400).json({ error: 'newNodeUrl is required.' });
    }

    if (!networkNodes.includes(newNodeUrl) && newNodeUrl !== myNodeUrl) {
        networkNodes.push(newNodeUrl);
        console.log(`Node ${myNodeUrl}: Added new node ${newNodeUrl}. Current networkNodes:`, networkNodes);
    } else {
        console.log(`Node ${myNodeUrl}: Node ${newNodeUrl} already known or is self.`);
    }
    res.json({ note: 'Node registered.' });
});

router.post('/register-nodes-bulk', (req, res) => {
    console.log(`Node ${myNodeUrl}: Received bulk registration request...`);
    const allNodes = req.body.allNetworkNodes;

    if (!Array.isArray(allNodes)) {
        return res.status(400).json({ error: 'allNetworkNodes must be an array.' });
    }

    allNodes.forEach(nodeUrl => {
        if (!networkNodes.includes(nodeUrl) && nodeUrl !== myNodeUrl) {
            networkNodes.push(nodeUrl);
            console.log(`Node ${myNodeUrl}: Added node ${nodeUrl} from bulk registration.`);
        }
    });
    console.log(`Node ${myNodeUrl}: Bulk registration successful. Current networkNodes:`, networkNodes);
    res.json({ note: 'Bulk registration successful.' });
});

module.exports = {
    router,
    myNodeUrl, // Exported for other modules to use (e.g., for logging)
    networkNodes, // Exported for other modules to use (e.g., for broadcasting)
    setMyNodeUrl // Export setter for index.js
};