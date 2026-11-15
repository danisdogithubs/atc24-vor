const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const WS_URL = 'wss://24data.ptfs.app/wss';

let latestMainData = {};
let latestEventData = {};
let lastUpdate = null;
let upstreamConnected = false;

const sseClients = new Set();

function broadcast(payload) {
    const message = `data: ${JSON.stringify(payload)}\n\n`;
    for (const res of sseClients) {
        res.write(message);
    }
}

function connect24Data() {
    // Node's ws client omits the Origin header by default, satisfying 24data
    const ws = new WebSocket(WS_URL);

    ws.on('open', () => {
        upstreamConnected = true;
        console.log('[24data] Connected to live WebSocket stream.');
        broadcast({ type: 'STATUS', connected: true });
    });

    ws.on('message', (raw) => {
        try {
            const msg = JSON.parse(raw);
            if (msg.t === 'ACFT_DATA') {
                latestMainData = msg.d || {};
                lastUpdate = msg.s || new Date().toISOString();
                broadcast({
                    type: 'TELEMETRY',
                    server: 'MAIN',
                    timestamp: lastUpdate,
                    aircraft: latestMainData
                });
            } else if (msg.t === 'EVENT_ACFT_DATA') {
                latestEventData = msg.d || {};
                lastUpdate = msg.s || new Date().toISOString();
                broadcast({
                    type: 'TELEMETRY',
                    server: 'EVENT',
                    timestamp: lastUpdate,
                    aircraft: latestEventData
                });
            }
        } catch (err) {
            console.error('[24data] Parse error:', err.message);
        }
    });

    ws.on('close', (code) => {
        upstreamConnected = false;
        console.log(`[24data] Disconnected (${code}). Reconnecting in 5s...`);
        broadcast({ type: 'STATUS', connected: false });
        setTimeout(connect24Data, 5000);
    });

    ws.on('error', (err) => {
        console.error('[24data] WebSocket error:', err.message);
    });
}

const server = http.createServer((req, res) => {
    // CORS headers so VORtest.html works via localhost:3000 or direct file:// open
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET');

    if (req.url === '/events') {
        res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        });

        // Send initial snapshot immediately upon browser connect
        res.write(`data: ${JSON.stringify({
            type: 'INIT',
            connected: upstreamConnected,
            timestamp: lastUpdate,
            mainAircraft: latestMainData,
            eventAircraft: latestEventData
        })}\n\n`);

        sseClients.add(res);
        req.on('close', () => sseClients.delete(res));
        return;
    }

    if (req.url === '/' || req.url === '/VORtest.html') {
        const htmlPath = path.join(__dirname, 'VORtest.html');
        fs.readFile(htmlPath, (err, content) => {
            if (err) {
                res.writeHead(500, { 'Content-Type': 'text/plain' });
                res.end('Error loading VORtest.html. Make sure it is in the same folder as VORtest.js.');
                return;
            }
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(content);
        });
        return;
    }

    res.writeHead(404);
    res.end('Not Found');
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`VOR Bridge running on port ${PORT}`);
    connect24Data();
});