'use strict';
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const PORT = Number(process.env.PORT || 10000);
const MAX_ROOMS = Number(process.env.MAX_ROOMS || 500);
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 250);
const rooms = new Map();
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ name: 'PROJETO FIRE', status: 'online', rooms: rooms.size, clients: wss.clients.size }));
  }
  res.writeHead(404); res.end('Not found');
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024, perMessageDeflate: false });
function send(ws, data) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); }
function snapshot(room) {
  const obj = {};
  for (const [id, client] of room.clients) obj[id] = client.presence || {};
  return { t: 's', o: obj };
}
function broadcast(room, data, except) {
  for (const client of room.clients.values()) if (client.ws !== except) send(client.ws, data);
}
function remove(ws) {
  const room = ws.room;
  if (!room) return;
  room.clients.delete(ws.id);
  ws.room = null;
  if (room.clients.size === 0) rooms.delete(room.code);
  else broadcast(room, { t: 'l', i: ws.id });
}
wss.on('connection', (ws) => {
  if (wss.clients.size > MAX_CLIENTS) { send(ws, { t: 'err', m: 'Servidor cheio' }); return ws.close(); }
  ws.id = Math.random().toString(36).slice(2, 12);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { t: 'err', m: 'Mensagem inválida' }); }
    if (msg.t === 'join') {
      const code = String(msg.c || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);
      const role = msg.r === 'host' ? 'host' : msg.r === 'guest' ? 'guest' : '';
      if (!code || !role) return send(ws, { t: 'err', m: 'Sala ou função inválida' });
      if (ws.room) remove(ws);
      let room = rooms.get(code);
      if (!room) {
        if (rooms.size >= MAX_ROOMS) return send(ws, { t: 'err', m: 'Servidor ocupado, tente novamente' });
        if (role !== 'host') return send(ws, { t: 'err', m: 'Sala não encontrada' });
        room = { code, clients: new Map() }; rooms.set(code, room);
      }
      if (room.clients.size >= 2) return send(ws, { t: 'err', m: 'Sala cheia' });
      if (role === 'host' && room.clients.size > 0) return send(ws, { t: 'err', m: 'Código já está em uso' });
      if (role === 'guest' && ![...room.clients.values()].some(c => c.role === 'host')) return send(ws, { t: 'err', m: 'Sala não encontrada' });
      ws.room = room; ws.role = role; ws.presence = {};
      room.clients.set(ws.id, { ws, role, presence: {} });
      send(ws, { t: 'ok' });
      send(ws, snapshot(room));
      broadcast(room, { t: 's', o: { [ws.id]: {} } }, ws);
      return;
    }
    if (!ws.room) return;
    if (msg.t === 'p' && msg.d && typeof msg.d === 'object' && !Array.isArray(msg.d)) {
      const clean = {};
      for (const [k, v] of Object.entries(msg.d)) {
        if (typeof k === 'string' && k.length <= 40 && ['string','number','boolean'].includes(typeof v)) clean[k] = typeof v === 'string' ? v.slice(0, 300) : v;
      }
      const entry = ws.room.clients.get(ws.id);
      if (!entry) return;
      entry.presence = { ...entry.presence, ...clean };
      broadcast(ws.room, { t: 'p', i: ws.id, d: clean }, ws);
    }
  });
  ws.on('close', () => remove(ws));
  ws.on('error', () => remove(ws));
});
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { remove(ws); ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));
server.listen(PORT, '0.0.0.0', () => console.log(`PROJETO FIRE WebSocket listening on ${PORT}`));
