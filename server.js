// Порт-Мираж: сервер для совместной игры.
// Раздаёт index.html и пересылает состояние игроков по WebSocket (/ws).
// Зависимостей нет, нужен только Node.js 18+. Запуск: node server.js
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const MAX_CLIENTS = 64, MAX_MSG = 2000, MAX_RATE = 40; // сообщений в секунду на игрока
const clients = new Map();
let nextId = 1;

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, buf) => {
      if (err) { res.writeHead(500); res.end('index.html not found next to server.js'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(buf);
    });
  } else if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok, players: ' + clients.size);
  } else { res.writeHead(404); res.end('not found'); }
});

function frame(payload, op = 1) {
  const p = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  let h;
  if (p.length < 126) h = Buffer.from([0x80 | op, p.length]);
  else { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(p.length, 2); }
  return Buffer.concat([h, p]);
}
function send(c, text) { if (!c.sock.destroyed) c.sock.write(frame(text)); }
function broadcast(room, text, except) { for (const o of clients.values()) if (o !== except && o.room === room) send(o, text); }
function drop(c) {
  if (!clients.has(c.id)) return;
  clients.delete(c.id); c.sock.destroy();
  broadcast(c.room, JSON.stringify({ t: 'l', id: c.id }));
}
function onMessage(c, text) {
  const now = Date.now();
  if (now - c.win > 1000) { c.win = now; c.n = 0; }
  if (++c.n > MAX_RATE || text.length > MAX_MSG) return;
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || m.t !== 's' || !m.d || typeof m.d !== 'object') return;
  const out = JSON.stringify({ t: 's', id: c.id, d: m.d });
  c.last = out; broadcast(c.room, out, c);
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!req.url.startsWith('/ws') || !key || clients.size >= MAX_CLIENTS) { sock.destroy(); return; }
  let room = 'main';
  try { room = (new URL(req.url, 'http://x').searchParams.get('room') || 'main').slice(0, 32); } catch (e) {}
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { id: nextId++, sock, room, buf: Buffer.alloc(0), last: null, alive: true, win: 0, n: 0 };
  clients.set(c.id, c);
  for (const o of clients.values()) if (o !== c && o.room === room && o.last) send(c, o.last);
  sock.on('data', chunk => {
    c.buf = Buffer.concat([c.buf, chunk]);
    if (c.buf.length > 65536) { drop(c); return; }
    while (c.buf.length >= 2) {
      const b0 = c.buf[0], b1 = c.buf[1], op = b0 & 15;
      let len = b1 & 127, off = 2;
      if (len === 126) { if (c.buf.length < 4) break; len = c.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { drop(c); return; }
      if (!(b1 & 128)) { drop(c); return; }               // клиент обязан маскировать кадры
      if (c.buf.length < off + 4 + len) break;
      const mask = c.buf.subarray(off, off + 4), data = Buffer.from(c.buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
      c.buf = c.buf.subarray(off + 4 + len);
      if (op === 8) { drop(c); return; }
      else if (op === 9) c.sock.write(frame(data, 10));
      else if (op === 10) c.alive = true;
      else if (op === 1 && (b0 & 128)) onMessage(c, data.toString('utf8'));
    }
  });
  sock.on('close', () => drop(c));
  sock.on('error', () => drop(c));
});

// пинг раз в 25 секунд: держит соединение живым и убирает отвалившихся игроков
setInterval(() => {
  for (const c of [...clients.values()]) {
    if (!c.alive) { drop(c); continue; }
    c.alive = false; if (!c.sock.destroyed) c.sock.write(frame('', 9));
  }
}, 25000);

server.listen(PORT, () => console.log('Порт-Мираж запущен: http://localhost:' + PORT));
