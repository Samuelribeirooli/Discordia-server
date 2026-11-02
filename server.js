// Chamada com Amigos — servidor sem dependências externas.
// Serve a interface, faz a sinalização WebRTC (WebSocket próprio) e, se achar o cloudflared, abre um link público HTTPS.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawn } = require('child_process');

const PORT = Number(process.env.PORT) || 3000;
const MAX_POR_SALA = 8; // malha P2P: ideal até ~6-8 pessoas
const salas = new Map(); // sala -> Map(id -> cliente)
let urlPublica = process.env.PUBLIC_URL || null;

// ---------- HTML (embutido no executável quando empacotado) ----------
let HTML = null;
try {
  const sea = require('node:sea');
  if (sea.isSea()) HTML = sea.getAsset('index.html', 'utf8');
} catch {}
if (!HTML) HTML = fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8');

// ---------- HTTP ----------
const server = http.createServer((req, res) => {
  if (req.url.startsWith('/config.json')) {
    const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun1.l.google.com:19302' }];
    if (process.env.TURN_URL) {
      iceServers.push({ urls: process.env.TURN_URL, username: process.env.TURN_USER, credential: process.env.TURN_PASS });
    }
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ iceServers, publicUrl: urlPublica }));
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(HTML);
});

// ---------- WebSocket mínimo (RFC 6455) ----------
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function quadro(dados, opcode = 1) {
  const p = Buffer.isBuffer(dados) ? dados : Buffer.from(dados);
  let h;
  if (p.length < 126) h = Buffer.from([0x80 | opcode, p.length]);
  else if (p.length < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | opcode; h[1] = 126; h.writeUInt16BE(p.length, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | opcode; h[1] = 127; h.writeBigUInt64BE(BigInt(p.length), 2); }
  return Buffer.concat([h, p]);
}

class Cliente {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.partes = []; this.vivo = true; this.aberto = true;
    this.onmsg = null; this.onclose = null;
    socket.on('data', (d) => { this.buf = Buffer.concat([this.buf, d]); this.ler(); });
    socket.on('close', () => this.fechado());
    socket.on('error', () => this.fechado());
  }
  ler() {
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = !!(b0 & 0x80), op = b0 & 0x0f, mascara = !!(b1 & 0x80);
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) return this.socket.destroy();
      const total = off + (mascara ? 4 : 0) + len;
      if (this.buf.length < total) return;
      let p = Buffer.from(this.buf.subarray(off + (mascara ? 4 : 0), total));
      if (mascara) { const m = this.buf.subarray(off, off + 4); for (let i = 0; i < p.length; i++) p[i] ^= m[i & 3]; }
      this.buf = this.buf.subarray(total);

      if (op === 8) { this.socket.end(quadro(Buffer.alloc(0), 8)); return this.fechado(); }
      if (op === 9) { this.socket.write(quadro(p, 10)); continue; }
      if (op === 10) { this.vivo = true; continue; }
      if (op === 1 || op === 2) this.partes = [p]; else if (op === 0) this.partes.push(p);
      if (fin && this.partes.length) {
        const texto = Buffer.concat(this.partes).toString('utf8'); this.partes = [];
        this.onmsg && this.onmsg(texto);
      }
    }
  }
  send(obj) { if (this.aberto) this.socket.write(quadro(JSON.stringify(obj))); }
  ping() { if (this.aberto) this.socket.write(quadro(Buffer.alloc(0), 9)); }
  fechado() {
    if (!this.aberto) return;
    this.aberto = false; this.socket.destroy();
    this.onclose && this.onclose();
  }
}

server.on('upgrade', (req, socket) => {
  const chave = req.headers['sec-websocket-key'];
  if (!chave) return socket.destroy();
  const aceite = crypto.createHash('sha1').update(chave + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + aceite + '\r\n\r\n');
  socket.setNoDelay(true);
  ligar(new Cliente(socket));
});

// ---------- Salas e sinalização ----------
const RETRANSMITIR = new Set(['estado', 'reacao']);

function ligar(c) {
  c.onmsg = (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const sala = salas.get(c.sala);

    if (m.type === 'join' && !c.sala) {
      const nomeSala = String(m.sala || '').trim().toLowerCase().replace(/[^a-z0-9-_]/g, '').slice(0, 40);
      if (!nomeSala) return c.send({ type: 'erro', text: 'Nome de sala inválido.' });
      if (!salas.has(nomeSala)) salas.set(nomeSala, new Map());
      const membros = salas.get(nomeSala);
      if (membros.size >= MAX_POR_SALA) return c.send({ type: 'erro', text: 'Essa sala está cheia.' });
      c.id = crypto.randomUUID().slice(0, 8);
      c.nome = String(m.nome || 'Anônimo').trim().slice(0, 24) || 'Anônimo';
      c.sala = nomeSala;
      const existentes = [...membros.values()].map((x) => ({ id: x.id, nome: x.nome }));
      membros.set(c.id, c);
      c.send({ type: 'joined', id: c.id, peers: existentes });
      membros.forEach((x) => x !== c && x.send({ type: 'peer-joined', id: c.id, nome: c.nome }));
      return;
    }
    if (!sala) return;

    if (m.type === 'signal') {
      const alvo = sala.get(m.to);
      if (alvo) alvo.send({ type: 'signal', from: c.id, data: m.data });
    } else if (m.type === 'chat') {
      const texto = String(m.text || '').slice(0, 500);
      if (texto) sala.forEach((x) => x.send({ type: 'chat', from: c.id, nome: c.nome, text: texto }));
    } else if (RETRANSMITIR.has(m.type)) {
      sala.forEach((x) => x !== c && x.send({ type: m.type, from: c.id, data: m.data }));
    }
  };
  c.onclose = () => {
    const sala = salas.get(c.sala);
    if (!sala) return;
    sala.delete(c.id);
    sala.forEach((x) => x.send({ type: 'peer-left', id: c.id }));
    if (sala.size === 0) salas.delete(c.sala);
  };
  c.vivo = true;
  c.timer = setInterval(() => { if (!c.vivo) { clearInterval(c.timer); return c.fechado(); } c.vivo = false; c.ping(); }, 30000);
}

// ---------- Link público (opcional, via cloudflared) ----------
function iniciarTunel() {
  const exe = process.platform === 'win32' ? 'cloudflared.exe' : 'cloudflared';
  const candidatos = [
    path.join(path.dirname(process.execPath), exe),
    path.join(process.cwd(), exe),
    exe, 'cloudflared',
  ];
  const tentar = (i) => {
    if (i >= candidatos.length) {
      console.log('\n  Para chamar amigos pela internet, baixe o "cloudflared" (arquivo único) e coloque na mesma pasta deste programa:');
      console.log('  https://github.com/cloudflare/cloudflared/releases/latest  → depois abra o programa de novo.\n');
      return;
    }
    let achou = false;
    const p = spawn(candidatos[i], ['tunnel', '--url', `http://localhost:${PORT}`, '--no-autoupdate'], { stdio: ['ignore', 'pipe', 'pipe'] });
    p.on('error', () => tentar(i + 1));
    const dados = (d) => {
      const r = String(d).match(/https:\/\/[a-z0-9-]+\.trycloudflare\.com/);
      if (r && !achou) {
        achou = true; urlPublica = r[0];
        console.log('\n  ✅ Link para seus amigos (HTTPS):  ' + urlPublica + '\n');
      }
    };
    p.stdout.on('data', dados); p.stderr.on('data', dados);
    process.on('exit', () => { try { p.kill(); } catch {} });
  };
  if (!urlPublica) tentar(0);
}

function abrirNavegador(url) {
  try {
    if (process.platform === 'win32') spawn('cmd', ['/c', 'start', '', url], { stdio: 'ignore', detached: true }).unref();
    else if (process.platform === 'darwin') spawn('open', [url], { stdio: 'ignore', detached: true }).unref();
    else spawn('xdg-open', [url], { stdio: 'ignore', detached: true }).unref();
  } catch {}
}

server.listen(PORT, () => {
  console.log('\n  📹 Chamada com Amigos');
  console.log(`  Rodando em http://localhost:${PORT}   (feche esta janela para encerrar)`);
  if (!process.env.NO_BROWSER) abrirNavegador(`http://localhost:${PORT}`);
  iniciarTunel();
});
server.on('error', (e) => { console.error(e.code === 'EADDRINUSE' ? `A porta ${PORT} já está em uso. Feche o outro programa ou use PORT=3001.` : e); process.exit(1); });
