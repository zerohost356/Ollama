const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { spawn, exec } = require('child_process');
const path = require('path');
const os = require('os');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

const PORT = process.env.PORT || 5000;
const OLLAMA_BASE = 'http://127.0.0.1:11434';

// ── Static files ────────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ── Ollama helpers ───────────────────────────────────────────────────────────
function ollamaRequest(endpoint, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(OLLAMA_BASE + endpoint);
    const http_ = require('http');
    const data = JSON.stringify(body);
    const req = http_.request({
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) }
    }, (res) => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => resolve({ status: res.statusCode, body: raw }));
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function ollamaStream(endpoint, body, onChunk, onDone, onError) {
  const http_ = require('http');
  const url = new URL(OLLAMA_BASE + endpoint);
  const data = JSON.stringify(body);
  const req = http_.request({
    hostname: url.hostname,
    port: url.port,
    path: url.pathname,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) }
  }, (res) => {
    let buf = '';
    res.on('data', (chunk) => {
      buf += chunk.toString();
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const obj = JSON.parse(line);
          if (obj.message?.content) onChunk(obj.message.content);
          if (obj.response) onChunk(obj.response);
          if (obj.done) onDone();
        } catch (_) {}
      }
    });
    res.on('end', () => { if (buf.trim()) { try { const o = JSON.parse(buf); if (o.done) onDone(); } catch(_){} } onDone(); });
  });
  req.on('error', onError);
  req.write(data);
  req.end();
}

function ollamaGetJSON(endpoint) {
  return new Promise((resolve, reject) => {
    const http_ = require('http');
    const url = new URL(OLLAMA_BASE + endpoint);
    const req = http_.request({ hostname: url.hostname, port: url.port, path: url.pathname, method: 'GET' }, (res) => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.end();
  });
}

// ── REST: model list ─────────────────────────────────────────────────────────
app.get('/api/models', async (req, res) => {
  const data = await ollamaGetJSON('/api/tags');
  if (!data) return res.json({ models: [], error: 'Ollama not running' });
  res.json({ models: (data.models || []).map(m => m.name) });
});

// ── REST: pull model ─────────────────────────────────────────────────────────
app.post('/api/pull', (req, res) => {
  const { model } = req.body;
  if (!model) return res.status(400).json({ error: 'model required' });
  const pull = spawn('ollama', ['pull', model], { env: process.env });
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  pull.stdout.on('data', d => res.write(`data: ${d.toString().trim()}\n\n`));
  pull.stderr.on('data', d => res.write(`data: ${d.toString().trim()}\n\n`));
  pull.on('close', code => { res.write(`data: DONE:${code}\n\n`); res.end(); });
});

// ── REST: system info ────────────────────────────────────────────────────────
app.get('/api/sysinfo', (req, res) => {
  res.json({
    platform: os.platform(),
    arch: os.arch(),
    hostname: os.hostname(),
    cpus: os.cpus().length,
    totalMem: (os.totalmem() / 1024 / 1024 / 1024).toFixed(2) + ' GB',
    freeMem: (os.freemem() / 1024 / 1024 / 1024).toFixed(2) + ' GB',
    uptime: Math.floor(os.uptime() / 60) + ' min',
    node: process.version
  });
});

// ── WebSocket: terminal + AI ─────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  // Persistent shell per client
  let shell = null;
  let shellHistory = [];

  function startShell() {
    if (shell) { try { shell.kill(); } catch(_){} }
    shell = spawn('bash', [], {
      env: { ...process.env, TERM: 'xterm-256color', HOME: process.env.HOME || '/root' },
      cwd: process.env.HOME || '/root'
    });

    shell.stdout.on('data', (data) => {
      socket.emit('terminal:data', data.toString());
    });
    shell.stderr.on('data', (data) => {
      socket.emit('terminal:data', data.toString());
    });
    shell.on('close', (code) => {
      socket.emit('terminal:data', `\r\n[Shell exited with code ${code}. Restarting...]\r\n`);
      setTimeout(startShell, 1000);
    });

    // Initial prompt
    shell.stdin.write('export PS1="\\[\\033[1;32m\\]ai-cloud\\[\\033[0m\\]:\\[\\033[1;34m\\]\\w\\[\\033[0m\\]$ "\n');
    shell.stdin.write('clear\n');
  }

  startShell();

  socket.on('terminal:input', (data) => {
    if (shell && shell.stdin.writable) {
      shell.stdin.write(data);
    }
  });

  socket.on('terminal:resize', ({ cols, rows }) => {
    // node-pty not available; best-effort via stty
    if (shell) {
      try {
        exec(`stty cols ${cols} rows ${rows}`, { env: process.env });
      } catch(_) {}
    }
  });

  // AI chat (streaming)
  const chatHistories = {};

  socket.on('ai:chat', ({ sessionId, model, message, history }) => {
    const messages = [
      {
        role: 'system',
        content: `You are an expert AI assistant integrated into a cloud terminal. 
You help with shell commands, system administration, coding, debugging, and general questions.
Be concise, accurate, and helpful. Format code with markdown code blocks.
Current system: ${os.platform()} ${os.arch()}, Node.js ${process.version}.`
      },
      ...(history || []),
      { role: 'user', content: message }
    ];

    socket.emit('ai:start', { sessionId });

    ollamaStream(
      '/api/chat',
      { model, messages, stream: true },
      (chunk) => socket.emit('ai:chunk', { sessionId, chunk }),
      () => socket.emit('ai:done', { sessionId }),
      (err) => socket.emit('ai:error', { sessionId, error: err.message })
    );
  });

  // AI: run command suggestion
  socket.on('ai:suggest-cmd', ({ model, prompt }) => {
    const messages = [
      { role: 'system', content: 'You are a shell command expert. Given a description, return ONLY the shell command(s) to accomplish the task. No explanation, no markdown, just the raw command(s). If multiple commands are needed, separate with &&.' },
      { role: 'user', content: prompt }
    ];

    let full = '';
    ollamaStream(
      '/api/chat',
      { model, messages, stream: true },
      (chunk) => { full += chunk; socket.emit('ai:cmd-chunk', chunk); },
      () => socket.emit('ai:cmd-done', full.trim()),
      (err) => socket.emit('ai:cmd-error', err.message)
    );
  });

  // Ollama: start/stop
  socket.on('ollama:start', () => {
    const proc = spawn('ollama', ['serve'], { detached: true, stdio: 'ignore', env: process.env });
    proc.unref();
    setTimeout(() => socket.emit('ollama:status', { running: true }), 2000);
  });

  socket.on('ollama:check', async () => {
    const data = await ollamaGetJSON('/api/tags');
    socket.emit('ollama:status', { running: !!data, models: data ? (data.models || []).map(m => m.name) : [] });
  });

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
    if (shell) { try { shell.kill(); } catch(_){} }
  });
});

// ── Ollama binary path ────────────────────────────────────────────────────────
// Prefer system-installed ollama; fall back to a user-writable location.
const OLLAMA_BIN_DIR = process.env.HOME ? `${process.env.HOME}/.local/bin` : '/tmp';
const OLLAMA_BIN = `${OLLAMA_BIN_DIR}/ollama`;

function resolveOllamaBin() {
  return new Promise((resolve) => {
    exec('which ollama', (err, stdout) => {
      if (!err && stdout.trim()) return resolve(stdout.trim());
      // Check our local install path
      exec(`test -x "${OLLAMA_BIN}"`, (e) => {
        resolve(e ? null : OLLAMA_BIN);
      });
    });
  });
}

// ── Install Ollama if missing (no root required — direct binary download) ──────
function installOllama() {
  return new Promise(async (resolve) => {
    const existing = await resolveOllamaBin();
    if (existing) {
      console.log(`Ollama already installed at ${existing}.`);
      return resolve(existing);
    }

    console.log('Ollama not found — downloading binary...');
    const arch = os.arch() === 'arm64' ? 'arm64' : 'amd64';
    const url = `https://ollama.com/download/ollama-linux-${arch}`;

    exec(`mkdir -p "${OLLAMA_BIN_DIR}"`, () => {
      const cmd = `curl -fsSL "${url}" -o "${OLLAMA_BIN}" && chmod +x "${OLLAMA_BIN}"`;
      exec(cmd, { env: process.env }, (err, stdout, stderr) => {
        if (err) {
          console.error('Failed to download Ollama binary:', stderr || err.message);
          return resolve(null);
        }
        console.log(`Ollama binary installed at ${OLLAMA_BIN}.`);
        resolve(OLLAMA_BIN);
      });
    });
  });
}

// ── Start Ollama server ───────────────────────────────────────────────────────
function startOllama(bin) {
  return new Promise((resolve) => {
    if (!bin) {
      console.warn('Ollama binary not available — skipping server start.');
      return resolve();
    }
    exec('curl -s http://127.0.0.1:11434/api/tags', (err, stdout) => {
      if (!err && stdout.includes('models')) {
        console.log('Ollama already running.');
        return resolve();
      }
      console.log('Starting Ollama server...');
      // Ensure PATH includes our bin dir so ollama can find its own helpers
      const env = { ...process.env, PATH: `${OLLAMA_BIN_DIR}:${process.env.PATH || '/usr/bin:/bin'}` };
      const proc = spawn(bin, ['serve'], { detached: true, stdio: 'ignore', env });
      proc.on('error', (e) => console.error('Failed to start Ollama:', e.message));
      proc.unref();
      // Wait for server to come up
      setTimeout(resolve, 4000);
    });
  });
}

// ── Pull llama3.2 if not present ──────────────────────────────────────────────
function ensureModel(bin, model) {
  return new Promise((resolve) => {
    if (!bin) {
      console.warn('Ollama binary not available — skipping model pull.');
      return resolve();
    }
    ollamaGetJSON('/api/tags').then((data) => {
      const models = data ? (data.models || []).map(m => m.name) : [];
      const exists = models.some(n => n === model || n.startsWith(model + ':'));
      if (exists) {
        console.log(`Model ${model} already available.`);
        return resolve();
      }
      console.log(`Pulling model ${model}...`);
      const env = { ...process.env, PATH: `${OLLAMA_BIN_DIR}:${process.env.PATH || '/usr/bin:/bin'}` };
      const pull = spawn(bin, ['pull', model], { stdio: ['ignore', 'pipe', 'pipe'], env });
      pull.stdout.on('data', d => process.stdout.write(d));
      pull.stderr.on('data', d => process.stderr.write(d));
      pull.on('error', (e) => { console.error('Pull error:', e.message); resolve(); });
      pull.on('close', (code) => {
        console.log(code === 0 ? `Model ${model} pulled successfully.` : `Failed to pull ${model} (exit ${code}).`);
        resolve();
      });
    });
  });
}

// ── Bootstrap sequence ────────────────────────────────────────────────────────
async function bootstrap() {
  const bin = await installOllama();
  await startOllama(bin);
  await ensureModel(bin, 'llama3.2');
}

bootstrap().catch(e => console.error('Bootstrap error:', e.message));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 AI Cloud Terminal running on http://0.0.0.0:${PORT}`);
  console.log(`🤖 Ollama endpoint: ${OLLAMA_BASE}`);
  console.log(`📟 No API key required — all AI runs locally\n`);
});
