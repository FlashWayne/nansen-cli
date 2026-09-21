/**
 * Nansen CLI - Smart Alerts Daemon
 * WebSocket client that connects to the Nansen alerts stream.
 * Handles reconnect, backfill, state persistence, and action hooks.
 */

import { EventEmitter } from 'events';
import { spawn } from 'child_process';
import WebSocket from 'ws';
import fs from 'fs';
import path from 'path';
import os from 'os';

// ── Constants ─────────────────────────────────────────────────────────────────

export const DEFAULT_WS_URL = 'wss://api.nansen.ai/v1/smart-alert/stream';
export const DEFAULT_REST_URL = 'https://api.nansen.ai/api/v1/smart-alert/past-alerts';
const DEFAULT_BASE_DELAY_MS = 5_000;
const MAX_DELAY_MS = 300_000;
const PING_INTERVAL_MS = 30_000;
const PONG_TIMEOUT_MS = 10_000;
const HANDSHAKE_TIMEOUT_MS = 15_000;
const RECENT_ALERT_LIMIT = 50;

// ── Helpers ───────────────────────────────────────────────────────────────────

function jitter(maxMs = 2000) {
  return Math.floor(Math.random() * maxMs);
}

function backoffDelay(attempt, baseMs = DEFAULT_BASE_DELAY_MS) {
  const raw = baseMs * Math.pow(2, attempt) + jitter();
  return Math.min(raw, MAX_DELAY_MS);
}

/**
 * Atomically write a JSON file: write to .tmp then rename.
 * Permissions: 0600 (owner read/write only — state may contain session IDs).
 */
function writeJsonAtomic(filePath, data) {
  const tmp = filePath + '.tmp';
  fs.mkdirSync(path.dirname(filePath), { recursive: true, mode: 0o700 });
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), { mode: 0o600 });
  fs.renameSync(tmp, filePath);
}

function readJsonSafe(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

/**
 * Interpolate {placeholder} tokens in a command string using safe alert metadata.
 * Alert data payload is NOT interpolated — only top-level string fields.
 * This prevents any injection from alert content into the shell command.
 */
export function interpolateCommand(template, alert) {
  const firedAt = String(alert.firedAt ?? '');
  const firedAtTime = Date.parse(firedAt);
  const normalizedFiredAt = Number.isFinite(firedAtTime)
    ? new Date(firedAtTime).toISOString()
    : sanitizeShell(firedAt);
  return template
    .replace(/\{alertId\}/g, sanitizeShell(alert.alertId ?? ''))
    .replace(/\{alertName\}/g, sanitizeShell(alert.alertName ?? ''))
    .replace(/\{alertType\}/g, sanitizeShell(alert.alertType ?? ''))
    .replace(/\{firedAt\}/g, normalizedFiredAt);
}

/**
 * Strip characters that could break a shell command.
 * Placeholder substitutions land inside a shell string that we don't fully control,
 * so we only allow a safe subset: alphanumeric, dash, underscore, dot, colon, slash, @.
 */
function sanitizeShell(str) {
  return String(str).replace(/[^a-zA-Z0-9\-_.:/@]/g, '');
}

// ── AlertsDaemon ──────────────────────────────────────────────────────────────

export class AlertsDaemon extends EventEmitter {
  /**
   * @param {object}   opts
   * @param {string}   opts.apiKey       Nansen API key (required)
   * @param {string}   [opts.wsUrl]      WebSocket server URL
   * @param {string}   [opts.restUrl]    /past-alerts endpoint base URL
   * @param {string}   [opts.action]     Shell command template to run per alert
   * @param {boolean}  [opts.actionEnv]  Pass alert JSON as NANSEN_ALERT env var (vs stdin)
   * @param {boolean}  [opts.foreground] Emit alert NDJSON to stdout (direct run only)
   * @param {boolean}  [opts.backfill]   Fetch past alerts on (re)connect (default: true)
   * @param {string}   [opts.stateFile]  Path to state JSON
   * @param {string}   [opts.logFile]    Append logs to this file (null = stderr only)
   * @param {function} [opts.log]        Custom log function(level, message)
   * @param {function} [opts.WebSocket]  Injected WebSocket class (for testing)
   * @param {function} [opts.fetchFn]    Injected fetch (for testing)
   * @param {function} [opts.spawnFn]    Injected child-process spawn (for testing)
   */
  constructor(opts = {}) {
    super();
    if (!opts.apiKey) throw new Error('apiKey is required');

    this.apiKey = opts.apiKey;
    this.wsUrl = opts.wsUrl ?? DEFAULT_WS_URL;
    this.restUrl = opts.restUrl ?? DEFAULT_REST_URL;
    this.action = opts.action ?? null;
    this.actionEnv = opts.actionEnv ?? false;
    this.foreground = opts.foreground ?? false;
    this.backfill = opts.backfill ?? true;
    this.stateFile = opts.stateFile ?? path.join(os.homedir(), '.nansen', 'alerts-daemon-state.json');
    this.logFile = opts.logFile ?? null;
    this._logFn = opts.log ?? null;
    this._WebSocket = opts.WebSocket ?? null;
    this._fetch = opts.fetchFn ?? globalThis.fetch;
    this._spawn = opts.spawnFn ?? spawn;

    this._ws = null;
    this._running = false;
    this._reconnectAttempt = 0;
    this._pingTimer = null;
    this._pongTimer = null;
    this._cancelReconnectWait = null;
    this._connectionGeneration = 0;
    const storedState = readJsonSafe(this.stateFile);
    this._state = storedState && typeof storedState === 'object' && !Array.isArray(storedState)
      ? storedState
      : {};
    const recentAlertKeys = Array.isArray(this._state.recentAlertKeys)
      ? this._state.recentAlertKeys.filter((key) => typeof key === 'string').slice(-RECENT_ALERT_LIMIT)
      : [];
    if (this._state.lastAlertId && this._state.lastAlertAt) {
      const legacyLastKey = JSON.stringify([String(this._state.lastAlertId), String(this._state.lastAlertAt)]);
      if (!recentAlertKeys.includes(legacyLastKey)) recentAlertKeys.push(legacyLastKey);
    }
    if (recentAlertKeys.length > RECENT_ALERT_LIMIT) recentAlertKeys.shift();
    this._state.recentAlertKeys = recentAlertKeys;
    this._recentAlertKeys = new Set(recentAlertKeys);
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────────

  async start() {
    if (this._running) return;
    this._running = true;
    this.log('info', 'Daemon starting');
    await this._connectLoop();
  }

  stop() {
    this._running = false;
    this._cancelReconnectWait?.();
    this._clearTimers();
    if (this._ws) {
      try {
        this._ws.close(1000, 'daemon stopped');
      } catch {
        // The socket may already be closed.
      }
      this._ws = null;
    }
    this.log('info', 'Daemon stopped');
  }

  // ── Connection loop ──────────────────────────────────────────────────────────

  async _connectLoop() {
    while (this._running) {
      try {
        await this._connect();
      } catch (err) {
        this.log('error', `Connection error: ${err.message}`);
      }

      if (!this._running) break;

      const delay = backoffDelay(this._reconnectAttempt);
      this.log('info', `Reconnecting in ${Math.round(delay / 1000)}s (attempt ${this._reconnectAttempt + 1})`);
      this._reconnectAttempt++;
      await this._waitForReconnect(delay);
    }
  }

  _waitForReconnect(delay) {
    return new Promise((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (this._cancelReconnectWait === finish) this._cancelReconnectWait = null;
        resolve();
      };
      const timer = setTimeout(finish, delay);
      this._cancelReconnectWait = finish;
    });
  }

  async _connect() {
    const WS = this._WebSocket ?? WebSocket;
    const generation = ++this._connectionGeneration;

    return new Promise((resolve) => {
      let settled = false;
      const backfillController = new AbortController();
      const ws = new WS(this.wsUrl, {
        headers: { apikey: this.apiKey },
        handshakeTimeout: HANDSHAKE_TIMEOUT_MS,
      });
      this._ws = ws;
      const isCurrent = () => !settled && this._connectionGeneration === generation && this._ws === ws;
      const settle = () => {
        if (settled) return;
        settled = true;
        backfillController.abort();
        if (this._connectionGeneration === generation) this._connectionGeneration++;
        if (this._ws === ws) {
          this._clearTimers();
          this._ws = null;
        }
        resolve();
      };

      ws.on('open', async () => {
        if (!isCurrent()) return;
        this._reconnectAttempt = 0;
        this.log('info', `Connected to ${this.wsUrl}`);
        this._startPing();
        this._saveState({ startedAt: this._state.startedAt ?? new Date().toISOString() });

        if (this.backfill && this._state.lastAlertAt) {
          try {
            await this._fetchPastAlerts(this._state.lastAlertAt, {
              signal: backfillController.signal,
              shouldDispatch: isCurrent,
            });
          } catch (err) {
            if (isCurrent()) this.log('warn', `Backfill failed: ${err.message}`);
          }
        }
      });

      ws.on('message', (raw) => {
        if (!isCurrent()) return;
        let msg;
        try {
          msg = JSON.parse(raw.toString());
        } catch {
          this.log('warn', 'Received unparseable message, ignoring');
          return;
        }
        this._handleMessage(msg);
      });

      ws.on('close', (code, reason) => {
        this.log('info', `Connection closed (code=${code} reason=${reason?.toString() ?? ''})`);
        settle();
      });

      ws.on('error', (err) => {
        if (!isCurrent()) return;
        this.log('error', `WebSocket error: ${err.message}`);
        if (/Unexpected server response: (401|403)\b/.test(err.message)) {
          this._running = false;
        }
        try {
          if (typeof ws.terminate === 'function') ws.terminate();
          else ws.close();
        } catch { /* the reconnect loop handles this failed connection */ }
        // Some WebSocket implementations do not emit `close` after a failed
        // handshake. Always settle so the reconnect loop cannot hang forever.
        settle();
      });
    });
  }

  // ── Message handling ─────────────────────────────────────────────────────────

  _handleMessage(msg) {
    if (!msg || typeof msg !== 'object' || Array.isArray(msg)) {
      this.log('warn', 'Received invalid message, ignoring');
      return;
    }

    switch (msg.type) {
      case 'connected':
        this._saveState({ sessionId: msg.sessionId });
        this.log('info', `Session: ${msg.sessionId}`);
        this.emit('connected', msg);
        break;

      case 'alert': {
        if (!msg.alertId || !msg.firedAt) {
          this.log('warn', 'Received invalid alert, ignoring');
          break;
        }
        const alertKey = JSON.stringify([String(msg.alertId), String(msg.firedAt)]);
        if (
          this._recentAlertKeys.has(alertKey) ||
          (msg.alertId === this._state.lastAlertId && msg.firedAt === this._state.lastAlertAt)
        ) {
          this.log('debug', `Duplicate alert ignored: ${msg.alertId}`);
          break;
        }
        // Log only metadata — not alert data payload (may contain market-sensitive info)
        this.log('info', `Alert: [${msg.alertId}] ${msg.alertName} (${msg.alertType}) at ${msg.firedAt}`);
        const recentAlertKeys = [...this._recentAlertKeys, alertKey].slice(-RECENT_ALERT_LIMIT);
        this._recentAlertKeys = new Set(recentAlertKeys);
        const statePatch = { recentAlertKeys };
        const alertTime = Date.parse(msg.firedAt);
        const cursorTime = Date.parse(this._state.lastAlertAt);
        if (Number.isFinite(alertTime) && (!Number.isFinite(cursorTime) || alertTime >= cursorTime)) {
          statePatch.lastAlertAt = msg.firedAt;
          statePatch.lastAlertId = msg.alertId;
        }
        // Persist deduplication state before dispatch so a crash/restart cannot
        // invoke an action hook twice for the same alert.
        this._saveState(statePatch);
        this.emit('alert', msg);
        this._dispatchAlert(msg);
        break;
      }

      case 'pong':
        this._clearPongTimer();
        break;

      case 'error':
        this.log('error', `Server error [${msg.code}]: ${msg.message}`);
        this.emit('server-error', msg);
        if (msg.code === 'UNAUTHORIZED') {
          this._running = false; // auth failures are not recoverable
          this._ws?.close();
        }
        break;

      default:
        this.log('debug', `Unknown message type: ${msg.type}`);
    }
  }

  _dispatchAlert(alert) {
    // 1. Emit NDJSON only for an explicit foreground pipe-mode run.
    if (this.foreground) process.stdout.write(JSON.stringify(alert) + '\n');

    // 2. Run --action hook if configured
    if (!this.action) return;

    const cmd = interpolateCommand(this.action, alert);
    const alertJson = JSON.stringify(alert);
    const env = { ...process.env };

    if (this.actionEnv) {
      env.NANSEN_ALERT = alertJson;
    }

    try {
      const child = this._spawn('/bin/sh', ['-c', cmd], {
        env,
        stdio: [this.actionEnv ? 'ignore' : 'pipe', 'inherit', 'inherit'],
      });

      if (!this.actionEnv) {
        child.stdin.on('error', (err) => {
          this.log('error', `Action hook stdin error: ${err.message}`);
        });
        child.stdin.write(alertJson);
        child.stdin.end();
      }

      child.on('error', (err) => {
        this.log('error', `Action hook error: ${err.message}`);
      });

      child.on('close', (code) => {
        if (code !== 0) {
          this.log('warn', `Action hook exited ${code} for alert ${alert.alertId}`);
        }
      });
    } catch (err) {
      this.log('error', `Failed to spawn action hook: ${err.message}`);
    }
  }

  // ── Backfill ─────────────────────────────────────────────────────────────────

  async _fetchPastAlerts(since, { signal, shouldDispatch = () => true } = {}) {
    this.log('info', `Backfilling since ${since}`);
    const url = new URL(this.restUrl);
    url.searchParams.set('since', since);
    url.searchParams.set('limit', '50');

    const res = await this._fetch(url.toString(), {
      headers: { apikey: this.apiKey },
      signal,
    });

    if (!shouldDispatch()) return;

    if (!res.ok) {
      throw new Error(`past-alerts ${res.status} ${res.statusText}`);
    }

    const body = await res.json();
    if (!shouldDispatch()) return;
    if (!Array.isArray(body?.alerts)) {
      throw new Error('past-alerts returned an invalid response');
    }
    const alerts = body.alerts;

    if (alerts.length === 0) {
      this.log('info', 'No missed alerts in backfill window');
      return;
    }

    this.log('info', `Replaying ${alerts.length} missed alert(s)`);
    for (const alert of alerts) {
      if (!shouldDispatch()) return;
      const alertTime = Date.parse(alert?.firedAt);
      const sinceTime = Date.parse(since);
      if (Number.isFinite(alertTime) && Number.isFinite(sinceTime) && alertTime < sinceTime) {
        this.log('debug', `Already-processed backfill alert ignored: ${alert.alertId ?? 'unknown'}`);
        continue;
      }
      this._handleMessage(alert);
    }
  }

  // ── Keepalive ────────────────────────────────────────────────────────────────

  _startPing() {
    this._clearTimers();
    this._pingTimer = setInterval(() => {
      if (this._ws?.readyState === 1 /* OPEN */) {
        this._ws.send(JSON.stringify({ type: 'ping', ts: Date.now() }));
        this._schedulePongTimeout();
      }
    }, PING_INTERVAL_MS);
  }

  _schedulePongTimeout() {
    this._clearPongTimer();
    this._pongTimer = setTimeout(() => {
      this._pongTimer = null;
      this.log('warn', 'Pong timeout — forcing reconnect');
      this._ws?.close();
    }, PONG_TIMEOUT_MS);
  }

  _clearPongTimer() {
    if (this._pongTimer) { clearTimeout(this._pongTimer); this._pongTimer = null; }
  }

  _clearTimers() {
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    this._clearPongTimer();
  }

  // ── State ────────────────────────────────────────────────────────────────────

  _saveState(patch) {
    this._state = { ...this._state, ...patch };
    try {
      writeJsonAtomic(this.stateFile, this._state);
    } catch (err) {
      this.log('warn', `State file write failed: ${err.message}`);
    }
  }

  // ── Logging ──────────────────────────────────────────────────────────────────

  log(level, message) {
    const line = `[${new Date().toISOString()}] [${level.toUpperCase()}] ${message}`;
    if (this._logFn) {
      this._logFn(level, message);
    } else {
      process.stderr.write(line + '\n');
    }
    if (this.logFile) {
      try {
        fs.mkdirSync(path.dirname(this.logFile), { recursive: true, mode: 0o700 });
        fs.appendFileSync(this.logFile, line + '\n', { mode: 0o600 });
      } catch (err) {
        process.stderr.write(`Failed to write daemon log: ${err.message}\n`);
      }
    }
    this.emit('log', { level, message, line });
  }
}

export default AlertsDaemon;
