/**
 * Nansen CLI - Alerts daemon subcommand
 *
 * nansen alerts daemon <start|stop|status|logs|run> [options]
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import { AlertsDaemon } from '../daemon/alerts-daemon.js';

const NANSEN_DIR = path.join(os.homedir(), '.nansen');
const DEFAULT_PID_FILE = path.join(NANSEN_DIR, 'alerts-daemon.pid');
const DEFAULT_LOG_FILE = path.join(NANSEN_DIR, 'alerts-daemon.log');
const DEFAULT_STATE_FILE = path.join(NANSEN_DIR, 'alerts-daemon-state.json');
const STOP_POLL_INTERVAL_MS = 50;
const STOP_POLL_ATTEMPTS = 100;
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '0.0.0.0', '[::1]']);

const DAEMON_HELP = `nansen alerts daemon — Listen to Smart Alert events in real-time

SUBCOMMANDS:
  run     Run in foreground (pipe mode — alerts emitted as NDJSON on stdout)
  start   Start daemon in background (writes PID file)
  stop    Stop the running daemon
  status  Show daemon state (running/stopped, last alert, PID)
  logs    Show the last 50 daemon log lines

OPTIONS:
  --action <cmd>          Shell command to run per alert.
                          Alert JSON passed on stdin. Supports {alertId}, {alertName},
                          {alertType}, {firedAt} placeholder substitutions.
  --action-env            Pass alert JSON as NANSEN_ALERT env var instead of stdin
  --no-backfill           Skip past-alert backfill on (re)connect
  --ws-url <url>          Override WebSocket server URL
  --rest-url <url>        Override REST backfill URL (required for non-standard WS paths)
  --state-file <path>     Path to state JSON (default: ~/.nansen/alerts-daemon-state.json)
  --pid-file <path>       Path to PID file (default: ~/.nansen/alerts-daemon.pid)
  --log-file <path>       Path to log file (default: ~/.nansen/alerts-daemon.log)

EXAMPLES:
  # Print all alerts as JSON (pipe to jq, agent, etc.)
  nansen alerts daemon run

  # Pipe to OpenClaw
  nansen alerts daemon run | openclaw inject

  # Start background daemon, wake up OpenClaw per alert
  nansen alerts daemon start --action 'openclaw inject --message "Alert: {alertName}"'

  # Run against local mock server (for development)
  nansen alerts daemon run --ws-url ws://localhost:9876/v1/smart-alert/stream

  # Check daemon status
  nansen alerts daemon status

  # Stop daemon
  nansen alerts daemon stop
`;

function readPid(pidFile) {
  try {
    const pid = Number(fs.readFileSync(pidFile, 'utf8').trim());
    return Number.isSafeInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function isProcessRunning(pid, killFn = process.kill.bind(process)) {
  if (!pid) return false;
  try {
    killFn(pid, 0); // signal 0 = just check existence
    return true;
  } catch (err) {
    return err?.code === 'EPERM';
  }
}

function removePidFileIfMatches(pidFile, expectedPid) {
  try {
    if (readPid(pidFile) === expectedPid) fs.unlinkSync(pidFile);
  } catch {
    // The PID file may already have been removed by `stop`.
  }
}

function validateServiceUrl(raw, optionName, secureProtocol, localProtocol) {
  let url;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`--${optionName} must be a valid URL`);
  }
  if (url.protocol !== secureProtocol && !(url.protocol === localProtocol && LOCAL_HOSTS.has(url.hostname))) {
    throw new Error(`--${optionName} must use ${secureProtocol}// (${localProtocol}// is allowed only for localhost)`);
  }
  return url;
}

export function resolveRestUrl(wsUrl, explicitRestUrl, backfill) {
  if (explicitRestUrl) return explicitRestUrl;
  if (!wsUrl || !backfill) return undefined;

  const url = new URL(wsUrl);
  if (url.pathname !== '/v1/smart-alert/stream') {
    throw new Error('--rest-url is required when --ws-url does not use /v1/smart-alert/stream (or pass --no-backfill)');
  }
  url.protocol = url.protocol === 'wss:' ? 'https:' : 'http:';
  url.pathname = '/api/v1/smart-alert/past-alerts';
  url.search = '';
  url.hash = '';
  return url.toString();
}

function acquireLifecycleLock(pidFile, killFn) {
  const lockFile = `${pidFile}.lock`;
  fs.mkdirSync(path.dirname(pidFile), { recursive: true, mode: 0o700 });

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fd = fs.openSync(lockFile, 'wx', 0o600);
      try {
        fs.writeFileSync(fd, String(process.pid));
      } catch (err) {
        try { fs.unlinkSync(lockFile); } catch { /* best effort */ }
        throw err;
      } finally {
        fs.closeSync(fd);
      }
      return () => removePidFileIfMatches(lockFile, process.pid);
    } catch (err) {
      if (err?.code !== 'EEXIST') throw err;
      const ownerPid = readPid(lockFile);
      if (isProcessRunning(ownerPid, killFn)) {
        throw new Error(`Daemon lifecycle operation already in progress (PID ${ownerPid})`, { cause: err });
      }
      removePidFileIfMatches(lockFile, ownerPid);
    }
  }
  throw new Error('Unable to acquire daemon lifecycle lock');
}

async function waitForProcessExit(pid, killFn, waitFn) {
  for (let attempt = 0; attempt < STOP_POLL_ATTEMPTS; attempt++) {
    if (!isProcessRunning(pid, killFn)) return true;
    await waitFn(STOP_POLL_INTERVAL_MS);
  }
  return !isProcessRunning(pid, killFn);
}

export function buildDaemonCommand(deps = {}) {
  const {
    log = console.log,
    getApiKey,
    spawnFn,
    killFn = process.kill.bind(process),
    waitFn = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  } = deps;

  return async (args, _apiInstance, flags, options) => {
    const sub = args[0];

    if (!sub || sub === 'help' || flags.help || flags.h) {
      log(DAEMON_HELP);
      return;
    }

    for (const key of ['action', 'ws-url', 'rest-url', 'state-file', 'pid-file', 'log-file']) {
      if (options[key] !== undefined && (typeof options[key] !== 'string' || !options[key])) {
        throw new Error(`--${key} must be a non-empty string`);
      }
    }

    if (options['ws-url']) {
      validateServiceUrl(options['ws-url'], 'ws-url', 'wss:', 'ws:');
    }
    if (options['rest-url']) {
      validateServiceUrl(options['rest-url'], 'rest-url', 'https:', 'http:');
    }
    const restUrl = sub === 'run' || sub === 'start'
      ? resolveRestUrl(options['ws-url'], options['rest-url'], !flags['no-backfill'])
      : undefined;

    const pidFile = options['pid-file'] ?? DEFAULT_PID_FILE;
    const logFile = options['log-file'] ?? DEFAULT_LOG_FILE;
    const stateFile = options['state-file'] ?? DEFAULT_STATE_FILE;

    const handlers = {
      // ── run ─────────────────────────────────────────────────────────────────
      'run': async () => {
        const apiKey = getApiKey?.() ?? process.env.NANSEN_API_KEY;
        if (!apiKey) {
          throw new Error('No API key found. Run: nansen login --api-key <key>');
        }

        const wsUrl = options['ws-url'];

        const daemon = new AlertsDaemon({
          apiKey,
          wsUrl,
          restUrl,
          action: options.action,
          actionEnv: flags['action-env'],
          backfill: !flags['no-backfill'],
          stateFile,
          logFile: options['log-file'] ?? null,
          log: (level, message) => {
            process.stderr.write(`[${new Date().toISOString()}] [${level.toUpperCase()}] ${message}\n`);
          },
        });

        const shutdown = () => daemon.stop();
        const cleanupPid = () => removePidFileIfMatches(pidFile, process.pid);
        process.once('SIGINT', shutdown);
        process.once('SIGTERM', shutdown);
        process.once('exit', cleanupPid);

        try {
          await daemon.start();
        } finally {
          process.off('SIGINT', shutdown);
          process.off('SIGTERM', shutdown);
        }
      },

      // ── start ────────────────────────────────────────────────────────────────
      'start': async () => {
        const apiKey = getApiKey?.() ?? process.env.NANSEN_API_KEY;
        if (!apiKey) {
          throw new Error('No API key found. Run: nansen login --api-key <key>');
        }

        const releaseLock = acquireLifecycleLock(pidFile, killFn);
        try {
          const pid = readPid(pidFile);
          if (isProcessRunning(pid, killFn)) {
            log(`Daemon already running (PID ${pid})`);
            return;
          }

          // Spawn detached child
          const spawn = spawnFn ?? (await import('child_process')).spawn;
          const argv = [
            ...process.argv.slice(0, 2), // node + script path
            'alerts', 'daemon', 'run',
            ...(options['ws-url'] ? ['--ws-url', options['ws-url']] : []),
            ...(options['rest-url'] ? ['--rest-url', options['rest-url']] : []),
            ...(options.action ? ['--action', options.action] : []),
            ...(flags['action-env'] ? ['--action-env'] : []),
            ...(flags['no-backfill'] ? ['--no-backfill'] : []),
            ...(options['state-file'] ? ['--state-file', options['state-file']] : []),
            '--pid-file', pidFile,
            '--log-file', logFile,
          ];

          const child = spawn(process.execPath, argv.slice(1), {
            detached: true,
            stdio: 'ignore',
            env: process.env,
          });
          if (!Number.isSafeInteger(child.pid) || child.pid <= 0) {
            // A failed spawn may emit `error` after returning a pid-less child.
            // Attach a listener so the actionable CLI error below is not followed
            // by an unhandled EventEmitter error.
            child.once?.('error', () => {});
            throw new Error('Failed to start daemon: child process did not provide a valid PID');
          }
          child.unref();

          fs.writeFileSync(pidFile, String(child.pid), { mode: 0o600 });

          log(`Daemon started (PID ${child.pid})`);
          log(`Log: ${logFile}`);
          log(`State: ${stateFile}`);
        } finally {
          releaseLock();
        }
      },

      // ── stop ─────────────────────────────────────────────────────────────────
      'stop': async () => {
        const releaseLock = acquireLifecycleLock(pidFile, killFn);
        try {
          const pid = readPid(pidFile);
          if (!isProcessRunning(pid, killFn)) {
            removePidFileIfMatches(pidFile, pid);
            log('Daemon is not running');
            return;
          }
          try {
            killFn(pid, 'SIGTERM');
            const stopped = await waitForProcessExit(pid, killFn, waitFn);
            if (!stopped) {
              log(`Failed to stop daemon: PID ${pid} did not exit within 5 seconds`);
              return;
            }
            removePidFileIfMatches(pidFile, pid);
            log(`Daemon stopped (PID ${pid})`);
          } catch (err) {
            log(`Failed to stop daemon: ${err.message}`);
          }
        } finally {
          releaseLock();
        }
      },

      // ── status ───────────────────────────────────────────────────────────────
      'status': () => {
        const pid = readPid(pidFile);
        const running = isProcessRunning(pid, killFn);

        let state = {};
        try {
          state = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
        } catch {
          // Missing or corrupt state is reported as empty.
        }

        const status = {
          running,
          pid: running ? pid : null,
          lastAlertAt: state.lastAlertAt ?? null,
          lastAlertId: state.lastAlertId ?? null,
          sessionId: state.sessionId ?? null,
          stateFile,
          logFile,
          pidFile,
        };

        return status;
      },

      // ── logs ─────────────────────────────────────────────────────────────────
      'logs': async () => {
        if (!fs.existsSync(logFile)) {
          log(`No log file found at ${logFile}. Has the daemon been started?`);
          return;
        }
        // Tail last 50 lines
        const content = fs.readFileSync(logFile, 'utf8');
        const lines = content.split('\n').filter(Boolean);
        const tail = lines.slice(-50).join('\n');
        log(tail);
      },
    };

    if (!handlers[sub]) {
      throw new Error(`Unknown daemon subcommand: ${sub}. Available: run, start, stop, status, logs`);
    }

    return handlers[sub]();
  };
}
