/**
 * Nansen CLI - Alerts daemon subcommand
 *
 * nansen alerts daemon <start|stop|status|logs|run> [options]
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import { fileURLToPath } from 'url';
import { randomUUID } from 'crypto';
import { AlertsDaemon } from '../daemon/alerts-daemon.js';

const NANSEN_DIR = path.join(os.homedir(), '.nansen');
const DEFAULT_PID_FILE = path.join(NANSEN_DIR, 'alerts-daemon.pid');
const DEFAULT_LOG_FILE = path.join(NANSEN_DIR, 'alerts-daemon.log');
const DEFAULT_STATE_FILE = path.join(NANSEN_DIR, 'alerts-daemon-state.json');
const STOP_POLL_INTERVAL_MS = 50;
const STOP_POLL_ATTEMPTS = 100;
const STARTUP_GRACE_MS = 250;
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '0.0.0.0', '[::1]']);
const CLI_ENTRYPOINT = fileURLToPath(new URL('../index.js', import.meta.url));

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
  --pid-file <path>       Path to PID file. start/stop/status default to
                          ~/.nansen/alerts-daemon.pid; run owns one only if explicit.
  --log-file <path>       Path to log file (default: ~/.nansen/alerts-daemon.log)

BACKGROUND CONTEXT:
  start reloads authentication from the inherited environment or ~/.nansen/config.json.
  NANSEN_BASE_URL does not infer streaming endpoints; use --ws-url/--rest-url.

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

/**
 * Reconstruct the detached daemon invocation from the options the daemon
 * actually supports. Do not reuse process.argv: it can contain wrapper or
 * parent-command arguments when the CLI is embedded. Authentication, HOME
 * configuration, and NANSEN_BASE_URL are preserved through the inherited env.
 */
export function buildDaemonChildArgv(options, flags, logFile) {
  return [
    CLI_ENTRYPOINT,
    'alerts', 'daemon', 'run',
    ...(options['ws-url'] ? ['--ws-url', options['ws-url']] : []),
    ...(options['rest-url'] ? ['--rest-url', options['rest-url']] : []),
    ...(options.action ? ['--action', options.action] : []),
    ...(flags['action-env'] ? ['--action-env'] : []),
    ...(flags['no-backfill'] ? ['--no-backfill'] : []),
    ...(options['state-file'] ? ['--state-file', options['state-file']] : []),
    '--daemon-mode', 'background',
    '--log-file', logFile,
  ];
}

function parseLockPid(raw) {
  try {
    try {
      const record = JSON.parse(raw.trim());
      const pid = Number(typeof record === 'number' ? record : record?.pid);
      return Number.isSafeInteger(pid) && pid > 0 ? pid : null;
    } catch {
      const pid = Number(raw.trim()); // compatibility with locks from older releases
      return Number.isSafeInteger(pid) && pid > 0 ? pid : null;
    }
  } catch {
    return null;
  }
}

function sameFileIdentity(left, right) {
  return left.dev === right.dev && left.ino === right.ino;
}

function unlinkIfSameIdentity(file, identity) {
  try {
    if (sameFileIdentity(fs.lstatSync(file), identity)) fs.unlinkSync(file);
  } catch {
    // Missing/replaced files are not ours to remove.
  }
}

function acquireLifecycleLock(pidFile, killFn) {
  const lockFile = `${pidFile}.lock`;
  fs.mkdirSync(path.dirname(pidFile), { recursive: true, mode: 0o700 });

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fd = fs.openSync(lockFile, 'wx', 0o600);
      const identity = fs.fstatSync(fd);
      try {
        fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, token: randomUUID() }));
      } catch (err) {
        unlinkIfSameIdentity(lockFile, identity);
        fs.closeSync(fd);
        throw err;
      }
      // Keep the descriptor open so its inode cannot be recycled. Cleanup is
      // based on file identity, not mutable contents: corruption cannot strand
      // our lock, while a replacement lock at the same path is left untouched.
      return () => {
        try {
          unlinkIfSameIdentity(lockFile, identity);
        } finally {
          fs.closeSync(fd);
        }
      };
    } catch (err) {
      if (err?.code !== 'EEXIST') throw err;
      let staleFd;
      try {
        staleFd = fs.openSync(lockFile, 'r');
        const identity = fs.fstatSync(staleFd);
        // Read ownership from the opened inode, not from the path. If another
        // process replaces the path, identity comparison below preserves it.
        const ownerPid = parseLockPid(fs.readFileSync(staleFd, 'utf8'));
        if (isProcessRunning(ownerPid, killFn)) {
          throw new Error(`Daemon lifecycle operation already in progress (PID ${ownerPid})`, { cause: err });
        }
        unlinkIfSameIdentity(lockFile, identity);
      } catch (lockErr) {
        if (lockErr?.code !== 'ENOENT') throw lockErr;
        // It disappeared while being inspected; retry acquisition.
      } finally {
        if (staleFd !== undefined) fs.closeSync(staleFd);
      }
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
    DaemonClass = AlertsDaemon,
    killFn = process.kill.bind(process),
    waitFn = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  } = deps;

  return async (args, _apiInstance, flags, options) => {
    const sub = args[0];

    if (!sub || sub === 'help' || flags.help || flags.h) {
      log(DAEMON_HELP);
      return;
    }

    for (const key of ['action', 'ws-url', 'rest-url', 'state-file', 'pid-file', 'log-file', 'daemon-mode']) {
      if (options[key] !== undefined && (typeof options[key] !== 'string' || !options[key])) {
        throw new Error(`--${key} must be a non-empty string`);
      }
    }
    if (options['daemon-mode'] !== undefined && options['daemon-mode'] !== 'background') {
      throw new Error('--daemon-mode must be background');
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

        const daemon = new DaemonClass({
          apiKey,
          wsUrl,
          restUrl,
          action: options.action,
          actionEnv: flags['action-env'],
          foreground: options['daemon-mode'] !== 'background',
          backfill: !flags['no-backfill'],
          stateFile,
          logFile: options['log-file'] ?? null,
          spawnFn,
          log: (level, message) => {
            process.stderr.write(`[${new Date().toISOString()}] [${level.toUpperCase()}] ${message}\n`);
          },
        });

        const ownsPidFile = options['pid-file'] !== undefined;
        if (ownsPidFile) {
          const releaseLock = acquireLifecycleLock(pidFile, killFn);
          try {
            const existingPid = readPid(pidFile);
            if (isProcessRunning(existingPid, killFn)) {
              throw new Error(`Daemon already running (PID ${existingPid})`);
            }
            removePidFileIfMatches(pidFile, existingPid);
            const fd = fs.openSync(pidFile, 'wx', 0o600);
            try {
              fs.writeFileSync(fd, String(process.pid));
            } finally {
              fs.closeSync(fd);
            }
          } finally {
            releaseLock();
          }
        }

        const shutdown = () => daemon.stop();
        const cleanupPid = () => {
          if (ownsPidFile) removePidFileIfMatches(pidFile, process.pid);
        };
        process.once('SIGINT', shutdown);
        process.once('SIGTERM', shutdown);
        process.once('exit', cleanupPid);

        try {
          await daemon.start();
        } finally {
          process.off('SIGINT', shutdown);
          process.off('SIGTERM', shutdown);
          process.off('exit', cleanupPid);
          cleanupPid();
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
          removePidFileIfMatches(pidFile, pid);

          const spawn = spawnFn ?? (await import('child_process')).spawn;
          // Reserve the PID path before spawning so an exclusive 0600 file is
          // guaranteed and a file-creation failure cannot orphan a child.
          const pidFd = fs.openSync(pidFile, 'wx', 0o600);
          const pidIdentity = fs.fstatSync(pidFd);

          const argv = buildDaemonChildArgv(options, flags, logFile);
          let child;
          try {
            child = spawn(process.execPath, argv, {
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
            fs.writeFileSync(pidFd, String(child.pid));
          } catch (err) {
            child?.kill?.();
            unlinkIfSameIdentity(pidFile, pidIdentity);
            throw err;
          } finally {
            fs.closeSync(pidFd);
          }
          child.unref();
          await waitFn(STARTUP_GRACE_MS);
          if (!isProcessRunning(child.pid, killFn)) {
            removePidFileIfMatches(pidFile, child.pid);
            throw new Error(`Daemon exited during startup. Check the log: ${logFile}`);
          }

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
