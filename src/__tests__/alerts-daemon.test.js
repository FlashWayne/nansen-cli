/**
 * Unit tests for AlertsDaemon
 * Uses an in-process mock WebSocket server to verify:
 *   - connection & auth
 *   - alert dispatch + stdout emission
 *   - ping/pong keepalive
 *   - reconnect on close
 *   - backfill on reconnect
 *   - state persistence
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EventEmitter } from 'events';
import { fileURLToPath } from 'url';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { buildDaemonChildArgv, buildDaemonCommand, resolveRestUrl } from '../commands/daemon.js';
import { AlertsDaemon, interpolateCommand } from '../daemon/alerts-daemon.js';

// ── Helpers ────────────────────────────────────────────────────────────────────

function makeAlert(overrides = {}) {
  return {
    type: 'alert',
    alertId: 'test-alert-001',
    alertName: 'Test Alert',
    alertType: 'sm-token-flows',
    firedAt: new Date().toISOString(),
    data: { chain: 'ethereum' },
    ...overrides,
  };
}

let daemonCounter = 0;
const temporaryStateFiles = [];

function makeDaemon(opts = {}) {
  class MockWS extends EventEmitter {
    constructor() {
      super();
      this.readyState = 1;
      this.send = vi.fn();
      this.close = vi.fn((code) => {
        this.readyState = 3;
        this.emit('close', code ?? 1000, '');
      });
      setImmediate(() => this.emit('open'));
    }
  }

  const mockFetch = vi.fn().mockResolvedValue({
    ok: true,
    json: async () => ({ alerts: [], count: 0 }),
  });

  const stateFile = opts.stateFile ?? path.join(os.tmpdir(), `nansen-daemon-${process.pid}-${++daemonCounter}.json`);
  if (!opts.stateFile) temporaryStateFiles.push(stateFile);
  const daemon = new AlertsDaemon({
    apiKey: 'test-key',
    wsUrl: 'ws://localhost:9876/v1/smart-alert/stream',
    restUrl: 'http://localhost:9876/api/v1/smart-alert/past-alerts',
    stateFile,
    backfill: false,
    foreground: true,
    WebSocket: MockWS,
    fetchFn: mockFetch,
    log: vi.fn(),
    ...opts,
  });

  return { daemon, MockWS, mockFetch };
}

// ── Tests ──────────────────────────────────────────────────────────────────────

describe('AlertsDaemon', () => {
  let originalStdoutWrite;
  let stdoutLines = [];

  beforeEach(() => {
    stdoutLines = [];
    originalStdoutWrite = process.stdout.write.bind(process.stdout);
    process.stdout.write = (data) => {
      if (typeof data === 'string') stdoutLines.push(data);
      return true;
    };
  });

  afterEach(() => {
    process.stdout.write = originalStdoutWrite;
    for (const stateFile of temporaryStateFiles.splice(0)) {
      try { fs.unlinkSync(stateFile); } catch { /* file may not have been written */ }
    }
  });

  it('throws if no apiKey provided', () => {
    expect(() => new AlertsDaemon({})).toThrow('apiKey is required');
  });

  it('keeps interpolated alert fields within one shell token', () => {
    const command = interpolateCommand('handler {alertName} {alertId}', {
      alertName: 'Safe Name --extra-argument',
      alertId: 'abc; touch /tmp/pwned',
    });

    expect(command).toBe('handler SafeName--extra-argument abctouch/tmp/pwned');
  });

  it('normalizes valid firedAt offsets and safely falls back for invalid timestamps', () => {
    expect(interpolateCommand('handler {firedAt}', {
      firedAt: '2026-03-20T11:00:00.123+01:00',
    })).toBe('handler 2026-03-20T10:00:00.123Z');
    expect(interpolateCommand('handler {firedAt}', {
      firedAt: 'not a date;$(touch /tmp/x)',
    })).toBe('handler notadatetouch/tmp/x');
  });

  it('ignores non-object messages', () => {
    const { daemon } = makeDaemon();
    expect(() => daemon._handleMessage(null)).not.toThrow();
  });

  it('emits "connected" event on WS open + connected message', async () => {
    let headers;

    class AutoMockWS extends EventEmitter {
      constructor(_url, options) {
        super();
        headers = options.headers;
        this.readyState = 1;
        this.send = vi.fn();
        this.close = vi.fn((code) => {
          this.readyState = 3;
          this.emit('close', code ?? 1000, '');
        });
        setImmediate(() => {
          this.emit('open');
          setImmediate(() => {
            this.emit('message', JSON.stringify({
              type: 'connected',
              sessionId: 'sess-abc',
              serverTime: new Date().toISOString(),
            }));
          });
        });
      }
    }

    const daemon = new AlertsDaemon({
      apiKey: 'test-key',
      wsUrl: 'ws://localhost/stream',
      restUrl: 'http://localhost/past-alerts',
      stateFile: '/tmp/test-daemon-state2.json',
      backfill: false,
      WebSocket: AutoMockWS,
      fetchFn: vi.fn(),
      log: vi.fn(),
    });

    const conn = await new Promise((resolve) => {
      daemon.once('connected', resolve);
      daemon.start();
    });

    daemon.stop();

    expect(conn.sessionId).toBe('sess-abc');
    expect(headers).toEqual({ apikey: 'test-key' });
  });

  it('settles a failed connection even when the socket never emits close', async () => {
    let socket;
    class ErrorOnlyMockWS extends EventEmitter {
      constructor() {
        super();
        socket = this;
        this.terminate = vi.fn();
        setImmediate(() => this.emit('error', new Error('DNS lookup failed')));
      }
    }

    const { daemon } = makeDaemon({ WebSocket: ErrorOnlyMockWS });

    await expect(daemon._connect()).resolves.toBeUndefined();
    expect(socket.terminate).toHaveBeenCalledOnce();
    expect(daemon._ws).toBeNull();
  });

  it('settles a failed connection when terminating the socket throws', async () => {
    class ThrowingTerminateMockWS extends EventEmitter {
      constructor() {
        super();
        this.terminate = vi.fn(() => { throw new Error('already destroyed'); });
        setImmediate(() => this.emit('error', new Error('DNS lookup failed')));
      }
    }

    const { daemon } = makeDaemon({ WebSocket: ThrowingTerminateMockWS });

    await expect(daemon._connect()).resolves.toBeUndefined();
    expect(daemon._ws).toBeNull();
  });

  it('does not dispatch stale backfill after a rapid close and reconnect', async () => {
    const sockets = [];
    class ControlledWS extends EventEmitter {
      constructor() {
        super();
        this.readyState = 1;
        this.send = vi.fn();
        this.close = vi.fn((code) => this.emit('close', code ?? 1000, ''));
        this.terminate = vi.fn(() => this.emit('close', 1006, ''));
        sockets.push(this);
      }
    }
    let resolveFirstFetch;
    const firstFetch = new Promise((resolve) => { resolveFirstFetch = resolve; });
    const fetchFn = vi.fn()
      .mockImplementationOnce(() => firstFetch)
      .mockResolvedValue({ ok: true, json: async () => ({ alerts: [] }) });
    const { daemon } = makeDaemon({ WebSocket: ControlledWS, fetchFn, backfill: true });
    daemon._state.lastAlertAt = '2026-03-20T10:00:00Z';
    const seen = [];
    daemon.on('alert', (alert) => seen.push(alert.alertId));

    const firstConnection = daemon._connect();
    sockets[0].emit('open');
    await vi.waitFor(() => expect(fetchFn).toHaveBeenCalledTimes(1));
    sockets[0].emit('close', 1006, 'rapid close');
    await firstConnection;

    const secondConnection = daemon._connect();
    sockets[1].emit('open');
    await vi.waitFor(() => expect(fetchFn).toHaveBeenCalledTimes(2));
    resolveFirstFetch({
      ok: true,
      json: async () => ({
        alerts: [makeAlert({ alertId: 'stale-backfill', firedAt: '2026-03-20T10:00:01Z' })],
      }),
    });
    await new Promise((resolve) => setImmediate(resolve));

    expect(fetchFn.mock.calls[0][1].signal.aborted).toBe(true);
    expect(seen).toEqual([]);
    sockets[1].emit('close', 1000, 'done');
    await secondConnection;
  });

  it('does not leave action stdin open in environment mode', () => {
    const child = new EventEmitter();
    const spawnFn = vi.fn(() => child);
    const { daemon } = makeDaemon({
      action: 'handler',
      actionEnv: true,
      spawnFn,
    });
    const alert = makeAlert();

    daemon._dispatchAlert(alert);

    expect(spawnFn).toHaveBeenCalledWith('/bin/sh', ['-c', 'handler'], expect.objectContaining({
      stdio: ['ignore', 'inherit', 'inherit'],
      env: expect.objectContaining({ NANSEN_ALERT: JSON.stringify(alert) }),
    }));
  });

  it('uses ignored stdin and accepts a mocked successful close in environment mode', () => {
    const child = new EventEmitter();
    const close = vi.fn();
    child.on('close', close);
    const spawnFn = vi.fn(() => child);
    const { daemon } = makeDaemon({
      action: 'handler',
      actionEnv: true,
      spawnFn,
    });

    daemon._dispatchAlert(makeAlert());
    child.emit('close', 0);

    expect(spawnFn).toHaveBeenCalledWith('/bin/sh', ['-c', 'handler'], expect.objectContaining({
      stdio: ['ignore', 'inherit', 'inherit'],
    }));
    expect(close).toHaveBeenCalledWith(0);
  });

  it('emits "alert" event and writes JSON to stdout', async () => {
    const alert = makeAlert();

    class AlertMockWS extends EventEmitter {
      constructor() {
        super();
        this.readyState = 1;
        this.send = vi.fn();
        this.close = vi.fn((code) => { this.readyState = 3; this.emit('close', code ?? 1000, ''); });
        setImmediate(() => {
          this.emit('open');
          setImmediate(() => this.emit('message', JSON.stringify(alert)));
        });
      }
    }

    const daemon = new AlertsDaemon({
      apiKey: 'test-key',
      wsUrl: 'ws://localhost/stream',
      restUrl: 'http://localhost/past-alerts',
      stateFile: '/tmp/test-daemon-state3.json',
      backfill: false,
      foreground: true,
      WebSocket: AlertMockWS,
      fetchFn: vi.fn(),
      log: vi.fn(),
    });

    const received = await new Promise((resolve) => {
      daemon.once('alert', resolve);
      daemon.start();
    });

    daemon.stop();

    expect(received.alertId).toBe('test-alert-001');
    expect(received.alertName).toBe('Test Alert');

    // Should have written NDJSON to stdout
    const line = stdoutLines.find((l) => l.includes('test-alert-001'));
    expect(line).toBeDefined();
    const parsed = JSON.parse(line.trim());
    expect(parsed.alertId).toBe('test-alert-001');
  });

  it('does not emit NDJSON in background mode', () => {
    const { daemon } = makeDaemon({ foreground: false });

    daemon._dispatchAlert(makeAlert());

    expect(stdoutLines).toEqual([]);
  });

  it('sends ping and handles pong', async () => {
    vi.useFakeTimers();

    let wsSendCalls = [];

    class PingMockWS extends EventEmitter {
      constructor() {
        super();
        this.readyState = 1;
        this.send = vi.fn((data) => wsSendCalls.push(JSON.parse(data)));
        this.close = vi.fn((code) => { this.readyState = 3; this.emit('close', code ?? 1000, ''); });
        setImmediate(() => this.emit('open'));
      }
    }

    const daemon = new AlertsDaemon({
      apiKey: 'test-key',
      wsUrl: 'ws://localhost/stream',
      restUrl: 'http://localhost/past-alerts',
      stateFile: '/tmp/test-daemon-state4.json',
      backfill: false,
      WebSocket: PingMockWS,
      fetchFn: vi.fn(),
      log: vi.fn(),
    });

    daemon.start();

    // Advance past the first setImmediate (open), then past one ping interval (30s)
    await vi.advanceTimersByTimeAsync(31_000);

    daemon.stop();

    const pings = wsSendCalls.filter((m) => m.type === 'ping');
    expect(pings.length).toBeGreaterThan(0);

    vi.useRealTimers();
  });

  it('keeps only one pending pong timeout', () => {
    vi.useFakeTimers();
    const { daemon } = makeDaemon();
    daemon._ws = { close: vi.fn() };

    daemon._schedulePongTimeout();
    daemon._schedulePongTimeout();

    expect(vi.getTimerCount()).toBe(1);
    daemon.stop();
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
  });

  it('stops reconnecting on UNAUTHORIZED server error', async () => {
    class AuthErrorMockWS extends EventEmitter {
      constructor() {
        super();
        this.readyState = 1;
        this.send = vi.fn();
        this.close = vi.fn((code) => { this.readyState = 3; this.emit('close', code ?? 1000, ''); });
        setImmediate(() => {
          this.emit('open');
          setImmediate(() => {
            this.emit('message', JSON.stringify({
              type: 'error',
              code: 'UNAUTHORIZED',
              message: 'Invalid API key',
            }));
          });
        });
      }
    }

    const daemon = new AlertsDaemon({
      apiKey: 'bad-key',
      wsUrl: 'ws://localhost/stream',
      restUrl: 'http://localhost/past-alerts',
      stateFile: '/tmp/test-daemon-state5.json',
      backfill: false,
      WebSocket: AuthErrorMockWS,
      fetchFn: vi.fn(),
      log: vi.fn(),
    });

    // Wait for both the error event and for the daemon loop to finish
    const errorMsg = await new Promise((resolve) => {
      daemon.once('server-error', resolve);
      daemon.start();
    });

    // The daemon sets _running = false synchronously in the error handler,
    // but the connect loop needs a microtask tick to process the close event.
    // Flush microtasks by awaiting a resolved promise a few times.
    for (let i = 0; i < 5; i++) await Promise.resolve();

    expect(daemon._running).toBe(false);
    expect(errorMsg.code).toBe('UNAUTHORIZED');

    daemon.stop(); // cleanup timers
  });

  it('calls /past-alerts on reconnect when backfill=true', async () => {
    const missedAlert = makeAlert({ alertId: 'past-001', alertName: 'Past Alert' });
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ alerts: [missedAlert], count: 1 }),
    });

    class BackfillMockWS extends EventEmitter {
      constructor() {
        super();
        this.readyState = 1;
        this.send = vi.fn();
        this.close = vi.fn((code) => { this.readyState = 3; this.emit('close', code ?? 1000, ''); });
        setImmediate(() => this.emit('open'));
      }
    }

    const daemon = new AlertsDaemon({
      apiKey: 'test-key',
      wsUrl: 'ws://localhost/stream',
      restUrl: 'http://localhost/past-alerts',
      stateFile: '/tmp/test-daemon-state6.json',
      backfill: true,
      WebSocket: BackfillMockWS,
      fetchFn: mockFetch,
      log: vi.fn(),
    });

    // Seed a lastAlertAt so backfill triggers
    daemon._state = { lastAlertAt: '2026-03-20T10:00:00Z' };

    const backfilledAlerts = [];
    daemon.on('alert', (a) => backfilledAlerts.push(a));

    // Start the daemon — it will connect, backfill, then wait for messages.
    // We use a log spy to detect when backfill completes.
    const backfillDone = new Promise((resolve) => {
      const origLog = daemon._logFn;
      daemon._logFn = (level, msg) => {
        origLog?.(level, msg);
        if (msg.includes('Replaying') || msg.includes('No missed alerts')) resolve();
      };
    });

    daemon.start();

    await backfillDone;

    daemon.stop();

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/past-alerts'),
      expect.objectContaining({ headers: { apikey: 'test-key' } })
    );
    expect(backfilledAlerts.some((a) => a.alertId === 'past-001')).toBe(true);
    expect(daemon._state).toMatchObject({
      lastAlertAt: missedAlert.firedAt,
      lastAlertId: 'past-001',
    });

    await daemon._fetchPastAlerts(daemon._state.lastAlertAt);
    expect(backfilledAlerts.filter((a) => a.alertId === 'past-001')).toHaveLength(1);

    daemon._handleMessage({
      ...missedAlert,
      firedAt: new Date(Date.parse(missedAlert.firedAt) + 1_000).toISOString(),
    });
    expect(backfilledAlerts.filter((a) => a.alertId === 'past-001')).toHaveLength(2);
  });

  it('does not replay backfill alerts older than the saved cursor', async () => {
    const { daemon } = makeDaemon();
    const seen = [];
    daemon.on('alert', (alert) => seen.push(alert.alertId));
    daemon._state = {
      lastAlertAt: '2026-03-20T10:00:00Z',
      lastAlertId: 'already-seen',
    };
    daemon._fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        alerts: [
          makeAlert({ alertId: 'older', firedAt: '2026-03-20T09:59:59Z' }),
          makeAlert({ alertId: 'newer', firedAt: '2026-03-20T10:00:01Z' }),
        ],
      }),
    });

    await daemon._fetchPastAlerts(daemon._state.lastAlertAt);

    expect(seen).toEqual(['newer']);
  });

  it('persists a bounded deduplication window before dispatch', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-dedup-'));
    const stateFile = path.join(dir, 'state.json');

    try {
      const { daemon } = makeDaemon({ stateFile });
      daemon._dispatchAlert = vi.fn();
      const first = makeAlert({ alertId: 'first', firedAt: '2026-03-20T10:00:00Z' });
      const second = makeAlert({ alertId: 'second', firedAt: '2026-03-20T10:00:01Z' });

      daemon._handleMessage(first);
      daemon._handleMessage(second);
      daemon._handleMessage(first);

      expect(daemon._dispatchAlert).toHaveBeenCalledTimes(2);
      expect(JSON.parse(fs.readFileSync(stateFile, 'utf8')).recentAlertKeys).toHaveLength(2);

      const { daemon: restarted } = makeDaemon({ stateFile });
      restarted._dispatchAlert = vi.fn();
      restarted._handleMessage(second);
      expect(restarted._dispatchAlert).not.toHaveBeenCalled();

      for (let index = 0; index < 55; index++) {
        restarted._handleMessage(makeAlert({
          alertId: `bounded-${index}`,
          firedAt: new Date(Date.UTC(2026, 2, 20, 10, 1, index)).toISOString(),
        }));
      }
      const persisted = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
      expect(persisted.recentAlertKeys).toHaveLength(50);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('advances the cursor by parsed time across offsets and milliseconds', () => {
    const { daemon } = makeDaemon();
    daemon._dispatchAlert = vi.fn();
    daemon._state.lastAlertAt = '2026-03-20T10:00:00.100Z';
    daemon._state.lastAlertId = 'cursor';

    daemon._handleMessage(makeAlert({
      alertId: 'later',
      firedAt: '2026-03-20T09:00:00.200-01:00',
    }));

    expect(daemon._state).toMatchObject({
      lastAlertAt: '2026-03-20T09:00:00.200-01:00',
      lastAlertId: 'later',
    });
  });

  it('never regresses the cursor for older or invalid timestamps', () => {
    const { daemon } = makeDaemon();
    daemon._dispatchAlert = vi.fn();
    daemon._state.lastAlertAt = '2026-03-20T10:00:00.900Z';
    daemon._state.lastAlertId = 'cursor';

    daemon._handleMessage(makeAlert({
      alertId: 'offset-older',
      firedAt: '2026-03-20T11:00:00+02:00',
    }));
    daemon._handleMessage(makeAlert({ alertId: 'invalid-time', firedAt: 'not-a-timestamp' }));

    expect(daemon._state).toMatchObject({
      lastAlertAt: '2026-03-20T10:00:00.900Z',
      lastAlertId: 'cursor',
    });

    daemon._state.lastAlertAt = 'corrupt-cursor';
    daemon._handleMessage(makeAlert({ alertId: 'recovered', firedAt: '2026-03-20T10:00:01Z' }));
    expect(daemon._state).toMatchObject({
      lastAlertAt: '2026-03-20T10:00:01Z',
      lastAlertId: 'recovered',
    });
  });

  it('stop interrupts an active reconnect delay', async () => {
    vi.useFakeTimers();

    class ClosingMockWS extends EventEmitter {
      constructor() {
        super();
        this.readyState = 1;
        this.close = vi.fn();
        setImmediate(() => this.emit('close', 1006, 'gone'));
      }
    }

    const { daemon } = makeDaemon({ WebSocket: ClosingMockWS });
    const started = daemon.start();
    await vi.advanceTimersByTimeAsync(1);
    daemon.stop();

    await expect(started).resolves.toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
  });
});

describe('daemon command', () => {
  it('builds a canonical daemon-only child invocation', () => {
    const argv = buildDaemonChildArgv({
      action: 'handler --mode env',
      'ws-url': 'wss://custom.example/v1/smart-alert/stream',
      'rest-url': 'https://custom.example/past-alerts',
      'state-file': '/tmp/state.json',
    }, {
      'action-env': true,
      'no-backfill': true,
      pretty: true,
    }, '/tmp/daemon.log');

    const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
    const packageJson = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8'));
    expect(argv[0]).toBe(path.resolve(packageRoot, packageJson.bin.nansen));
    expect(argv.slice(1)).toEqual([
      'alerts', 'daemon', 'run',
      '--ws-url', 'wss://custom.example/v1/smart-alert/stream',
      '--rest-url', 'https://custom.example/past-alerts',
      '--action', 'handler --mode env',
      '--action-env', '--no-backfill',
      '--state-file', '/tmp/state.json',
      '--daemon-mode', 'background',
      '--log-file', '/tmp/daemon.log',
    ]);
    expect(argv).not.toContain('--pid-file');
    expect(argv).not.toContain('--pretty');
  });

  it('lets a direct run explicitly own a PID file visible to status', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-run-pid-'));
    const pidFile = path.join(dir, 'daemon.pid');
    let resolveStart;
    let reportStarted;
    let daemonOptions;
    const spawnFn = vi.fn();
    const started = new Promise((resolve) => { reportStarted = resolve; });
    class FakeDaemon {
      constructor(options) { daemonOptions = options; }
      start() {
        reportStarted();
        return new Promise((resolve) => { resolveStart = resolve; });
      }
      stop() {}
    }
    const killFn = vi.fn((pid) => {
      if (pid === process.pid) return;
      throw Object.assign(new Error('gone'), { code: 'ESRCH' });
    });
    const command = buildDaemonCommand({
      getApiKey: () => 'test-key',
      DaemonClass: FakeDaemon,
      spawnFn,
      killFn,
    });

    try {
      const running = command(['run'], null, {}, { 'pid-file': pidFile });
      await started;
      expect(daemonOptions).toEqual(expect.objectContaining({ foreground: true, spawnFn }));
      expect(fs.readFileSync(pidFile, 'utf8')).toBe(String(process.pid));
      await expect(command(['status'], null, {}, { 'pid-file': pidFile }))
        .resolves.toMatchObject({ running: true, pid: process.pid });

      resolveStart();
      await running;
      expect(fs.existsSync(pidFile)).toBe(false);
      await expect(command(['status'], null, {}, { 'pid-file': pidFile }))
        .resolves.toMatchObject({ running: false, pid: null });
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('threads background output mode and injected spawn into the daemon', async () => {
    let daemonOptions;
    const spawnFn = vi.fn();
    class FakeDaemon {
      constructor(options) { daemonOptions = options; }
      async start() {}
      stop() {}
    }
    const command = buildDaemonCommand({
      getApiKey: () => 'test-key',
      DaemonClass: FakeDaemon,
      spawnFn,
    });

    await command(['run'], null, {}, { 'daemon-mode': 'background' });

    expect(daemonOptions).toEqual(expect.objectContaining({
      foreground: false,
      spawnFn,
    }));
  });

  it('derives backfill safely or requires an explicit REST URL', () => {
    expect(resolveRestUrl(
      'wss://custom.example/v1/smart-alert/stream?ignored=yes',
      undefined,
      true,
    )).toBe('https://custom.example/api/v1/smart-alert/past-alerts');
    expect(resolveRestUrl(
      'wss://custom.example/alerts',
      'https://custom.example/backfill',
      true,
    )).toBe('https://custom.example/backfill');
    expect(resolveRestUrl('wss://custom.example/alerts', undefined, false)).toBeUndefined();
    expect(() => resolveRestUrl('wss://custom.example/alerts', undefined, true))
      .toThrow('--rest-url is required');
  });

  it('refuses to start a background daemon without an API key', async () => {
    const command = buildDaemonCommand({ log: vi.fn(), getApiKey: () => null });
    await expect(command(['start'], null, {}, {})).rejects.toThrow('No API key found');
  });

  it('refuses a non-standard WebSocket path without a REST backfill URL', async () => {
    const spawnFn = vi.fn();
    const command = buildDaemonCommand({
      log: vi.fn(),
      getApiKey: () => 'test-key',
      spawnFn,
    });

    await expect(command(['start'], null, {}, {
      'ws-url': 'wss://custom.example/alerts',
    })).rejects.toThrow('--rest-url is required');
    expect(spawnFn).not.toHaveBeenCalled();
  });

  it('does not write a PID file when spawn returns an invalid PID', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-test-'));
    const pidFile = path.join(dir, 'daemon.pid');
    const child = { pid: undefined, once: vi.fn(), unref: vi.fn() };
    const command = buildDaemonCommand({
      log: vi.fn(),
      getApiKey: () => 'test-key',
      spawnFn: vi.fn(() => child),
    });

    try {
      await expect(command(['start'], null, {}, {
        'pid-file': pidFile,
        'state-file': path.join(dir, 'state.json'),
        'log-file': path.join(dir, 'daemon.log'),
      })).rejects.toThrow('child process did not provide a valid PID');
      expect(child.once).toHaveBeenCalledWith('error', expect.any(Function));
      expect(child.unref).not.toHaveBeenCalled();
      expect(fs.existsSync(pidFile)).toBe(false);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('removes the PID file when the child exits during startup', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-start-'));
    const pidFile = path.join(dir, 'daemon.pid');
    const child = { pid: 4242, once: vi.fn(), unref: vi.fn() };
    const command = buildDaemonCommand({
      log: vi.fn(),
      getApiKey: () => 'test-key',
      spawnFn: vi.fn(() => child),
      killFn: vi.fn(() => { throw Object.assign(new Error('gone'), { code: 'ESRCH' }); }),
      waitFn: vi.fn(async () => {}),
    });

    try {
      await expect(command(['start'], null, {}, {
        'pid-file': pidFile,
        'log-file': path.join(dir, 'daemon.log'),
      })).rejects.toThrow('Daemon exited during startup');
      expect(child.unref).toHaveBeenCalledOnce();
      expect(fs.existsSync(pidFile)).toBe(false);
      expect(fs.existsSync(`${pidFile}.lock`)).toBe(false);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('keeps the PID file until the daemon has exited', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-stop-'));
    const pidFile = path.join(dir, 'daemon.pid');
    const logs = [];
    let signaled = false;
    let postSignalChecks = 0;
    fs.writeFileSync(pidFile, '4242');
    const killFn = vi.fn((_pid, signal) => {
      if (signal === 'SIGTERM') {
        signaled = true;
        return;
      }
      if (!signaled || postSignalChecks++ < 2) return;
      throw Object.assign(new Error('gone'), { code: 'ESRCH' });
    });
    const waitFn = vi.fn(async () => {
      expect(fs.readFileSync(pidFile, 'utf8')).toBe('4242');
    });
    const command = buildDaemonCommand({
      log: (line) => logs.push(line),
      killFn,
      waitFn,
    });

    try {
      await command(['stop'], null, {}, { 'pid-file': pidFile });
      expect(waitFn).toHaveBeenCalledTimes(2);
      expect(fs.existsSync(pidFile)).toBe(false);
      expect(logs).toContain('Daemon stopped (PID 4242)');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('keeps the PID file when graceful shutdown times out', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-stop-'));
    const pidFile = path.join(dir, 'daemon.pid');
    const logs = [];
    fs.writeFileSync(pidFile, '4242');
    const command = buildDaemonCommand({
      log: (line) => logs.push(line),
      killFn: vi.fn(),
      waitFn: vi.fn(async () => {}),
    });

    try {
      await command(['stop'], null, {}, { 'pid-file': pidFile });
      expect(fs.readFileSync(pidFile, 'utf8')).toBe('4242');
      expect(logs).toContain('Failed to stop daemon: PID 4242 did not exit within 5 seconds');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('serializes lifecycle operations with an atomic lock', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-lock-'));
    const pidFile = path.join(dir, 'daemon.pid');
    fs.writeFileSync(`${pidFile}.lock`, String(process.pid));
    const spawnFn = vi.fn();
    const command = buildDaemonCommand({
      log: vi.fn(),
      getApiKey: () => 'test-key',
      spawnFn,
    });

    try {
      await expect(command(['start'], null, {}, { 'pid-file': pidFile }))
        .rejects.toThrow('lifecycle operation already in progress');
      expect(spawnFn).not.toHaveBeenCalled();
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('releases its lock by file identity even if lock contents are corrupted', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-lock-corrupt-'));
    const pidFile = path.join(dir, 'daemon.pid');
    const lockFile = `${pidFile}.lock`;
    const child = { pid: 4242, once: vi.fn(), unref: vi.fn() };
    const command = buildDaemonCommand({
      log: vi.fn(),
      getApiKey: () => 'test-key',
      spawnFn: vi.fn(() => child),
      killFn: vi.fn(),
      waitFn: vi.fn(async () => fs.writeFileSync(lockFile, 'corrupted')),
    });

    try {
      await command(['start'], null, {}, { 'pid-file': pidFile });
      expect(fs.statSync(pidFile).mode & 0o777).toBe(0o600);
      expect(fs.existsSync(lockFile)).toBe(false);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('does not remove a replacement lifecycle lock during release', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-lock-replaced-'));
    const pidFile = path.join(dir, 'daemon.pid');
    const lockFile = `${pidFile}.lock`;
    const child = { pid: 4242, once: vi.fn(), unref: vi.fn() };
    const command = buildDaemonCommand({
      log: vi.fn(),
      getApiKey: () => 'test-key',
      spawnFn: vi.fn(() => child),
      killFn: vi.fn(),
      waitFn: vi.fn(async () => {
        fs.unlinkSync(lockFile);
        fs.writeFileSync(lockFile, String(process.pid));
      }),
    });

    try {
      await command(['start'], null, {}, { 'pid-file': pidFile });
      expect(fs.readFileSync(lockFile, 'utf8')).toBe(String(process.pid));
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('does not treat an invalid PID file as a running daemon', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nansen-daemon-test-'));
    const pidFile = path.join(dir, 'daemon.pid');
    fs.writeFileSync(pidFile, '-1');

    try {
      const command = buildDaemonCommand({ log: vi.fn() });
      const status = await command(['status'], null, {}, {
        'pid-file': pidFile,
        'state-file': path.join(dir, 'state.json'),
        'log-file': path.join(dir, 'daemon.log'),
      });

      expect(status.running).toBe(false);
      expect(status.pid).toBeNull();
      await expect(command(['status'], null, {}, {
        'pid-file': -1,
      })).rejects.toThrow('--pid-file must be a non-empty string');
      await expect(command(['status'], null, {}, {
        'ws-url': 'ws://example.com/stream',
      })).rejects.toThrow('--ws-url must use wss://');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
