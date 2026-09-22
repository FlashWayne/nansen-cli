const MAX_BACKFILL_COUNT = 1000;

export function parseMockServerOptions(args) {
  const values = {
    '--port': '9876',
    '--interval': '10',
    '--backfill-count': '0',
  };
  const seen = new Set();

  for (let index = 0; index < args.length; index += 2) {
    const option = args[index];
    const value = args[index + 1];
    if (!(option in values)) throw new Error(`Unknown option: ${option}`);
    if (seen.has(option)) throw new Error(`${option} may only be specified once`);
    if (value === undefined || value.startsWith('--')) throw new Error(`${option} requires a value`);
    seen.add(option);
    values[option] = value;
  }

  const port = Number(values['--port']);
  const intervalSec = Number(values['--interval']);
  const backfillCount = Number(values['--backfill-count']);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65535) {
    throw new Error('--port must be an integer between 1 and 65535');
  }
  if (!Number.isFinite(intervalSec) || intervalSec <= 0) {
    throw new Error('--interval must be a positive number of seconds');
  }
  if (!Number.isSafeInteger(backfillCount) || backfillCount < 0 || backfillCount > MAX_BACKFILL_COUNT) {
    throw new Error(`--backfill-count must be an integer between 0 and ${MAX_BACKFILL_COUNT}`);
  }
  return { port, intervalSec, backfillCount };
}

export function buildMockBackfillAlerts(templates, count, now = Date.now()) {
  return Array.from({ length: count }, (_, index) => {
    const { type: _transportType, ...template } = templates[index % templates.length];
    return {
      ...template,
      alertId: `mock-backfill-${index + 1}`,
      alertName: `[Backfill] ${template.alertName}`,
      firedAt: new Date(now - (count - index) * 60_000).toISOString(),
    };
  });
}
