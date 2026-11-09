const { retryPendingQlikDailyRevenue } = require('./alexModule1');

const DEFAULT_INTERVAL_MS = 60_000;

function startAlexModule1RetryWorker({
  pool,
  intervalMs = DEFAULT_INTERVAL_MS,
  logger = console,
  runRetry = retryPendingQlikDailyRevenue,
  setIntervalFn = setInterval,
  clearIntervalFn = clearInterval,
} = {}) {
  if (!pool) throw new Error('Alex-1-Wiederholungspruefung benoetigt einen Datenbank-Pool.');
  if (!Number.isInteger(intervalMs) || intervalMs < 1_000) {
    throw new Error('Alex-1-Wiederholungsintervall muss mindestens 1000 ms betragen.');
  }

  let running = false;
  let stopped = false;

  const runOnce = async () => {
    if (running || stopped) return { skipped: true };
    running = true;
    try {
      const result = await runRetry(pool);
      const failures = result.results.filter((item) => item.status === 'ERROR');
      if (result.checked > 0 || failures.length > 0) {
        logger.info(
          `Alex 1: ${result.checked} offene Qlik-Tage geprueft, ${result.synchronized} gebucht, ${failures.length} Fehler.`,
        );
      }
      for (const failure of failures) {
        logger.error(`Alex 1: Qlik-Wiederholungspruefung fuer ${failure.businessDate} fehlgeschlagen.`, failure.error);
      }
      return result;
    } catch (error) {
      logger.error('Alex 1: Qlik-Wiederholungspruefung fehlgeschlagen.', error);
      return { checked: 0, synchronized: 0, results: [], error };
    } finally {
      running = false;
    }
  };

  const timer = setIntervalFn(() => { void runOnce(); }, intervalMs);
  if (typeof timer?.unref === 'function') timer.unref();
  void runOnce();

  return {
    runOnce,
    stop() {
      stopped = true;
      clearIntervalFn(timer);
    },
  };
}

module.exports = { DEFAULT_INTERVAL_MS, startAlexModule1RetryWorker };
