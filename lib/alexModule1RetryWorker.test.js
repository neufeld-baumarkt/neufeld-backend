const test = require('node:test');
const assert = require('node:assert/strict');
const { startAlexModule1RetryWorker } = require('./alexModule1RetryWorker');

const tick = () => new Promise((resolve) => setImmediate(resolve));

test('prueft offene Alex-1-Qlik-Tage sofort und danach minuetlich erneut', async () => {
  let scheduled;
  let cleared = false;
  let runs = 0;
  const timer = { unrefCalled: false, unref() { this.unrefCalled = true; } };
  const worker = startAlexModule1RetryWorker({
    pool: {},
    intervalMs: 60_000,
    logger: { info() {}, error() {} },
    async runRetry() {
      runs += 1;
      return { checked: 1, synchronized: runs === 2 ? 1 : 0, results: [] };
    },
    setIntervalFn(callback, intervalMs) {
      assert.equal(intervalMs, 60_000);
      scheduled = callback;
      return timer;
    },
    clearIntervalFn(value) {
      assert.equal(value, timer);
      cleared = true;
    },
  });

  await tick();
  assert.equal(runs, 1);
  assert.equal(timer.unrefCalled, true);
  scheduled();
  await tick();
  assert.equal(runs, 2);
  worker.stop();
  assert.equal(cleared, true);
  assert.deepEqual(await worker.runOnce(), { skipped: true });
});

test('startet keinen parallelen Wiederholungslauf', async () => {
  let finish;
  const pending = new Promise((resolve) => { finish = resolve; });
  let runs = 0;
  const worker = startAlexModule1RetryWorker({
    pool: {},
    logger: { info() {}, error() {} },
    async runRetry() {
      runs += 1;
      await pending;
      return { checked: 0, synchronized: 0, results: [] };
    },
    setIntervalFn() { return { unref() {} }; },
    clearIntervalFn() {},
  });
  await tick();
  assert.deepEqual(await worker.runOnce(), { skipped: true });
  assert.equal(runs, 1);
  finish();
  await tick();
  worker.stop();
});
