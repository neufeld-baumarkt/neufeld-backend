const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const test = require('node:test');
const { CHANNEL, streamAlexModule2Changes } = require('./alexModule2LiveSync');

test('authenticated Alex 2 stream relays committed DB notifications and releases its connection', async () => {
  const client = new EventEmitter();
  const queries = [];
  let released = 0;
  client.query = async (sql) => queries.push(sql);
  client.release = () => { released += 1; };
  const req = new EventEmitter();
  const res = new EventEmitter();
  const writes = [];
  res.status = () => res;
  res.set = () => res;
  res.flushHeaders = () => {};
  res.write = (value) => writes.push(value);
  res.end = () => { res.writableEnded = true; };

  await streamAlexModule2Changes({ pool: { connect: async () => client }, req, res, heartbeatMs: 60000 });
  client.emit('notification', { channel: CHANNEL, payload: '{"table":"payment_days"}' });
  assert.deepEqual(queries, [`LISTEN ${CHANNEL}`]);
  assert.match(writes.join(''), /event: ready/);
  assert.match(writes.join(''), /event: change\ndata: \{"table":"payment_days"\}/);
  req.emit('close');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(queries.at(-1), `UNLISTEN ${CHANNEL}`);
  assert.equal(released, 1);
});
