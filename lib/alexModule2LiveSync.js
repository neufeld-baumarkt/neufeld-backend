const CHANNEL = 'alex_module_2_changed';

function singleLine(value) {
  return String(value || '{}').replace(/[\r\n]+/g, ' ');
}

async function streamAlexModule2Changes({ pool, req, res, heartbeatMs = 20000 }) {
  let client;
  let heartbeat;
  let closed = false;

  const cleanup = async () => {
    if (closed) return;
    closed = true;
    if (heartbeat) clearInterval(heartbeat);
    if (client) {
      client.removeAllListeners('notification');
      client.removeAllListeners('error');
      await client.query(`UNLISTEN ${CHANNEL}`).catch(() => {});
      client.release();
    }
    if (!res.writableEnded) res.end();
  };

  try {
    client = await pool.connect();
    await client.query(`LISTEN ${CHANNEL}`);
    res.status(200);
    res.set({
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders?.();
    res.write('retry: 3000\nevent: ready\ndata: {"connected":true}\n\n');

    client.on('notification', (notification) => {
      if (!closed && notification.channel === CHANNEL) {
        res.write(`event: change\ndata: ${singleLine(notification.payload)}\n\n`);
      }
    });
    client.on('error', cleanup);
    heartbeat = setInterval(() => {
      if (!closed) res.write(`: heartbeat ${Date.now()}\n\n`);
    }, heartbeatMs);
    heartbeat.unref?.();
    req.once('aborted', cleanup);
    req.once('close', cleanup);
    res.once('close', cleanup);
  } catch (error) {
    await cleanup();
    throw error;
  }
}

module.exports = { CHANNEL, streamAlexModule2Changes };
