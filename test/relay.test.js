import assert from 'node:assert/strict';
import { after, before, beforeEach, test } from 'node:test';

import { enqueue, prune, Relay } from '../src/index.js';
import { connect, disconnect, inTransaction, readStream, reset, silentLogger } from './support/services.js';

let services;

before(async () => {
    services = await connect();
});

beforeEach(() => reset(services));

after(() => disconnect(services));

function relayWith(overrides = {}) {
    return new Relay({ ...services, logger: silentLogger, ...overrides });
}

function orderPlaced(tx, orderId) {
    return enqueue(tx, { topic: 'orders', key: orderId, payload: { type: 'placed', orderId } });
}

test('a committed event reaches its stream', async () => {
    const { pool, redis } = services;
    await inTransaction(pool, (tx) => orderPlaced(tx, 'o-1'));

    assert.equal(await relayWith().drain(), 1);
    assert.deepEqual(await readStream(redis, 'orders'), [
        { id: '1', key: 'o-1', payload: { type: 'placed', orderId: 'o-1' } },
    ]);
});

test('a rolled back transaction publishes nothing', async () => {
    const { pool, redis } = services;
    await assert.rejects(
        inTransaction(pool, async (tx) => {
            await orderPlaced(tx, 'o-1');
            throw new Error('payment declined');
        }),
        /payment declined/,
    );

    assert.equal(await relayWith().drain(), 0);
    assert.deepEqual(await readStream(redis, 'orders'), []);
});

test('events keep insertion order across batches', async () => {
    const { pool, redis } = services;
    const keys = Array.from({ length: 7 }, (_, index) => `o-${index + 1}`);
    for (const key of keys) {
        await inTransaction(pool, (tx) => orderPlaced(tx, key));
    }

    assert.equal(await relayWith({ batchSize: 3 }).drain(), 7);
    assert.deepEqual((await readStream(redis, 'orders')).map((event) => event.key), keys);
});

test('events are routed to the stream named by their topic', async () => {
    const { pool, redis } = services;
    await inTransaction(pool, async (tx) => {
        await orderPlaced(tx, 'o-1');
        await enqueue(tx, { topic: 'invoices', key: 'i-1', payload: { type: 'issued' } });
    });

    await relayWith().drain();
    assert.equal((await readStream(redis, 'orders')).length, 1);
    assert.equal((await readStream(redis, 'invoices')).length, 1);
});

test('draining twice does not publish twice', async () => {
    const { pool, redis } = services;
    await inTransaction(pool, (tx) => orderPlaced(tx, 'o-1'));
    const relay = relayWith();

    await relay.drain();
    assert.equal(await relay.drain(), 0);
    assert.equal((await readStream(redis, 'orders')).length, 1);
});

test('a failed publish leaves the batch pending and is retried', async () => {
    const { pool, redis } = services;
    await inTransaction(pool, async (tx) => {
        await orderPlaced(tx, 'o-1');
        await orderPlaced(tx, 'o-2');
    });
    let calls = 0;
    const flaky = {
        xAdd(...args) {
            calls += 1;
            if (calls === 2) {
                return Promise.reject(new Error('connection reset'));
            }
            return redis.xAdd(...args);
        },
    };
    const relay = relayWith({ redis: flaky });

    await assert.rejects(relay.drain(), /connection reset/);
    const pending = await pool.query('SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL');
    assert.equal(pending.rows[0].n, 2);

    assert.equal(await relay.drain(), 2);
    assert.deepEqual((await readStream(redis, 'orders')).map((event) => event.key), ['o-1', 'o-1', 'o-2']);
});

test('concurrent relays never publish the same event twice', async () => {
    const { pool, redis } = services;
    await inTransaction(pool, async (tx) => {
        for (let n = 1; n <= 50; n += 1) {
            await orderPlaced(tx, `o-${n}`);
        }
    });

    const relays = [relayWith({ batchSize: 10 }), relayWith({ batchSize: 10 }), relayWith({ batchSize: 10 })];
    await Promise.all(relays.map((relay) => relay.drain()));
    await relays[0].drain();

    const ids = (await readStream(redis, 'orders')).map((event) => Number(event.id));
    assert.deepEqual(ids, Array.from({ length: 50 }, (_, index) => index + 1));
});

test('run publishes on notification without waiting for the poll interval', async () => {
    const { pool, redis } = services;
    const stop = new AbortController();
    const running = relayWith({ pollMs: 60_000 }).run(stop.signal);
    await new Promise((resolve) => setTimeout(resolve, 200));

    await inTransaction(pool, (tx) => orderPlaced(tx, 'o-1'));
    const deadline = Date.now() + 2000;
    while ((await redis.xLen('orders')) === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
    }
    stop.abort();
    await running;

    assert.equal(await redis.xLen('orders'), 1);
});

test('enqueue rejects an event without a topic or key', async () => {
    const { pool } = services;
    await assert.rejects(enqueue(pool, { topic: '', key: 'o-1', payload: {} }), TypeError);
    await assert.rejects(enqueue(pool, { topic: 'orders', key: '', payload: {} }), TypeError);
});

test('prune removes only events published before the retention window', async () => {
    const { pool } = services;
    await inTransaction(pool, async (tx) => {
        await orderPlaced(tx, 'old');
        await orderPlaced(tx, 'recent');
        await orderPlaced(tx, 'pending');
    });
    await pool.query("UPDATE outbox SET published_at = now() - interval '3 days' WHERE key = 'old'");
    await pool.query("UPDATE outbox SET published_at = now() WHERE key = 'recent'");

    assert.equal(await prune(pool, 48), 1);
    const left = await pool.query('SELECT key FROM outbox ORDER BY id');
    assert.deepEqual(left.rows.map((row) => row.key), ['recent', 'pending']);
});
