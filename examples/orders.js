import { Console } from 'node:console';

import pg from 'pg';
import { createClient } from 'redis';

import { enqueue, migrate, Relay } from '../src/index.js';

const out = new Console(process.stdout);
const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/postgres',
});
const redis = createClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });

async function placeOrder(orderId, total, { fail = false } = {}) {
    const tx = await pool.connect();
    try {
        await tx.query('BEGIN');
        await tx.query('INSERT INTO demo_orders (id, total) VALUES ($1, $2)', [orderId, total]);
        await enqueue(tx, { topic: 'demo.orders', key: orderId, payload: { type: 'order.placed', orderId, total } });
        if (fail) {
            throw new Error('card declined');
        }
        await tx.query('COMMIT');
        out.log(`app      committed   ${orderId}  total=${total}`);
    } catch (error) {
        await tx.query('ROLLBACK');
        out.log(`app      rolled back ${orderId}  (${error.message})`);
    } finally {
        tx.release();
    }
}

await redis.connect();
await migrate(pool);
await pool.query('CREATE TABLE IF NOT EXISTS demo_orders (id text PRIMARY KEY, total numeric NOT NULL)');
await pool.query('TRUNCATE demo_orders');
await redis.del('demo.orders');

await placeOrder('ord-1001', 42.5);
await placeOrder('ord-1002', 310, { fail: true });
await placeOrder('ord-1003', 18.9);

const relay = new Relay({ pool, redis, logger: { info() {}, error() {} } });
out.log(`relay    published ${await relay.drain()} event(s) to demo.orders`);

for (const { id, message } of await redis.xRange('demo.orders', '-', '+')) {
    out.log(`consumer ${id}  ${message.key}  ${message.payload}`);
}

await redis.quit();
await pool.end();
