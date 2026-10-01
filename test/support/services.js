import pg from 'pg';
import { createClient } from 'redis';

import { migrate } from '../../src/index.js';

export const DATABASE_URL = process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/outbox_test';
export const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';

export const silentLogger = { info() {}, error() {} };

export async function connect() {
    const pool = new pg.Pool({ connectionString: DATABASE_URL });
    const redis = createClient({ url: REDIS_URL });
    await redis.connect();
    await migrate(pool);
    return { pool, redis };
}

export async function reset({ pool, redis }) {
    await pool.query('TRUNCATE outbox RESTART IDENTITY');
    await redis.flushDb();
}

export async function disconnect({ pool, redis }) {
    await redis.quit();
    await pool.end();
}

export async function inTransaction(pool, work) {
    const tx = await pool.connect();
    try {
        await tx.query('BEGIN');
        const result = await work(tx);
        await tx.query('COMMIT');
        return result;
    } catch (error) {
        await tx.query('ROLLBACK');
        throw error;
    } finally {
        tx.release();
    }
}

export async function readStream(redis, topic) {
    const entries = await redis.xRange(topic, '-', '+');
    return entries.map(({ message }) => ({
        id: message.id,
        key: message.key,
        payload: JSON.parse(message.payload),
    }));
}
