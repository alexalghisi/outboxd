import { setTimeout as sleep } from 'node:timers/promises';

import { backoff } from './backoff.js';
import { WAKE_CHANNEL } from './schema.js';

const RELAY_LOCK = 7_302_019;

export class Relay {
    #pool;
    #redis;
    #logger;
    #batchSize;
    #pollMs;
    #maxBackoffMs;
    #wake = null;

    constructor({ pool, redis, logger, batchSize = 100, pollMs = 1000, maxBackoffMs = 30_000 }) {
        this.#pool = pool;
        this.#redis = redis;
        this.#logger = logger;
        this.#batchSize = batchSize;
        this.#pollMs = pollMs;
        this.#maxBackoffMs = maxBackoffMs;
    }

    async drainBatch() {
        const tx = await this.#pool.connect();
        try {
            await tx.query('BEGIN');
            const lock = await tx.query('SELECT pg_try_advisory_xact_lock($1) AS held', [RELAY_LOCK]);
            if (!lock.rows[0].held) {
                await tx.query('ROLLBACK');
                return 0;
            }
            const { rows } = await tx.query(
                `SELECT id, topic, key, payload
                   FROM outbox
                  WHERE published_at IS NULL
                  ORDER BY id
                  LIMIT $1`,
                [this.#batchSize],
            );
            for (const row of rows) {
                await this.#redis.xAdd(row.topic, '*', {
                    id: row.id,
                    key: row.key,
                    payload: JSON.stringify(row.payload),
                });
            }
            await tx.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [
                rows.map((row) => row.id),
            ]);
            await tx.query('COMMIT');
            return rows.length;
        } catch (error) {
            await tx.query('ROLLBACK');
            throw error;
        } finally {
            tx.release();
        }
    }

    async drain() {
        let total = 0;
        let published;
        do {
            published = await this.drainBatch();
            total += published;
        } while (published === this.#batchSize);
        return total;
    }

    async run(signal) {
        const listener = await this.#pool.connect();
        listener.on('notification', () => this.#wake?.abort());
        await listener.query(`LISTEN ${WAKE_CHANNEL}`);
        let failures = 0;
        try {
            while (!signal.aborted) {
                try {
                    const published = await this.drain();
                    failures = 0;
                    if (published > 0) {
                        this.#logger.info(`published ${published} event(s)`);
                    }
                    await this.#pause(this.#pollMs, signal);
                } catch (error) {
                    failures += 1;
                    const delay = backoff(failures, { baseMs: this.#pollMs, maxMs: this.#maxBackoffMs });
                    this.#logger.error(`relay failed, retrying in ${delay} ms: ${error.message}`);
                    await this.#pause(delay, signal);
                }
            }
        } finally {
            listener.removeAllListeners('notification');
            listener.release();
        }
    }

    async #pause(ms, signal) {
        this.#wake = new AbortController();
        try {
            await sleep(ms, undefined, { signal: AbortSignal.any([signal, this.#wake.signal]) });
        } catch (error) {
            if (error.name !== 'AbortError') {
                throw error;
            }
        } finally {
            this.#wake = null;
        }
    }
}
