import assert from 'node:assert/strict';
import { test } from 'node:test';

import { loadConfig } from '../src/config.js';

test('applies defaults when only the database is configured', () => {
    assert.deepEqual(loadConfig({ DATABASE_URL: 'postgres://db/app' }), {
        databaseUrl: 'postgres://db/app',
        redisUrl: 'redis://localhost:6379',
        batchSize: 100,
        pollMs: 1000,
        retentionHours: 168,
    });
});

test('reads overrides from the environment', () => {
    const config = loadConfig({
        DATABASE_URL: 'postgres://db/app',
        REDIS_URL: 'redis://cache:6380',
        OUTBOX_BATCH_SIZE: '25',
        OUTBOX_POLL_MS: '250',
        OUTBOX_RETENTION_HOURS: '24',
    });
    assert.equal(config.redisUrl, 'redis://cache:6380');
    assert.equal(config.batchSize, 25);
    assert.equal(config.pollMs, 250);
    assert.equal(config.retentionHours, 24);
});

test('refuses to start without a database', () => {
    assert.throws(() => loadConfig({}), /DATABASE_URL is required/);
});

for (const value of ['0', '-5', '1.5', 'many']) {
    test(`rejects a batch size of "${value}"`, () => {
        assert.throws(
            () => loadConfig({ DATABASE_URL: 'postgres://db/app', OUTBOX_BATCH_SIZE: value }),
            /OUTBOX_BATCH_SIZE must be a positive integer/,
        );
    });
}
