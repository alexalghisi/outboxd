import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { after, before, beforeEach, test } from 'node:test';
import { fileURLToPath } from 'node:url';

import { enqueue } from '../src/index.js';
import { connect, DATABASE_URL, disconnect, readStream, REDIS_URL, reset } from './support/services.js';

const BIN = fileURLToPath(new URL('../bin/outboxd.js', import.meta.url));

let services;

before(async () => {
    services = await connect();
});

beforeEach(() => reset(services));

after(() => disconnect(services));

function outboxd(args, env = {}) {
    const child = spawn(process.execPath, [BIN, ...args], {
        env: { PATH: process.env.PATH, DATABASE_URL, REDIS_URL, ...env },
    });
    child.output = '';
    child.stderr.on('data', (chunk) => {
        child.output += chunk;
    });
    return child;
}

async function until(condition) {
    const deadline = Date.now() + 5000;
    while (!(await condition())) {
        assert.ok(Date.now() < deadline, 'condition was not met within 5 s');
        await new Promise((resolve) => setTimeout(resolve, 25));
    }
}

test('migrate is safe to run repeatedly', async () => {
    for (let run = 0; run < 2; run += 1) {
        const child = outboxd(['migrate']);
        const [code] = await once(child, 'exit');
        assert.equal(code, 0, child.output);
        assert.match(child.output, /outbox schema is up to date/);
    }
});

test('run relays events until it receives SIGTERM', async () => {
    const { pool, redis } = services;
    const child = outboxd(['run']);
    await until(() => child.output.includes('relay started'));

    await enqueue(pool, { topic: 'orders', key: 'o-1', payload: { type: 'placed' } });
    await until(async () => (await redis.xLen('orders')) === 1);
    child.kill('SIGTERM');
    const [code] = await once(child, 'exit');

    assert.equal(code, 0, child.output);
    assert.match(child.output, /published 1 event\(s\)/);
    assert.match(child.output, /relay stopped/);
    assert.deepEqual(await readStream(redis, 'orders'), [{ id: '1', key: 'o-1', payload: { type: 'placed' } }]);
});

test('an unknown command prints usage and exits with 2', async () => {
    const child = outboxd(['publish']);
    const [code] = await once(child, 'exit');
    assert.equal(code, 2);
    assert.match(child.output, /usage: outboxd <command>/);
});

test('a missing DATABASE_URL is reported without a stack trace', async () => {
    const child = outboxd(['migrate'], { DATABASE_URL: '' });
    const [code] = await once(child, 'exit');
    assert.equal(code, 1);
    assert.equal(child.output.trim(), 'DATABASE_URL is required');
});
