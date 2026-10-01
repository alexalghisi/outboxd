import assert from 'node:assert/strict';
import { test } from 'node:test';

import { backoff } from '../src/backoff.js';

const limits = { baseMs: 1000, maxMs: 30_000 };

test('first failure waits one base interval', () => {
    assert.equal(backoff(1, limits), 1000);
});

test('delay doubles with every consecutive failure', () => {
    assert.deepEqual([2, 3, 4].map((failures) => backoff(failures, limits)), [2000, 4000, 8000]);
});

test('delay never exceeds the ceiling', () => {
    assert.equal(backoff(20, limits), 30_000);
});
