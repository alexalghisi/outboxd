function positiveInt(env, name, fallback) {
    if (env[name] === undefined) {
        return fallback;
    }
    const value = Number(env[name]);
    if (!Number.isInteger(value) || value <= 0) {
        throw new Error(`${name} must be a positive integer, got "${env[name]}"`);
    }
    return value;
}

export function loadConfig(env) {
    if (!env.DATABASE_URL) {
        throw new Error('DATABASE_URL is required');
    }
    return {
        databaseUrl: env.DATABASE_URL,
        redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
        batchSize: positiveInt(env, 'OUTBOX_BATCH_SIZE', 100),
        pollMs: positiveInt(env, 'OUTBOX_POLL_MS', 1000),
        retentionHours: positiveInt(env, 'OUTBOX_RETENTION_HOURS', 168),
    };
}
