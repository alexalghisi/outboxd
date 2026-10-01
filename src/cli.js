import pg from 'pg';
import { createClient } from 'redis';

import { loadConfig } from './config.js';
import { prune } from './outbox.js';
import { Relay } from './relay.js';
import { migrate } from './schema.js';

const USAGE = `usage: outboxd <command>

  migrate   create the outbox table and its wake-up trigger
  run       relay pending events to Redis Streams until stopped
  prune     delete events published more than OUTBOX_RETENTION_HOURS ago
`;

async function relay(pool, config, logger) {
    const redis = createClient({ url: config.redisUrl });
    redis.on('error', (error) => logger.error(`redis: ${error.message}`));
    await redis.connect();
    const stop = new AbortController();
    process.once('SIGINT', () => stop.abort());
    process.once('SIGTERM', () => stop.abort());
    logger.info('relay started');
    try {
        await new Relay({ pool, redis, logger, ...config }).run(stop.signal);
    } finally {
        await redis.quit();
    }
    logger.info('relay stopped');
}

export async function main(argv, env, logger) {
    const [command] = argv;
    if (!['migrate', 'run', 'prune'].includes(command)) {
        logger.error(USAGE);
        return 2;
    }
    const config = loadConfig(env);
    const pool = new pg.Pool({ connectionString: config.databaseUrl });
    try {
        if (command === 'migrate') {
            await migrate(pool);
            logger.info('outbox schema is up to date');
        } else if (command === 'prune') {
            logger.info(`pruned ${await prune(pool, config.retentionHours)} event(s)`);
        } else {
            await relay(pool, config, logger);
        }
    } finally {
        await pool.end();
    }
    return 0;
}
