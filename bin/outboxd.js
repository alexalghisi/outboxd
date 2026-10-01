#!/usr/bin/env node
import { Console } from 'node:console';

import { main } from '../src/cli.js';

const logger = new Console(process.stderr);

try {
    process.exitCode = await main(process.argv.slice(2), process.env, logger);
} catch (error) {
    logger.error(error.message);
    process.exitCode = 1;
}
