export function backoff(failures, { baseMs, maxMs }) {
    return Math.min(maxMs, baseMs * 2 ** (failures - 1));
}
