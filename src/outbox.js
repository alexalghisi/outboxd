export async function enqueue(tx, { topic, key, payload }) {
    if (!topic || !key) {
        throw new TypeError('An outbox event needs a topic and a key');
    }
    const { rows } = await tx.query(
        'INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3) RETURNING id',
        [topic, key, JSON.stringify(payload)],
    );
    return rows[0].id;
}

export async function prune(db, olderThanHours) {
    const { rowCount } = await db.query(
        `DELETE FROM outbox
          WHERE published_at < now() - make_interval(hours => $1)`,
        [olderThanHours],
    );
    return rowCount;
}
