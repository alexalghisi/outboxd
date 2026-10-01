export const WAKE_CHANNEL = 'outbox_wake';

const STATEMENTS = [
    `CREATE TABLE IF NOT EXISTS outbox (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        topic text NOT NULL,
        key text NOT NULL,
        payload jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        published_at timestamptz
    )`,
    `CREATE INDEX IF NOT EXISTS outbox_pending_idx
        ON outbox (id) WHERE published_at IS NULL`,
    `CREATE OR REPLACE FUNCTION outbox_wake() RETURNS trigger AS $$
    BEGIN
        PERFORM pg_notify('${WAKE_CHANNEL}', '');
        RETURN NULL;
    END
    $$ LANGUAGE plpgsql`,
    `CREATE OR REPLACE TRIGGER outbox_wake_trigger
        AFTER INSERT ON outbox
        FOR EACH STATEMENT EXECUTE FUNCTION outbox_wake()`,
];

export async function migrate(db) {
    for (const statement of STATEMENTS) {
        await db.query(statement);
    }
}
