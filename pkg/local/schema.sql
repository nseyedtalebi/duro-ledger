CREATE TABLE IF NOT EXISTS local_events (
    id              TEXT PRIMARY KEY,
    occurred_at     TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    actor           TEXT NOT NULL,
    content         TEXT NOT NULL,
    refs            TEXT NOT NULL,
    resource_uri    TEXT NOT NULL DEFAULT '',
    local_created   INTEGER NOT NULL,
    remote_sequence INTEGER,
    synced_at       TEXT,
    last_error      TEXT
);

CREATE TABLE IF NOT EXISTS local_cursor (
    id       INTEGER PRIMARY KEY CHECK (id = 1),
    sequence INTEGER NOT NULL
);

-- Opaque blob bytes staged alongside a pending event. Kept in its own
-- table, distinct from local_events' JSON content/refs columns, so blob
-- bytes are never mistaken for event content.
CREATE TABLE IF NOT EXISTS local_blobs (
    event_id   TEXT PRIMARY KEY REFERENCES local_events(id),
    sha256     TEXT NOT NULL,
    media_type TEXT NOT NULL DEFAULT '',
    size_bytes INTEGER NOT NULL,
    content    BLOB NOT NULL
);

-- Filesystem-backed blobs are already durable outside SQLite. Retain immutable
-- metadata for canonical linking and idempotent retries after local bytes are
-- reclaimed.
CREATE TABLE IF NOT EXISTS local_blob_refs (
    event_id   TEXT PRIMARY KEY REFERENCES local_events(id),
    sha256     TEXT NOT NULL,
    media_type TEXT NOT NULL DEFAULT '',
    size_bytes INTEGER NOT NULL
);
