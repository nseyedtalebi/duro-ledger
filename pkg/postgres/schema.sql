-- Canonical event table. sequence is the PostgreSQL identity Duro's pull
-- cursor orders by; gaps in it are expected (rolled-back inserts, deleted
-- rows, concurrent writers) and must never be backfilled.
CREATE TABLE IF NOT EXISTS events (
    sequence    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id          UUID NOT NULL UNIQUE,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type  TEXT NOT NULL,
    actor       TEXT NOT NULL,
    content     JSONB NOT NULL CHECK (jsonb_typeof(content) = 'object'),
    refs        JSONB NOT NULL CHECK (jsonb_typeof(refs) = 'object')
);

ALTER TABLE events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS events_public_baseline ON events;
CREATE POLICY events_public_baseline ON events AS PERMISSIVE FOR ALL TO PUBLIC USING (true) WITH CHECK (true);

-- Canonical blob store, content-addressed by sha256. Deduplicated on
-- insert: two events whose blob bytes hash the same share one row.
-- content is nullable: a blob whose bytes are held by an external backend
-- (e.g. pkg/blob's filesystem store) still gets a metadata row here --
-- sha256/size_bytes/media_type and the event FK -- without duplicating
-- bytes into PostgreSQL.
CREATE TABLE IF NOT EXISTS blobs (
    id          UUID PRIMARY KEY,
    sha256      BYTEA NOT NULL UNIQUE,
    media_type  TEXT,
    size_bytes  BIGINT NOT NULL CHECK (size_bytes >= 0),
    content     BYTEA,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotent for a database initialized before content became nullable.
ALTER TABLE blobs ALTER COLUMN content DROP NOT NULL;

-- An event may carry at most one associated blob, referenced by digest
-- rather than blobs.id so dedup (same bytes, different uploader) never
-- requires the caller to know which row happened to win the insert race.
ALTER TABLE events ADD COLUMN IF NOT EXISTS blob_sha256 BYTEA REFERENCES blobs(sha256);
-- Blob bytes are deduplicated, but their requested media type belongs to the
-- event: identical bytes can represent different documents.
ALTER TABLE events ADD COLUMN IF NOT EXISTS blob_media_type TEXT;

-- New canonical IDs receive their actor from the authenticated PostgreSQL
-- role. Existing IDs retain their historical actor during idempotent retries.
CREATE OR REPLACE FUNCTION duro_bind_event_actor() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    existing_actor TEXT;
BEGIN
    SELECT e.actor INTO existing_actor FROM events AS e WHERE e.id = NEW.id;
    IF FOUND THEN
        NEW.actor := existing_actor;
    ELSE
        NEW.actor := current_user;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS events_bind_actor ON events;
CREATE TRIGGER events_bind_actor
BEFORE INSERT ON events
FOR EACH ROW EXECUTE FUNCTION duro_bind_event_actor();

-- Scoped projectors page only their fixed event types and deployment scope.
CREATE INDEX IF NOT EXISTS events_type_scope_sequence_idx
    ON events (event_type, (content->>'deployment_scope'), sequence);
