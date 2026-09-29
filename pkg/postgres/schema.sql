-- Duro's canonical event ledger. This is the entire schema: one
-- append-only table. id, received_at, and actor are database-owned:
-- ordinary writer credentials get column-level INSERT privilege on
-- event_type/content/refs only (see ProvisionWriter and the README), so
-- they physically cannot forge identity, time, or authority, and no trigger
-- or SECURITY DEFINER function is needed to police that boundary.
--
-- pg_catalog is schema-qualified throughout so a writer role's search_path
-- can never substitute a same-named function ahead of the built-in one.
-- The table itself is always addressed as public.events for the same
-- reason: an unqualified "events" would resolve against whatever schema a
-- caller's search_path happens to put first.
CREATE TABLE IF NOT EXISTS public.events (
    -- clock_timestamp() (real wall-clock time at row evaluation), not
    -- now()/transaction_timestamp() (frozen at transaction start): the
    -- contract's received_at is the insertion instant.
    id          UUID NOT NULL DEFAULT pg_catalog.uuidv7() PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    event_type  TEXT NOT NULL,
    actor       TEXT NOT NULL DEFAULT session_user,
    content     JSONB NOT NULL DEFAULT '{}'::jsonb,
    refs        JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Nonblank means more than non-empty: a string made entirely of Unicode
    -- whitespace (including non-ASCII separators such as U+00A0 NBSP or
    -- U+3000 IDEOGRAPHIC SPACE) is blank too. The trim set is the 25
    -- characters Go's unicode.IsSpace accepts, written as explicit code
    -- points rather than a POSIX [[:space:]] class whose treatment of
    -- non-ASCII characters depends on server locale/ctype. Spelling it out
    -- keeps the rule identical in every deployment and identical to the
    -- strings.TrimSpace pre-check in pkg/event.
    CONSTRAINT events_event_type_nonblank CHECK (
        pg_catalog.btrim(
            event_type,
            U&'\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000'
        ) <> ''
    ),
    -- octet_length(convert_to(x, 'UTF8')) rather than octet_length(x): the
    -- latter measures bytes in the database's server_encoding, which equals
    -- UTF-8 byte length only if server_encoding is itself UTF8. Initialize
    -- refuses non-UTF8 databases, but this keeps the rule correct even if
    -- the schema is applied by hand elsewhere.
    CONSTRAINT events_event_type_max_bytes CHECK (pg_catalog.octet_length(pg_catalog.convert_to(event_type, 'UTF8')) <= 255),
    CONSTRAINT events_content_is_object CHECK (pg_catalog.jsonb_typeof(content) = 'object'),
    CONSTRAINT events_content_max_bytes CHECK (pg_catalog.octet_length(pg_catalog.convert_to(content::text, 'UTF8')) <= 1048576),
    CONSTRAINT events_refs_is_object CHECK (pg_catalog.jsonb_typeof(refs) = 'object'),
    CONSTRAINT events_refs_max_bytes CHECK (pg_catalog.octet_length(pg_catalog.convert_to(refs::text, 'UTF8')) <= 1048576)
);

-- The PRIMARY KEY above already gives ascending-id reads (the contract's
-- required ordering) a btree index; no separate index is needed.

-- Belt-and-suspenders: a freshly created table has no PUBLIC grants by
-- default, but state that explicitly rather than relying on the cluster's
-- default privileges never having been changed. Scoped to this one table,
-- not a cluster-wide REVOKE.
REVOKE ALL ON public.events FROM PUBLIC;
