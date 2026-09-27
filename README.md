# Duro

Duro is a small event ledger for retryable facts and optional verified bytes.
PostgreSQL is the sole canonical event authority; SQLite is an optional local
queue for offline work and retry. Duro carries domain meaning in flexible JSON,
keeps exact bytes in an explicit blob backend, and leaves read models rebuildable.

```text
optional local SQLite queue → PostgreSQL canonical events → ordered replay
                                      └→ PostgreSQL blobs or filesystem CAS
```

Read [the architecture guide](docs/architecture.md) for the authority model,
design rationale, code-review map, and explicit non-goals.

## Commands

```sh
duro append --local PATH --type TYPE --content JSON [--refs JSON] [--resource-uri ABSOLUTE_URI]
duro file --local PATH --body FILE --source REFERENCE [--resource-uri ABSOLUTE_URI] [--type EVENT_TYPE] [--media-type TYPE] [--id EVENT_UUID] [--occurred-at RFC3339] [--blob-store postgres|filesystem] [--blob-root ABSOLUTE_PATH]
duro init --postgres ADMIN_DSN
duro sync --local PATH --postgres DSN [--blob-store postgres|filesystem] [--blob-root ABSOLUTE_PATH]
duro pull --local PATH --postgres DSN
duro read --postgres DSN --source SOURCE [--max-bytes N] [--blob-store postgres|filesystem] [--blob-root ABSOLUTE_PATH]
duro list --postgres DSN [--after SEQUENCE] [--limit N]
duro kg --postgres DSN [--subject S] [--predicate P] [--object O]
duro artifact put --postgres DSN --blob-root ABSOLUTE_PATH --file PATH [--locator ABSOLUTE_URI ...]
duro artifact verify --postgres DSN --blob-root ABSOLUTE_PATH --sha256 LOWERCASE_HEX
duro artifact locate --postgres DSN [--prefix LITERAL_URI_PREFIX]
```

Each command emits machine-readable JSON on success. Run `duro init` once with
an administrator/provisioning DSN. `sync` and `pull` deliberately do not apply
DDL, so ordinary ingest credentials can be denied schema-mutation privileges.

For large immutable artifacts, use the filesystem backend on both `file` and
`sync`. `file` streams the source into the filesystem CAS and queues only its
digest, size, and media type in SQLite; `sync` re-verifies those bytes before
linking the canonical event, then reclaims local retry bytes while retaining
small digest metadata for idempotent conflict checks. Use a
specific event type such as `dataset.filed` or `run.artifact.filed`; the
default `document.filed` remains for Duro's document reader. This is for
artifact-level provenance, not one event per dataset row.

`artifact` catalogs filesystem-CAS bytes without creating an event: `put`
writes verified bytes then records their digest, size, verification time, and
optional source locator observations; `verify` rehashes one cataloged blob;
`locate` emits those observations as JSON. Locators are observations, not
fallback read paths or canonical resource identity.

`kg` is a disposable knowledge-graph projection. It replays
`knowledge.fact` events from sequence zero, applies reversible
`knowledge.fact.invalidated` events, deduplicates exact `(subject, predicate,
object)` triples, and returns canonical sequence/event provenance.

## Local PostgreSQL with Docker Compose

`compose.yaml` is a PostgreSQL service definition. It requires an explicit
password and can bind PostgreSQL's data directory to a host path. Provision the
embedded schema explicitly with `duro init` using an administrator/provisioning
DSN before ordinary clients or the least-privilege writer open the database.
The default bind is loopback; for an internal Compose deployment, do not
publish a port.

```sh
export DURO_POSTGRES_PASSWORD='development-only-password'
docker compose up -d postgres
docker compose ps

DURO_POSTGRES_TEST_DSN='postgresql://duro:***@localhost:5433/duro?sslmode=disable' \
  go test -count=1 ./...
```

The included Compose file is a one-role local-development database. A private
deployment must provision its own restricted PostgreSQL roles and SCRAM
passwords. Duro does not impose a transport policy: the deployment must keep
PostgreSQL on an internal network or loopback-only bind and must never expose a
plaintext listener publicly.

## Blob backends

PostgreSQL is the default blob backend. To retain canonical document bytes on a
filesystem mounted on the Duro host, use the same explicit backend selection
for both `sync` and `read`:

```sh
duro sync --local /var/lib/duro/queue.sqlite --postgres DSN \
  --blob-store filesystem --blob-root /srv/duro-blobs
duro read --postgres DSN --source urn:example:document:123 \
  --blob-store filesystem --blob-root /srv/duro-blobs
```

The root must be absolute. The filesystem backend uses a sharded SHA-256
content-addressed layout and verifies a blob before returning it. In this mode,
the filesystem is byte authority while PostgreSQL retains event provenance and
metadata. Keep SQLite on local storage: it is a single-writer durable queue,
not shared network storage.

Stop the container while preserving its data with `docker compose down`. Use an
owner-readable environment file containing a high-entropy
`DURO_POSTGRES_PASSWORD`, an operator-controlled data directory, and a
loopback-only or private-network bind. Do not use `docker compose down -v`
against a deployment dataset.

## Integration verification

Run the integration suite against a disposable PostgreSQL database:

```sh
cd path/to/duro-ledger
DURO_POSTGRES_TEST_DSN='postgresql://USER@HOST/DB?sslmode=disable' \
  go test -count=1 -v ./test/integration
```

The suite covers offline restart, canonical insertion and idempotency, changed
payload conflicts, invalid submissions, atomic blob acceptance, fresh-replica
pull, concurrent sequence assignment, crash-window retry, and replay from
sequence zero. Case 10 additionally requires `DURO_INGEST_TEST_DSN`, a
restricted ordinary-ingest role, and verifies that the role cannot update or
delete canonical rows. Without that DSN the test is explicitly skipped; a
superuser connection is not accepted as proof of the authorization boundary.

`pkg/cas` is Duro's native filesystem content-addressed store. Its artifact
catalog commands intentionally omit destructive reconciliation and multi-store
replica management until a concrete deployment needs them.
