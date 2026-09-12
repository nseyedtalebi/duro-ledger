# Integration verification

Run against a disposable PostgreSQL database, never a production database.
The suite resets `events` and `blobs` between subcases while holding advisory
lock `918273645` so package-level integration tests cannot truncate each
other's state.

Base suite:

```sh
cd path/to/duro-ledger
DURO_POSTGRES_TEST_DSN='postgresql://USER@HOST/DB?sslmode=disable' \
  go test -count=1 -v ./test/integration
```

Case 10 additionally needs an ordinary ingest role. The following is the
local PostgreSQL setup used for the verified run; replace the database/user
as appropriate and do not reuse a production role:

```sh
psql -d postgres -v ON_ERROR_STOP=1 -c \
  "CREATE ROLE duro_acceptance_ingest LOGIN; \
   GRANT CONNECT ON DATABASE postgres TO duro_acceptance_ingest; \
   GRANT USAGE ON SCHEMA public TO duro_acceptance_ingest; \
   GRANT SELECT, INSERT ON TABLE events, blobs TO duro_acceptance_ingest; \
   GRANT USAGE, SELECT ON SEQUENCE events_sequence_seq TO duro_acceptance_ingest;"

DURO_POSTGRES_TEST_DSN='postgresql://TEST_USER@localhost/postgres?sslmode=disable' \
DURO_INGEST_TEST_DSN='postgresql://duro_acceptance_ingest@localhost/postgres?sslmode=disable' \
  go test -count=1 -v ./test/integration
```

Teardown:

```sh
psql -d postgres -v ON_ERROR_STOP=1 -c \
  "REVOKE ALL PRIVILEGES ON TABLE events, blobs FROM duro_acceptance_ingest; \
   REVOKE ALL PRIVILEGES ON SEQUENCE events_sequence_seq FROM duro_acceptance_ingest; \
   REVOKE ALL PRIVILEGES ON SCHEMA public FROM duro_acceptance_ingest; \
   REVOKE CONNECT ON DATABASE postgres FROM duro_acceptance_ingest; \
   DROP ROLE duro_acceptance_ingest;"
```

If `DURO_INGEST_TEST_DSN` is absent, case 10 is explicitly skipped rather than
claimed as proven by a superuser connection.
