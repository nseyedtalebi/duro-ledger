package postgres

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nseyedtalebi/duro-ledger/internal/pgtest"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
)

// initializedDB returns an administrator DSN for a fresh database with
// Duro's schema applied.
func initializedDB(t *testing.T) string {
	t.Helper()
	dsn := pgtest.NewDatabase(t)
	if err := Initialize(dsn); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return dsn
}

func openStore(t *testing.T, dsn string) *Store {
	t.Helper()
	s, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func openDBT(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := openDB(dsn)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// assertUUIDv7 checks the version and variant bits of a database-generated
// id: the contract requires database-side UUIDv7, not any UUID.
func assertUUIDv7(t *testing.T, id string) {
	t.Helper()
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	if err != nil || len(raw) != 16 {
		t.Fatalf("id %q is not a 16-byte UUID: %v", id, err)
	}
	if version := raw[6] >> 4; version != 7 {
		t.Errorf("id %s has UUID version %d, want 7", id, version)
	}
	if variant := raw[8] >> 6; variant != 0b10 {
		t.Errorf("id %s has variant bits %02b, want 10", id, variant)
	}
}

func TestAppendRoundTrip(t *testing.T) {
	dsn := initializedDB(t)
	// Initialize is idempotent against an already-compliant database.
	if err := Initialize(dsn); err != nil {
		t.Fatalf("second Initialize: %v", err)
	}
	s := openStore(t, dsn)

	before := time.Now().Add(-time.Minute)
	se, err := s.Append(event.New{EventType: "document.tagged", Content: json.RawMessage(`{"tag":"reviewed"}`)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	assertUUIDv7(t, se.ID)
	if se.EventType != "document.tagged" {
		t.Errorf("event_type = %q", se.EventType)
	}
	if se.ReceivedAt.Before(before) || se.ReceivedAt.After(time.Now().Add(time.Minute)) {
		t.Errorf("received_at = %v, want the insertion instant", se.ReceivedAt)
	}
	if string(se.Refs) != "{}" {
		t.Errorf("refs = %s, want the {} default", se.Refs)
	}

	db := openDBT(t, dsn)
	var sessionUser string
	if err := db.QueryRow(`SELECT session_user`).Scan(&sessionUser); err != nil {
		t.Fatal(err)
	}
	if se.Actor != sessionUser {
		t.Errorf("actor = %q, want the authenticated login identity %q", se.Actor, sessionUser)
	}

	// Committed readback through a plain SQL read, as a projector would do.
	var gotID, gotType, gotActor, gotContent, gotRefs string
	if err := db.QueryRow(`SELECT id::text, event_type, actor, content::text, refs::text FROM public.events ORDER BY id`).
		Scan(&gotID, &gotType, &gotActor, &gotContent, &gotRefs); err != nil {
		t.Fatalf("readback: %v", err)
	}
	if gotID != se.ID || gotType != se.EventType || gotActor != se.Actor || gotRefs != "{}" {
		t.Errorf("readback mismatch: %s %s %s %s", gotID, gotType, gotActor, gotRefs)
	}
	if gotContent != `{"tag": "reviewed"}` {
		t.Errorf("content = %s, want jsonb-normalized form", gotContent)
	}

	// Repeating the same input creates another event.
	se2, err := s.Append(event.New{EventType: "document.tagged", Content: json.RawMessage(`{"tag":"reviewed"}`)})
	if err != nil {
		t.Fatalf("second Append: %v", err)
	}
	if se2.ID == se.ID {
		t.Fatal("repeated append reused the same id")
	}
	if se2.ID < se.ID {
		t.Errorf("uuidv7 ids should ascend with time: %s then %s", se.ID, se2.ID)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM public.events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("row count = %d, want 2", count)
	}

	// Client-side validation failures never reach the database.
	if err := func() error { _, err := s.Append(event.New{EventType: " \t "}); return err }(); err == nil {
		t.Error("Append with blank event_type = nil, want validation error")
	}
	var ae *AppendError
	if _, err := s.Append(event.New{EventType: "a", Content: json.RawMessage(`[]`)}); err == nil || errors.As(err, &ae) {
		t.Errorf("Append with non-object content = %v, want a plain validation error", err)
	}
}

func TestGet(t *testing.T) {
	dsn := initializedDB(t)
	admin := openStore(t, dsn)
	want, err := admin.Append(event.New{EventType: "readback", Content: json.RawMessage(`{"n":1}`), Refs: json.RawMessage(`{"source":"test"}`)})
	if err != nil {
		t.Fatal(err)
	}
	role, readerDSN := pgtest.NewRole(t, dsn)
	if err := ProvisionReader(dsn, role); err != nil {
		t.Fatal(err)
	}
	reader := openStore(t, readerDSN)
	for _, id := range []string{want.ID, strings.ToUpper(want.ID)} {
		got, err := reader.Get(id)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("Get(%q) = %+v, %v; want %+v", id, got, err, want)
		}
	}
	if _, err := reader.Get("00000000-0000-7000-8000-000000000000"); !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("missing event error = %v", err)
	}
	if _, err := (&Store{}).Get("bad"); err == nil || errors.Is(err, ErrEventNotFound) {
		t.Fatalf("invalid id must fail before database access: %v", err)
	}
	var count int
	if err := admin.db.QueryRow(`SELECT count(*) FROM public.events`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("Get changed ledger: count=%d, err=%v", count, err)
	}
	reader.Close()
	if _, err := reader.Get(want.ID); err == nil || errors.Is(err, ErrEventNotFound) {
		t.Fatalf("closed database should return a database error, got %v", err)
	}
}

// TestDatabaseEnforcesFieldRules drives the constraints directly, because
// the contract makes PostgreSQL -- not Duro -- the authority for them.
func TestDatabaseEnforcesFieldRules(t *testing.T) {
	db := openDBT(t, initializedDB(t))

	// A JSON object that is over 1 MiB as raw text but normalizes well under
	// it must be accepted: the limit is measured on jsonb::text.
	padding := strings.Repeat(" ", 1<<20)
	paddedDuplicateKeys := `{` + padding + `"k"` + padding + `:` + padding + `1,"k":2` + padding + `}`
	var stored string
	if err := db.QueryRow(
		`INSERT INTO public.events (event_type, content) VALUES ('t', $1::jsonb) RETURNING content::text`,
		paddedDuplicateKeys).Scan(&stored); err != nil {
		t.Fatalf("whitespace-padded input that normalizes small must be accepted: %v", err)
	}
	if stored != `{"k": 2}` {
		t.Errorf("stored content = %s, want last-value-wins normalization", stored)
	}

	// Compact JSON that is still over 1 MiB after normalization is rejected.
	oversize := `{"k":"` + strings.Repeat("x", 1<<20) + `"}`
	rejected := []struct {
		name, stmt, arg string
	}{
		{"empty event_type", `INSERT INTO public.events (event_type) VALUES ($1)`, ""},
		{"ascii blank event_type", `INSERT INTO public.events (event_type) VALUES ($1)`, "  \t\n "},
		{"unicode blank event_type", `INSERT INTO public.events (event_type) VALUES ($1)`, " 　 "},
		{"oversized event_type", `INSERT INTO public.events (event_type) VALUES ($1)`, strings.Repeat("a", 256)},
		{"multibyte oversized event_type", `INSERT INTO public.events (event_type) VALUES ($1)`, strings.Repeat("é", 128)},
		{"content array", `INSERT INTO public.events (event_type, content) VALUES ('t', $1::jsonb)`, `[]`},
		{"content scalar", `INSERT INTO public.events (event_type, content) VALUES ('t', $1::jsonb)`, `3`},
		{"content null", `INSERT INTO public.events (event_type, content) VALUES ('t', $1::jsonb)`, `null`},
		{"refs array", `INSERT INTO public.events (event_type, refs) VALUES ('t', $1::jsonb)`, `[]`},
		{"oversized content", `INSERT INTO public.events (event_type, content) VALUES ('t', $1::jsonb)`, oversize},
		{"oversized refs", `INSERT INTO public.events (event_type, refs) VALUES ('t', $1::jsonb)`, oversize},
	}
	for _, tc := range rejected {
		if _, err := db.Exec(tc.stmt, tc.arg); err == nil {
			t.Errorf("%s: insert succeeded, want rejection", tc.name)
		}
	}
	// A 255-byte event_type is fine.
	if _, err := db.Exec(`INSERT INTO public.events (event_type) VALUES ($1)`, strings.Repeat("a", 255)); err != nil {
		t.Errorf("255-byte event_type rejected: %v", err)
	}
	// Explicit NULLs in the database-owned columns are rejected outright.
	for _, col := range []string{"id", "received_at", "actor"} {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO public.events (event_type, %s) VALUES ('t', NULL)`, col)); err == nil {
			t.Errorf("NULL %s accepted", col)
		}
	}
}

// TestSearchPathCannotRedirectAppends puts a decoy "events" table ahead of
// public on the search path. Every statement Duro issues is schema-qualified,
// so appends must still land in public.events.
func TestSearchPathCannotRedirectAppends(t *testing.T) {
	dsn := initializedDB(t)
	admin := openDBT(t, dsn)
	for _, stmt := range []string{
		`CREATE SCHEMA decoy`,
		`CREATE TABLE decoy.events (id UUID DEFAULT pg_catalog.uuidv7(), received_at TIMESTAMPTZ DEFAULT pg_catalog.clock_timestamp(),
			event_type TEXT, actor TEXT DEFAULT 'forged', content JSONB DEFAULT '{}'::jsonb, refs JSONB DEFAULT '{}'::jsonb)`,
		`SET search_path TO decoy, public`,
	} {
		if _, err := admin.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// Apply the hostile search_path to every new session in this database.
	var dbName string
	if err := admin.QueryRow(`SELECT pg_catalog.current_database()`).Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(fmt.Sprintf(`ALTER DATABASE %s SET search_path TO decoy, public`, pgx.Identifier{dbName}.Sanitize())); err != nil {
		t.Fatalf("setting hostile search_path: %v", err)
	}

	// A fresh Initialize must still be a compatible no-op, and the append
	// must land in the canonical table.
	if err := Initialize(dsn); err != nil {
		t.Fatalf("Initialize under a hostile search_path: %v", err)
	}
	if _, err := openStore(t, dsn).Append(event.New{EventType: "not.decoyed"}); err != nil {
		t.Fatalf("Append under a hostile search_path: %v", err)
	}
	var canonical, decoyed int
	if err := admin.QueryRow(`SELECT (SELECT count(*) FROM public.events), (SELECT count(*) FROM decoy.events)`).Scan(&canonical, &decoyed); err != nil {
		t.Fatal(err)
	}
	if canonical != 1 || decoyed != 0 {
		t.Errorf("public.events has %d rows, decoy.events has %d; want 1 and 0", canonical, decoyed)
	}
}

func TestFreshSchemaHasNoPublicGrants(t *testing.T) {
	db := openDBT(t, initializedDB(t))
	for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"} {
		var has bool
		if err := db.QueryRow(`SELECT pg_catalog.has_table_privilege('public', 'public.events', $1)`, priv).Scan(&has); err != nil {
			t.Fatal(err)
		}
		if has {
			t.Errorf("PUBLIC holds %s on public.events", priv)
		}
	}
}

func TestInitializeRefusesIncompatibleSchema(t *testing.T) {
	// Each variant has the contract's exact column set and types, so a
	// shape-only compatibility check would wrongly accept every one of
	// them.
	variants := map[string]string{
		"no primary key": `CREATE TABLE public.events (
			id UUID NOT NULL DEFAULT pg_catalog.uuidv7(),
			received_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
			event_type TEXT NOT NULL, actor TEXT NOT NULL DEFAULT session_user,
			content JSONB NOT NULL DEFAULT '{}'::jsonb, refs JSONB NOT NULL DEFAULT '{}'::jsonb)`,
		"caller-supplied id": `CREATE TABLE public.events (
			id UUID NOT NULL PRIMARY KEY,
			received_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
			event_type TEXT NOT NULL, actor TEXT NOT NULL DEFAULT session_user,
			content JSONB NOT NULL DEFAULT '{}'::jsonb, refs JSONB NOT NULL DEFAULT '{}'::jsonb)`,
		"transaction timestamp": `CREATE TABLE public.events (
			id UUID NOT NULL DEFAULT pg_catalog.uuidv7() PRIMARY KEY,
			received_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
			event_type TEXT NOT NULL, actor TEXT NOT NULL DEFAULT session_user,
			content JSONB NOT NULL DEFAULT '{}'::jsonb, refs JSONB NOT NULL DEFAULT '{}'::jsonb)`,
		"forgeable actor": `CREATE TABLE public.events (
			id UUID NOT NULL DEFAULT pg_catalog.uuidv7() PRIMARY KEY,
			received_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
			event_type TEXT NOT NULL, actor TEXT NOT NULL,
			content JSONB NOT NULL DEFAULT '{}'::jsonb, refs JSONB NOT NULL DEFAULT '{}'::jsonb)`,
		"nullable content": `CREATE TABLE public.events (
			id UUID NOT NULL DEFAULT pg_catalog.uuidv7() PRIMARY KEY,
			received_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
			event_type TEXT NOT NULL, actor TEXT NOT NULL DEFAULT session_user,
			content JSONB DEFAULT '{}'::jsonb, refs JSONB NOT NULL DEFAULT '{}'::jsonb)`,
		"no checks": `CREATE TABLE public.events (
			id UUID NOT NULL DEFAULT pg_catalog.uuidv7() PRIMARY KEY,
			received_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
			event_type TEXT NOT NULL, actor TEXT NOT NULL DEFAULT session_user,
			content JSONB NOT NULL DEFAULT '{}'::jsonb, refs JSONB NOT NULL DEFAULT '{}'::jsonb,
			CONSTRAINT events_event_type_nonblank CHECK (event_type <> ''))`,
	}
	for name, ddl := range variants {
		t.Run(name, func(t *testing.T) {
			dsn := pgtest.NewDatabase(t)
			db := openDBT(t, dsn)
			if _, err := db.Exec(ddl); err != nil {
				t.Fatalf("creating legacy table: %v", err)
			}
			if _, err := db.Exec(`INSERT INTO public.events (id, event_type, actor) VALUES (pg_catalog.uuidv7(), 'legacy.event', 'someone')`); err != nil {
				// Not every variant accepts this exact insert; a row is
				// nice-to-have for the untouched check, not essential.
				t.Logf("legacy insert: %v", err)
			}
			err := Initialize(dsn)
			if err == nil {
				t.Fatal("Initialize accepted an incompatible public.events")
			}
			if !strings.Contains(err.Error(), "does not match the current contract schema") {
				t.Errorf("Initialize error = %v, want a schema-mismatch refusal", err)
			}
			// The legacy table and its data must be untouched: same
			// definition, same rows, and no leftover reference table.
			var relkind string
			if err := db.QueryRow(`SELECT relkind::text FROM pg_catalog.pg_class WHERE oid = 'public.events'::pg_catalog.regclass`).Scan(&relkind); err != nil {
				t.Fatalf("legacy table gone: %v", err)
			}
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM public.events`).Scan(&n); err != nil {
				t.Fatalf("legacy rows unreadable: %v", err)
			}
			var leftovers int
			if err := db.QueryRow(`SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'duro_events_reference'`).Scan(&leftovers); err != nil {
				t.Fatal(err)
			}
			if leftovers != 0 {
				t.Errorf("Initialize left %d reference tables behind", leftovers)
			}
		})
	}
}

func adminExec(t *testing.T, dsn, stmt string) {
	t.Helper()
	if _, err := openDBT(t, dsn).Exec(stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}

// TestInitializeRefusesDifferentSemantics covers relations whose column
// descriptors can look right while the relation means something else:
// unlogged storage (events would not survive a crash), a view, and extra
// user triggers or rules that can rewrite or suppress appends.
func TestInitializeRefusesDifferentSemantics(t *testing.T) {
	cases := []struct {
		name            string
		initializeFirst bool
		setup           []string
	}{
		{"unlogged table", false, []string{strings.ReplaceAll(schema, "CREATE TABLE IF NOT EXISTS", "CREATE UNLOGGED TABLE IF NOT EXISTS")}},
		{"view over another table", false, []string{
			strings.ReplaceAll(schema, "public.events", "public.events_real"),
			`CREATE VIEW public.events AS SELECT * FROM public.events_real`,
		}},
		{"user trigger", true, []string{
			`CREATE FUNCTION public.duro_test_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`,
			`CREATE TRIGGER duro_test_trigger BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION public.duro_test_trigger()`,
		}},
		{"rule", true, []string{
			`CREATE RULE duro_test_rule AS ON DELETE TO public.events DO INSTEAD NOTHING`,
		}},
		// Row-level security makes the canonical table a per-role filtered
		// projection: reads can be narrowed and appends rejected per role,
		// neither of which is the standalone contract.
		{"row security enabled", true, []string{
			`ALTER TABLE public.events ENABLE ROW LEVEL SECURITY`,
		}},
		{"row security forced", true, []string{
			`ALTER TABLE public.events FORCE ROW LEVEL SECURITY`,
		}},
		{"policy attached", true, []string{
			`CREATE POLICY duro_test_policy ON public.events FOR SELECT USING (true)`,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := pgtest.NewDatabase(t)
			if tc.initializeFirst {
				if err := Initialize(dsn); err != nil {
					t.Fatalf("Initialize: %v", err)
				}
				// An existing row witnesses that a refusal changes no data.
				adminExec(t, dsn, `INSERT INTO public.events (event_type) VALUES ('drift.witness')`)
			}
			for _, stmt := range tc.setup {
				adminExec(t, dsn, stmt)
			}
			err := Initialize(dsn)
			if err == nil {
				t.Fatal("Initialize accepted a relation with different semantics")
			}
			if !strings.Contains(err.Error(), "refusing") {
				t.Errorf("Initialize error = %v, want a refusal", err)
			}
			// Nothing was altered or dropped.
			var relkind string
			if err := openDBT(t, dsn).QueryRow(
				`SELECT relkind::text FROM pg_catalog.pg_class WHERE oid = 'public.events'::pg_catalog.regclass`).Scan(&relkind); err != nil {
				t.Fatalf("relation gone after refusal: %v", err)
			}
			if tc.initializeFirst {
				// The connecting role is the cluster superuser, so this count
				// is not itself filtered by the RLS cases above.
				var rows int
				if err := openDBT(t, dsn).QueryRow(`SELECT count(*) FROM public.events`).Scan(&rows); err != nil {
					t.Fatalf("counting rows after refusal: %v", err)
				}
				if rows != 1 {
					t.Errorf("events holds %d rows after a refusal, want the 1 that existed before", rows)
				}
			}
		})
	}
}

// privileges snapshots what a role can do to the canonical table, so a
// refused provisioning can be shown to have granted nothing.
func privileges(t *testing.T, dsn, role string) string {
	t.Helper()
	var out string
	if err := openDBT(t, dsn).QueryRow(`
		SELECT pg_catalog.concat_ws(',',
			pg_catalog.has_table_privilege($1, 'public.events', 'SELECT'),
			pg_catalog.has_any_column_privilege($1, 'public.events', 'INSERT'),
			pg_catalog.has_any_column_privilege($1, 'public.events', 'UPDATE'),
			pg_catalog.has_table_privilege($1, 'public.events', 'DELETE'),
			pg_catalog.has_schema_privilege($1, 'public', 'USAGE'))`, role).Scan(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestProvisionEligibility is the writer/reader boundary Hypatia broke:
// every form of authority that would let a "restricted" role forge
// server-owned columns or mutate the ledger -- including authority reachable
// only by SET ROLE, which NOINHERIT hides from inherited-privilege checks --
// must be refused, and a refusal must grant nothing.
func TestProvisionEligibility(t *testing.T) {
	stmt := func(format string) func(*testing.T, string, string) {
		return func(t *testing.T, dsn, role string) {
			adminExec(t, dsn, fmt.Sprintf(format, pgx.Identifier{role}.Sanitize()))
		}
	}
	cases := []struct {
		name    string
		prepare func(t *testing.T, dsn, role string)
	}{
		{"insert on id", stmt(`GRANT INSERT (id) ON public.events TO %s`)},
		{"insert on received_at", stmt(`GRANT INSERT (received_at) ON public.events TO %s`)},
		{"insert on actor", stmt(`GRANT INSERT (actor) ON public.events TO %s`)},
		{"table-wide insert", stmt(`GRANT INSERT ON public.events TO %s`)},
		{"table-wide update", stmt(`GRANT UPDATE ON public.events TO %s`)},
		{"column update", stmt(`GRANT UPDATE (event_type) ON public.events TO %s`)},
		{"delete", stmt(`GRANT DELETE ON public.events TO %s`)},
		{"truncate", stmt(`GRANT TRUNCATE ON public.events TO %s`)},
		{"trigger", stmt(`GRANT TRIGGER ON public.events TO %s`)},
		{"createrole", stmt(`ALTER ROLE %s CREATEROLE`)},
		{"createdb", stmt(`ALTER ROLE %s CREATEDB`)},
		{"bypassrls", stmt(`ALTER ROLE %s BYPASSRLS`)},
		{"replication", stmt(`ALTER ROLE %s REPLICATION`)},
		{"table owner", stmt(`ALTER TABLE public.events OWNER TO %s`)},
		{"schema owner", stmt(`ALTER SCHEMA public OWNER TO %s`)},
		{"database owner", func(t *testing.T, dsn, role string) {
			var db string
			if err := openDBT(t, dsn).QueryRow(`SELECT pg_catalog.current_database()`).Scan(&db); err != nil {
				t.Fatal(err)
			}
			var admin string
			if err := openDBT(t, dsn).QueryRow(`SELECT session_user`).Scan(&admin); err != nil {
				t.Fatal(err)
			}
			adminExec(t, dsn, fmt.Sprintf(`ALTER DATABASE %s OWNER TO %s`, pgx.Identifier{db}.Sanitize(), pgx.Identifier{role}.Sanitize()))
			// Hand ownership back before the role is dropped: a database is
			// not covered by DROP OWNED BY.
			t.Cleanup(func() {
				adminExec(t, dsn, fmt.Sprintf(`ALTER DATABASE %s OWNER TO %s`, pgx.Identifier{db}.Sanitize(), pgx.Identifier{admin}.Sanitize()))
			})
		}},
		{"mutation reachable only by SET ROLE", func(t *testing.T, dsn, role string) {
			mutator, _ := pgtest.NewRole(t, dsn)
			adminExec(t, dsn, fmt.Sprintf(`GRANT UPDATE ON public.events TO %s`, pgx.Identifier{mutator}.Sanitize()))
			adminExec(t, dsn, fmt.Sprintf(`ALTER ROLE %s NOINHERIT`, pgx.Identifier{role}.Sanitize()))
			adminExec(t, dsn, fmt.Sprintf(`GRANT %s TO %s WITH INHERIT FALSE, SET TRUE`,
				pgx.Identifier{mutator}.Sanitize(), pgx.Identifier{role}.Sanitize()))
		}},
		{"insert reachable only by SET ROLE", func(t *testing.T, dsn, role string) {
			forger, _ := pgtest.NewRole(t, dsn)
			adminExec(t, dsn, fmt.Sprintf(`GRANT INSERT (actor) ON public.events TO %s`, pgx.Identifier{forger}.Sanitize()))
			adminExec(t, dsn, fmt.Sprintf(`ALTER ROLE %s NOINHERIT`, pgx.Identifier{role}.Sanitize()))
			adminExec(t, dsn, fmt.Sprintf(`GRANT %s TO %s WITH INHERIT FALSE, SET TRUE`,
				pgx.Identifier{forger}.Sanitize(), pgx.Identifier{role}.Sanitize()))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := initializedDB(t)
			role, _ := pgtest.NewRole(t, dsn)
			tc.prepare(t, dsn, role)

			before := privileges(t, dsn, role)
			if err := ProvisionWriter(dsn, role); err == nil {
				t.Error("ProvisionWriter accepted a role it cannot restrict")
			}
			if err := ProvisionReader(dsn, role); err == nil {
				t.Error("ProvisionReader accepted a role it cannot restrict")
			}
			if after := privileges(t, dsn, role); after != before {
				t.Errorf("a refused provisioning changed privileges: %s -> %s", before, after)
			}
		})
	}
}

func TestProvisionAcceptsSafeRolesIdempotently(t *testing.T) {
	dsn := initializedDB(t)
	writer, writerDSN := pgtest.NewRole(t, dsn)
	reader, readerDSN := pgtest.NewRole(t, dsn)
	// Twice each: re-provisioning an already-provisioned role must succeed,
	// even though the writer now holds INSERT on the unprotected columns.
	for i := range 2 {
		if err := ProvisionWriter(dsn, writer); err != nil {
			t.Fatalf("ProvisionWriter (pass %d): %v", i, err)
		}
		if err := ProvisionReader(dsn, reader); err != nil {
			t.Fatalf("ProvisionReader (pass %d): %v", i, err)
		}
	}
	se, err := openStore(t, writerDSN).Append(event.New{EventType: "still.works"})
	if err != nil {
		t.Fatalf("writer Append: %v", err)
	}
	if se.Actor != writer {
		t.Errorf("actor = %q, want %q", se.Actor, writer)
	}
	// The provisioned writer still cannot touch server-owned columns.
	if _, err := openDBT(t, writerDSN).Exec(`INSERT INTO public.events (event_type, actor) VALUES ('forge', 'forged')`); err == nil {
		t.Error("provisioned writer could forge actor")
	}
	var n int
	if err := openDBT(t, readerDSN).QueryRow(`SELECT count(*) FROM public.events`).Scan(&n); err != nil || n != 1 {
		t.Errorf("reader SELECT = %d, %v", n, err)
	}
}

func TestInitializeConcurrent(t *testing.T) {
	dsn := pgtest.NewDatabase(t)
	const n = 6
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = Initialize(dsn)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent Initialize %d: %v", i, err)
		}
	}
	if _, err := openStore(t, dsn).Append(event.New{EventType: "after.concurrent.init"}); err != nil {
		t.Fatalf("Append after concurrent Initialize: %v", err)
	}
}

func TestWriterRoleBoundary(t *testing.T) {
	adminDSN := initializedDB(t)
	writer, writerDSN := pgtest.NewRole(t, adminDSN)
	if err := ProvisionWriter(adminDSN, writer); err != nil {
		t.Fatalf("ProvisionWriter: %v", err)
	}
	// A second writer role, granted to the first, exercises SET ROLE.
	other, _ := pgtest.NewRole(t, adminDSN)
	if err := ProvisionWriter(adminDSN, other); err != nil {
		t.Fatalf("ProvisionWriter(other): %v", err)
	}
	admin := openDBT(t, adminDSN)
	if _, err := admin.Exec(fmt.Sprintf(`GRANT %s TO %s`, pgx.Identifier{other}.Sanitize(), pgx.Identifier{writer}.Sanitize())); err != nil {
		t.Fatalf("granting role membership: %v", err)
	}

	store := openStore(t, writerDSN)
	se, err := store.Append(event.New{EventType: "writer.append"})
	if err != nil {
		t.Fatalf("writer Append: %v", err)
	}
	if se.Actor != writer {
		t.Errorf("actor = %q, want the writer login identity %q", se.Actor, writer)
	}

	db := openDBT(t, writerDSN)
	forbidden := []struct{ name, stmt string }{
		{"forge id", `INSERT INTO public.events (event_type, id) VALUES ('forge', pg_catalog.uuidv7())`},
		{"forge received_at", `INSERT INTO public.events (event_type, received_at) VALUES ('forge', '2000-01-01T00:00:00Z')`},
		{"forge actor", `INSERT INTO public.events (event_type, actor) VALUES ('forge', 'somebody-else')`},
		{"insert whole row", `INSERT INTO public.events VALUES (pg_catalog.uuidv7(), pg_catalog.now(), 'forge', 'somebody-else', '{}', '{}')`},
		{"update", `UPDATE public.events SET event_type = 'rewritten'`},
		{"update actor", `UPDATE public.events SET actor = 'somebody-else'`},
		{"delete", `DELETE FROM public.events`},
		{"truncate", `TRUNCATE public.events`},
		{"alter", `ALTER TABLE public.events DROP CONSTRAINT events_event_type_nonblank`},
		{"drop", `DROP TABLE public.events`},
		{"rename", `ALTER TABLE public.events RENAME TO events_old`},
	}
	for _, tc := range forbidden {
		if _, err := db.Exec(tc.stmt); err == nil {
			t.Errorf("writer was allowed to %s", tc.name)
		} else if !strings.Contains(err.Error(), "permission denied") && !strings.Contains(err.Error(), "must be owner") {
			t.Errorf("writer %s failed for the wrong reason: %v", tc.name, err)
		}
	}

	// SET ROLE changes current_user but never session_user, so actor still
	// records the authenticated login identity.
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(`SET ROLE %s`, pgx.Identifier{other}.Sanitize())); err != nil {
		t.Fatalf("SET ROLE: %v", err)
	}
	var actor, currentUser string
	if err := conn.QueryRowContext(t.Context(),
		`INSERT INTO public.events (event_type) VALUES ('set.role') RETURNING actor, current_user`).Scan(&actor, &currentUser); err != nil {
		t.Fatalf("insert after SET ROLE: %v", err)
	}
	if currentUser != other {
		t.Fatalf("current_user = %q, want %q (test setup)", currentUser, other)
	}
	if actor != writer {
		t.Errorf("actor after SET ROLE = %q, want the session_user %q", actor, writer)
	}
}

func TestReaderRoleBoundary(t *testing.T) {
	adminDSN := initializedDB(t)
	if _, err := openStore(t, adminDSN).Append(event.New{EventType: "readable"}); err != nil {
		t.Fatalf("seed Append: %v", err)
	}
	reader, readerDSN := pgtest.NewRole(t, adminDSN)
	if err := ProvisionReader(adminDSN, reader); err != nil {
		t.Fatalf("ProvisionReader: %v", err)
	}
	db := openDBT(t, readerDSN)
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM public.events`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("reader SELECT = %d, %v", n, err)
	}
	if _, err := db.Exec(`INSERT INTO public.events (event_type) VALUES ('reader.append')`); err == nil {
		t.Error("reader was allowed to append")
	}
	// Through the library path too: a reader cannot append.
	if _, err := openStore(t, readerDSN).Append(event.New{EventType: "reader.append"}); err == nil {
		t.Error("reader Append = nil, want permission denied")
	} else {
		var ae *AppendError
		if !errors.As(err, &ae) || ae.Outcome != OutcomeNotCommitted {
			t.Errorf("reader Append error = %v, want *AppendError with not_committed", err)
		}
	}
}

func TestProvisionRefusesUnsafeRoles(t *testing.T) {
	adminDSN := initializedDB(t)
	admin := openDBT(t, adminDSN)

	var owner string
	if err := admin.QueryRow(`SELECT session_user`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionWriter(adminDSN, owner); err == nil {
		t.Error("ProvisionWriter accepted the administrator/owner role")
	}
	if err := ProvisionWriter(adminDSN, "duro_role_does_not_exist"); err == nil {
		t.Error("ProvisionWriter accepted a nonexistent role")
	}

	super, _ := pgtest.NewRole(t, adminDSN)
	if _, err := admin.Exec(fmt.Sprintf(`ALTER ROLE %s SUPERUSER`, pgx.Identifier{super}.Sanitize())); err != nil {
		t.Fatalf("making superuser: %v", err)
	}
	if err := ProvisionWriter(adminDSN, super); err == nil {
		t.Error("ProvisionWriter accepted a superuser role")
	}

	mutator, _ := pgtest.NewRole(t, adminDSN)
	if _, err := admin.Exec(fmt.Sprintf(`GRANT UPDATE ON public.events TO %s`, pgx.Identifier{mutator}.Sanitize())); err != nil {
		t.Fatalf("granting UPDATE: %v", err)
	}
	if err := ProvisionWriter(adminDSN, mutator); err == nil {
		t.Error("ProvisionWriter accepted a role that already holds UPDATE")
	}

	// Inherited mutation privilege counts too.
	group, _ := pgtest.NewRole(t, adminDSN)
	member, _ := pgtest.NewRole(t, adminDSN)
	if _, err := admin.Exec(fmt.Sprintf(`GRANT DELETE ON public.events TO %s`, pgx.Identifier{group}.Sanitize())); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(fmt.Sprintf(`GRANT %s TO %s`, pgx.Identifier{group}.Sanitize(), pgx.Identifier{member}.Sanitize())); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionWriter(adminDSN, member); err == nil {
		t.Error("ProvisionWriter accepted a role inheriting DELETE")
	}
	// A refusal must not have added any grant on the way.
	var hasInsert bool
	if err := admin.QueryRow(`SELECT pg_catalog.has_table_privilege($1, 'public.events', 'INSERT')`, member).Scan(&hasInsert); err != nil {
		t.Fatal(err)
	}
	if hasInsert {
		t.Error("a refused provisioning still granted INSERT")
	}

	nologin, _ := pgtest.NewRole(t, adminDSN)
	if _, err := admin.Exec(fmt.Sprintf(`ALTER ROLE %s NOLOGIN`, pgx.Identifier{nologin}.Sanitize())); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionWriter(adminDSN, nologin); err == nil {
		t.Error("ProvisionWriter accepted a role that cannot log in")
	}

	// A reader must not already be able to append.
	appender, _ := pgtest.NewRole(t, adminDSN)
	if err := ProvisionWriter(adminDSN, appender); err != nil {
		t.Fatalf("ProvisionWriter: %v", err)
	}
	if err := ProvisionReader(adminDSN, appender); err == nil {
		t.Error("ProvisionReader accepted a role that already holds INSERT")
	}
}

// TestSnapshotReadAndCommitOrderInversion demonstrates the documented
// reading semantics: ids ascend in generation order, which is not commit
// order, and a read-only repeatable-read transaction sees one stable
// snapshot for a whole multi-query rebuild.
func TestSnapshotReadAndCommitOrderInversion(t *testing.T) {
	dsn := initializedDB(t)
	db := openDBT(t, dsn)
	ctx := t.Context()

	// A commits first in id order but last in commit order.
	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var idA string
	if err := txA.QueryRow(`INSERT INTO public.events (event_type) VALUES ('early.id.late.commit') RETURNING id::text`).Scan(&idA); err != nil {
		t.Fatal(err)
	}
	txB, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var idB string
	if err := txB.QueryRow(`INSERT INTO public.events (event_type) VALUES ('late.id.early.commit') RETURNING id::text`).Scan(&idB); err != nil {
		t.Fatal(err)
	}
	if !(idA < idB) {
		t.Fatalf("expected ascending ids by generation time: %s then %s", idA, idB)
	}
	if err := txB.Commit(); err != nil {
		t.Fatal(err)
	}

	// A projector starting now sees B but not A: id order is not commit
	// order, and events outside the snapshot arrive on the next rebuild.
	snapshot, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Rollback()
	first := readIDs(t, snapshot)
	if len(first) != 1 || first[0] != idB {
		t.Fatalf("snapshot = %v, want only the committed %s", first, idB)
	}

	if err := txA.Commit(); err != nil {
		t.Fatal(err)
	}
	// Every query in the rebuild transaction uses the same snapshot, even
	// though A has since committed with a lower id.
	if second := readIDs(t, snapshot); len(second) != 1 || second[0] != idB {
		t.Errorf("second read in the same repeatable-read transaction = %v, want %v", second, first)
	}
	// A read-only transaction cannot write, so a projector cannot corrupt
	// the ledger it is rebuilding from.
	if _, err := snapshot.Exec(`INSERT INTO public.events (event_type) VALUES ('projector.write')`); err == nil {
		t.Error("read-only transaction was allowed to insert")
	}
	if err := snapshot.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatal(err)
	}

	// The next rebuild sees both, in ascending id order.
	fresh, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Rollback()
	if all := readIDs(t, fresh); len(all) != 2 || all[0] != idA || all[1] != idB {
		t.Errorf("full reread = %v, want [%s %s]", all, idA, idB)
	}
}

func readIDs(t *testing.T, tx *sql.Tx) []string {
	t.Helper()
	rows, err := tx.Query(`SELECT id::text FROM public.events ORDER BY id`)
	if err != nil {
		t.Fatalf("reading ledger: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestClassifyCommitErr(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want CommitOutcome
	}{
		{"server error", &pgconn.PgError{Message: "rejected"}, OutcomeNotCommitted},
		{"wrapped server error", fmt.Errorf("commit: %w", &pgconn.PgError{}), OutcomeNotCommitted},
		{"commit rolled back", pgx.ErrTxCommitRollback, OutcomeNotCommitted},
		{"tx already done", sql.ErrTxDone, OutcomeNotCommitted},
		{"connection lost", io.ErrUnexpectedEOF, OutcomeUnknown},
		{"wrapped connection loss", fmt.Errorf("commit: %w", io.ErrUnexpectedEOF), OutcomeUnknown},
		// Ordinary ERROR-severity answers are the server saying this
		// transaction did not commit.
		{"check violation", &pgconn.PgError{Severity: "ERROR", Code: "23514", Message: "check constraint"}, OutcomeNotCommitted},
		{"deferred constraint at commit", &pgconn.PgError{Severity: "ERROR", Code: "23505"}, OutcomeNotCommitted},
		// A dying session is not evidence of a rollback: the commit may have
		// landed just before the acknowledgement was lost.
		{"fatal severity", &pgconn.PgError{Severity: "FATAL", Code: "XX000"}, OutcomeUnknown},
		{"panic severity", &pgconn.PgError{Severity: "PANIC", Code: "XX000"}, OutcomeUnknown},
		{"connection exception", &pgconn.PgError{Severity: "ERROR", Code: "08006"}, OutcomeUnknown},
		{"connection does not exist", &pgconn.PgError{Severity: "ERROR", Code: "08003"}, OutcomeUnknown},
		{"admin shutdown", &pgconn.PgError{Severity: "FATAL", Code: "57P01"}, OutcomeUnknown},
		{"crash shutdown", &pgconn.PgError{Severity: "FATAL", Code: "57P02"}, OutcomeUnknown},
		{"cannot connect now", &pgconn.PgError{Severity: "FATAL", Code: "57P03"}, OutcomeUnknown},
		{"wrapped admin shutdown", fmt.Errorf("commit: %w", &pgconn.PgError{Code: "57P01"}), OutcomeUnknown},
	} {
		if got := classifyCommitErr(tc.err); got != tc.want {
			t.Errorf("classifyCommitErr(%s) = %s, want %s", tc.name, got, tc.want)
		}
	}
	if OutcomeUnknown.String() != "unknown" || OutcomeNotCommitted.String() != "not_committed" {
		t.Error("CommitOutcome strings are part of the reported error schema")
	}
}

// TestAppendOnLostConnection covers the connection-loss path without a
// proxy: closing the pool's connections underneath an open transaction
// makes the COMMIT unanswerable.
func TestAppendOnLostConnection(t *testing.T) {
	dsn := initializedDB(t)
	s := openStore(t, dsn)
	if _, err := s.Append(event.New{EventType: "before.close"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := s.Append(event.New{EventType: "after.close"})
	var ae *AppendError
	if !errors.As(err, &ae) {
		t.Fatalf("Append on a closed store = %v, want *AppendError", err)
	}
	// database/sql refuses to use a closed pool before sending anything, so
	// this is a definite non-commit, never an unknown outcome.
	if ae.Outcome != OutcomeNotCommitted {
		t.Errorf("outcome = %s, want not_committed", ae.Outcome)
	}
}
