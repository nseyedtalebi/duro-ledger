package postgres

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nseyedtalebi/duro-ledger/pkg/event"
)

const evt1ID = "22222222-2222-2222-2222-222222222222"
const evt2ID = "33333333-3333-3333-3333-333333333333"
const evt3ID = "44444444-4444-4444-4444-444444444444"
const testAdvisoryLockKey = 918273645

// openTestStore skips the test clearly when DURO_POSTGRES_TEST_DSN is
// unset, and otherwise opens a Store against it with a clean events table.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("DURO_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("DURO_POSTGRES_TEST_DSN not set; skipping PostgreSQL integration test")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(`SELECT pg_advisory_lock($1)`, testAdvisoryLockKey); err != nil {
		raw.Close()
		t.Fatalf("acquire test lock: %v", err)
	}
	if err := Initialize(dsn); err != nil {
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
		t.Fatalf("Initialize: %v", err)
	}
	s, err := Open(dsn)
	if err != nil {
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.db.Exec(`TRUNCATE events, blobs RESTART IDENTITY`); err != nil {
		s.Close()
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
		t.Fatalf("truncate events: %v", err)
	}
	t.Cleanup(func() {
		s.Close()
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
	})
	return s
}

func mustEvent(t *testing.T, id, content string) event.Event {
	t.Helper()
	ev, err := event.New(id, "test.type", "tester", time.Now().UTC(), json.RawMessage(content), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	return ev
}

func mustEventAt(t *testing.T, id string, occurredAt time.Time, content string) event.Event {
	t.Helper()
	ev, err := event.New(id, "test.type", "tester", occurredAt, json.RawMessage(content), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	return ev
}

// TestSchemaDefinesBlobsAndJSONChecks runs without a live DSN: it checks the
// embedded schema text directly for the blobs table and the JSON-object
// CHECK constraints on events, so the spec shape is verified even where
// DURO_POSTGRES_TEST_DSN is unset.
func TestSchemaDefinesBlobsAndJSONChecks(t *testing.T) {
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS blobs",
		"sha256      BYTEA NOT NULL UNIQUE",
		"size_bytes  BIGINT NOT NULL CHECK (size_bytes >= 0)",
		"content     BYTEA,",
		"ALTER TABLE blobs ALTER COLUMN content DROP NOT NULL;",
		"blob_media_type TEXT",
		"resource_uri TEXT",
		"CHECK (jsonb_typeof(content) = 'object')",
		"CHECK (jsonb_typeof(refs) = 'object')",
		"CREATE OR REPLACE FUNCTION duro_bind_event_actor()",
		"current_user",
		"CREATE TRIGGER events_bind_actor",
	} {
		if !strings.Contains(schema, want) {
			t.Errorf("schema.sql missing expected fragment: %q", want)
		}
	}
}

func TestScopedEventQueryRejectsInvalidArgumentsBeforeDatabaseAccess(t *testing.T) {
	s := &Store{}
	for _, request := range []struct {
		types []string
		scope string
		after int64
		limit int
	}{
		{nil, "scope-a", 0, 1},
		{[]string{""}, "scope-a", 0, 1},
		{[]string{"hermes.memory.turn"}, "", 0, 1},
		{[]string{"hermes.memory.turn"}, "scope-a", -1, 1},
		{[]string{"hermes.memory.turn"}, "scope-a", 0, 0},
		{[]string{"hermes.memory.turn"}, "scope-a", 0, 501},
	} {
		if _, err := s.ListEventsByTypeAndScope(request.types, request.scope, request.after, request.limit); err == nil {
			t.Fatalf("invalid scoped query was accepted: %#v", request)
		}
	}
}

func TestSchemaDefinesEventsRLSPublicBaseline(t *testing.T) {
	for _, want := range []string{
		"ALTER TABLE events ENABLE ROW LEVEL SECURITY;",
		"DROP POLICY IF EXISTS events_public_baseline ON events;",
		"CREATE POLICY events_public_baseline ON events AS PERMISSIVE FOR ALL TO PUBLIC USING (true) WITH CHECK (true);",
	} {
		if !strings.Contains(schema, want) {
			t.Errorf("schema.sql missing events RLS baseline fragment: %q", want)
		}
	}
}

func TestSchemaDefinesScopedEventSequenceIndex(t *testing.T) {
	if !strings.Contains(schema, "events_type_scope_sequence_idx") {
		t.Fatal("schema.sql missing scoped event sequence index")
	}
}

func TestListEventsByTypeAndScopeFiltersCanonicalPage(t *testing.T) {
	s := openTestStore(t)
	for _, input := range []struct{ id, typ, content string }{
		{"11111111-1111-1111-1111-111111111111", "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c","user_text":"alpha","assistant_text":"a"}`},
		{"22222222-2222-2222-2222-222222222222", "hermes.memory.redacted", `{"deployment_scope":"scope-a","target_event_id":"11111111-1111-1111-1111-111111111111"}`},
		{"33333333-3333-3333-3333-333333333333", "hermes.memory.turn", `{"deployment_scope":"scope-b","conversation_id":"c","user_text":"beta","assistant_text":"b"}`},
		{"44444444-4444-4444-4444-444444444444", "other.type", `{"deployment_scope":"scope-a"}`},
	} {
		ev, err := event.New(input.id, input.typ, "writer", time.Now().UTC(), json.RawMessage(input.content), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Insert(ev); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.ListEventsByTypeAndScope([]string{"hermes.memory.turn", "hermes.memory.redacted"}, "scope-a", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Event.ID != "11111111-1111-1111-1111-111111111111" || rows[1].Event.ID != "22222222-2222-2222-2222-222222222222" {
		t.Fatalf("scoped rows = %#v", rows)
	}
}

func TestReadBlobMetadataReturnsVerifiedMetadataWithoutContent(t *testing.T) {
	s := openTestStore(t)
	body := []byte("canonical body")
	if outcome, _, err := s.InsertWithBlob(mustEvent(t, evt1ID, `{"source":"cura/drawers/example"}`), body, blobDigest(body), "text/plain"); err != nil || outcome != Accepted {
		t.Fatalf("InsertWithBlob = outcome:%v err:%v", outcome, err)
	}
	metadata, ok, err := s.ReadBlobMetadata(blobDigest(body))
	if err != nil || !ok {
		t.Fatalf("ReadBlobMetadata = metadata:%#v ok:%v err:%v", metadata, ok, err)
	}
	if metadata.SHA256 != blobDigest(body) || metadata.MediaType != "text/plain" || metadata.Size != int64(len(body)) || metadata.Content != nil {
		t.Fatalf("metadata = %#v", metadata)
	}
}

func TestResourceURIOrdinaryInsertPullAndChangedRetry(t *testing.T) {
	s := openTestStore(t)
	at := time.Date(2026, 9, 12, 20, 0, 0, 0, time.UTC)
	original, err := event.NewWithResourceURI(evt1ID, "document.filed", "writer", at, json.RawMessage(`{"source":"cura/drawers/example"}`), nil, "cura://palace/wing/room/example")
	if err != nil {
		t.Fatal(err)
	}
	outcome, sequence, err := s.Insert(original)
	if err != nil || outcome != Accepted || sequence <= 0 {
		t.Fatalf("Insert = outcome:%v sequence:%d err:%v", outcome, sequence, err)
	}
	outcome, retrySequence, err := s.Insert(original)
	if err != nil || outcome != AlreadyPresent || retrySequence != sequence {
		t.Fatalf("exact retry = outcome:%v sequence:%d err:%v", outcome, retrySequence, err)
	}
	changed := original
	changed.ResourceURI = "cura://palace/wing/room/correction"
	outcome, retrySequence, err = s.Insert(changed)
	if err != nil || outcome != Conflict || retrySequence != sequence {
		t.Fatalf("changed URI retry = outcome:%v sequence:%d err:%v", outcome, retrySequence, err)
	}
	rows, err := s.Pull(0, 10)
	if err != nil || len(rows) != 1 || rows[0].Event.ResourceURI != original.ResourceURI {
		t.Fatalf("Pull = rows:%#v err:%v", rows, err)
	}
}

func TestResourceURIBlobInsertChangedRetryConflicts(t *testing.T) {
	s := openTestStore(t)
	at := time.Date(2026, 9, 12, 20, 0, 0, 0, time.UTC)
	original, err := event.NewWithResourceURI(evt1ID, "document.filed", "writer", at, json.RawMessage(`{"source":"cura/drawers/example"}`), nil, "cura://palace/wing/room/example")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("canonical body")
	outcome, sequence, err := s.InsertWithBlob(original, body, blobDigest(body), "text/plain")
	if err != nil || outcome != Accepted || sequence <= 0 {
		t.Fatalf("InsertWithBlob = outcome:%v sequence:%d err:%v", outcome, sequence, err)
	}
	changed := original
	changed.ResourceURI = "cura://palace/wing/room/correction"
	outcome, retrySequence, err := s.InsertWithBlob(changed, body, blobDigest(body), "text/plain")
	if err != nil || outcome != Conflict || retrySequence != sequence {
		t.Fatalf("changed URI blob retry = outcome:%v sequence:%d err:%v", outcome, retrySequence, err)
	}
}

func TestInsertBindsNewActorToAuthenticatedRole(t *testing.T) {
	s := openTestStore(t)

	var currentUser string
	if err := s.db.QueryRow(`SELECT current_user`).Scan(&currentUser); err != nil {
		t.Fatalf("current_user: %v", err)
	}
	if _, _, err := s.Insert(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var actor string
	if err := s.db.QueryRow(`SELECT actor FROM events WHERE id = $1`, evt1ID).Scan(&actor); err != nil {
		t.Fatalf("actor: %v", err)
	}
	if actor != currentUser {
		t.Fatalf("actor = %q, want authenticated role %q", actor, currentUser)
	}
}

func TestInitializeAppliesSchema(t *testing.T) {
	dsn := os.Getenv("DURO_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("DURO_POSTGRES_TEST_DSN not set; skipping PostgreSQL integration test")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(`SELECT pg_advisory_lock($1)`, testAdvisoryLockKey); err != nil {
		t.Fatal(err)
	}
	defer raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)

	if err := Initialize(dsn); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	var events, blobs sql.NullString
	if err := raw.QueryRow(`SELECT to_regclass('public.events'), to_regclass('public.blobs')`).Scan(&events, &blobs); err != nil {
		t.Fatal(err)
	}
	if events.String != "events" || blobs.String != "blobs" {
		t.Fatalf("schema missing tables: events=%q blobs=%q", events.String, blobs.String)
	}
}

func TestInitializeEnablesEventsRLSWithPublicBaseline(t *testing.T) {
	dsn := os.Getenv("DURO_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("DURO_POSTGRES_TEST_DSN not set; skipping PostgreSQL integration test")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(`SELECT pg_advisory_lock($1)`, testAdvisoryLockKey); err != nil {
		t.Fatal(err)
	}
	defer raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)

	if err := Initialize(dsn); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if err := Initialize(dsn); err != nil {
		t.Fatalf("Initialize again: %v", err)
	}

	var (
		rlsEnabled  bool
		permissive  string
		command     string
		public      bool
		using       string
		withCheck   string
		policyCount int
	)
	err = raw.QueryRow(`
		SELECT c.relrowsecurity, v.permissive, v.cmd,
		       p.polroles = ARRAY[0::oid],
		       pg_get_expr(p.polqual, p.polrelid),
		       pg_get_expr(p.polwithcheck, p.polrelid),
		       (SELECT COUNT(*) FROM pg_policy AS p2 WHERE p2.polrelid = c.oid AND p2.polname = 'events_public_baseline')
		FROM pg_class AS c
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		JOIN pg_policy AS p ON p.polrelid = c.oid
		JOIN pg_policies AS v ON v.schemaname = n.nspname AND v.tablename = c.relname AND v.policyname = p.polname
		WHERE n.nspname = 'public' AND c.relname = 'events'
		  AND p.polname = 'events_public_baseline'
	`).Scan(&rlsEnabled, &permissive, &command, &public, &using, &withCheck, &policyCount)
	if err != nil {
		t.Fatalf("read events RLS baseline: %v", err)
	}
	if !rlsEnabled || permissive != "PERMISSIVE" || command != "ALL" || !public || using != "true" || withCheck != "true" || policyCount != 1 {
		t.Fatalf("events RLS baseline = rls=%t permissive=%q command=%q public=%t using=%q withCheck=%q count=%d", rlsEnabled, permissive, command, public, using, withCheck, policyCount)
	}
}

// TestInsertRejectsInvalidEventWithTypedError runs without a live DSN:
// Insert validates before touching the database, so a zero-value Store
// exercises the rejection path on its own.
func TestInsertRejectsInvalidEventWithTypedError(t *testing.T) {
	s := &Store{}
	invalid := event.Event{ID: evt1ID} // missing event_type/actor/content/refs

	outcome, seq, err := s.Insert(invalid)
	if outcome != Rejected {
		t.Fatalf("want Rejected, got %v", outcome)
	}
	if seq != 0 {
		t.Fatalf("want sequence 0, got %d", seq)
	}
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("want a *ValidationError, got %T: %v", err, err)
	}
	if !strings.Contains(verr.Error(), "event_type") {
		t.Fatalf("want validation reason in error, got %q", verr.Error())
	}
	// The typed error must wrap the underlying validation error so callers
	// can still inspect it with errors.Is/errors.As.
	if errors.Unwrap(verr) == nil {
		t.Fatal("want ValidationError to unwrap to the underlying error")
	}
}

func TestInsertAcceptsFreshEvent(t *testing.T) {
	s := openTestStore(t)

	outcome, seq, err := s.Insert(mustEvent(t, evt1ID, `{"a":1}`))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if outcome != Accepted {
		t.Fatalf("want Accepted, got %v", outcome)
	}
	if seq <= 0 {
		t.Fatalf("want positive sequence, got %d", seq)
	}
}

func TestInsertDuplicateSameContentIsAlreadyPresent(t *testing.T) {
	s := openTestStore(t)
	at := time.Now().UTC()
	ev := mustEventAt(t, evt1ID, at, `{"a":1}`)

	_, firstSeq, err := s.Insert(ev)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	outcome, seq, err := s.Insert(ev)
	if err != nil {
		t.Fatalf("Insert retry: %v", err)
	}
	if outcome != AlreadyPresent {
		t.Fatalf("want AlreadyPresent, got %v", outcome)
	}
	if seq != firstSeq {
		t.Fatalf("want same sequence %d on retry, got %d", firstSeq, seq)
	}

	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE id = $1`, evt1ID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("want exactly 1 row, got %d", count)
	}
}

func TestInsertDuplicateWithReorderedKeysIsAlreadyPresent(t *testing.T) {
	s := openTestStore(t)
	at := time.Now().UTC()

	if _, _, err := s.Insert(mustEventAt(t, evt1ID, at, `{"a":1,"b":2}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	outcome, _, err := s.Insert(mustEventAt(t, evt1ID, at, `{ "b" : 2 , "a" : 1 }`))
	if err != nil {
		t.Fatalf("Insert retry: %v", err)
	}
	if outcome != AlreadyPresent {
		t.Fatalf("want AlreadyPresent for reordered/whitespace-differing duplicate, got %v", outcome)
	}
}

func TestInsertChangedContentIsConflict(t *testing.T) {
	s := openTestStore(t)
	at := time.Now().UTC()

	if _, _, err := s.Insert(mustEventAt(t, evt1ID, at, `{"a":1}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	outcome, _, err := s.Insert(mustEventAt(t, evt1ID, at, `{"a":2}`))
	if err != nil {
		t.Fatalf("Insert retry: %v", err)
	}
	if outcome != Conflict {
		t.Fatalf("want Conflict, got %v", outcome)
	}

	// Original content must be untouched.
	var content string
	if err := s.db.QueryRow(`SELECT content::text FROM events WHERE id = $1`, evt1ID).Scan(&content); err != nil {
		t.Fatalf("select content: %v", err)
	}
	if content != `{"a": 1}` {
		t.Fatalf("want original content preserved, got %s", content)
	}
}

func TestInsertChangedOccurredAtIsConflict(t *testing.T) {
	s := openTestStore(t)

	first := mustEventAt(t, evt1ID, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), `{"a":1}`)
	if _, _, err := s.Insert(first); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	second := mustEventAt(t, evt1ID, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), `{"a":1}`)
	outcome, _, err := s.Insert(second)
	if err != nil {
		t.Fatalf("Insert retry: %v", err)
	}
	if outcome != Conflict {
		t.Fatalf("want Conflict for changed occurred_at, got %v", outcome)
	}
}

func TestInsertNanosecondOccurredAtRoundTripsAsAlreadyPresent(t *testing.T) {
	// PostgreSQL TIMESTAMPTZ has microsecond resolution; a retry of the
	// exact same event submitted with full nanosecond precision must not
	// be misreported as a conflict due to that truncation.
	s := openTestStore(t)
	at := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	ev := mustEventAt(t, evt1ID, at, `{"a":1}`)

	if _, _, err := s.Insert(ev); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	outcome, _, err := s.Insert(ev)
	if err != nil {
		t.Fatalf("Insert retry: %v", err)
	}
	if outcome != AlreadyPresent {
		t.Fatalf("want AlreadyPresent despite nanosecond truncation, got %v", outcome)
	}
}

func TestInsertConcurrentSameIDIsRaceFree(t *testing.T) {
	// Insert used to SELECT for an existing row, then INSERT if none was
	// found: two concurrent callers for the same new ID could both pass
	// the SELECT before either committed, and the loser would hit a raw
	// unique-violation error instead of a clean Outcome. INSERT ... ON
	// CONFLICT DO NOTHING makes that race atomic; this exercises it with
	// real concurrent transactions against PostgreSQL.
	s := openTestStore(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)

	const n = 8
	outcomes := make([]Outcome, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			outcomes[i], _, errs[i] = s.Insert(ev)
		}(i)
	}
	wg.Wait()

	accepted, present := 0, 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: Insert: %v", i, err)
		}
		switch outcomes[i] {
		case Accepted:
			accepted++
		case AlreadyPresent:
			present++
		default:
			t.Fatalf("goroutine %d: want Accepted or AlreadyPresent, got %v", i, outcomes[i])
		}
	}
	if accepted != 1 {
		t.Fatalf("want exactly 1 Accepted among concurrent inserts, got %d", accepted)
	}
	if present != n-1 {
		t.Fatalf("want %d AlreadyPresent among concurrent inserts, got %d", n-1, present)
	}

	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE id = $1`, evt1ID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("want exactly 1 row, got %d", count)
	}
}

func TestInsertRejectsInvalidEvent(t *testing.T) {
	s := openTestStore(t)

	invalid := event.Event{ID: evt1ID} // missing event_type/actor/content/refs
	outcome, _, err := s.Insert(invalid)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if outcome != Rejected {
		t.Fatalf("want Rejected, got %v", outcome)
	}

	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("rejected event must not reach canonical storage, got %d rows", count)
	}
}

func TestPullOrdersBySequence(t *testing.T) {
	s := openTestStore(t)

	for _, id := range []string{evt1ID, evt2ID, evt3ID} {
		if _, _, err := s.Insert(mustEvent(t, id, `{"a":1}`)); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
	}

	got, err := s.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Sequence <= got[i-1].Sequence {
			t.Fatalf("want ascending sequence, got %d then %d", got[i-1].Sequence, got[i].Sequence)
		}
	}
	wantIDs := []string{evt1ID, evt2ID, evt3ID}
	for i, id := range wantIDs {
		if got[i].Event.ID != id {
			t.Fatalf("row %d: want id %s, got %s", i, id, got[i].Event.ID)
		}
	}
}

func TestPullRespectsAfterCursor(t *testing.T) {
	s := openTestStore(t)

	_, seq1, err := s.Insert(mustEvent(t, evt1ID, `{"a":1}`))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, _, err := s.Insert(mustEvent(t, evt2ID, `{"a":1}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := s.Pull(seq1, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 row after cursor, got %d", len(got))
	}
	if got[0].Event.ID != evt2ID {
		t.Fatalf("want %s, got %s", evt2ID, got[0].Event.ID)
	}
}

func TestPullRespectsLimit(t *testing.T) {
	s := openTestStore(t)

	for _, id := range []string{evt1ID, evt2ID, evt3ID} {
		if _, _, err := s.Insert(mustEvent(t, id, `{"a":1}`)); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
	}

	got, err := s.Pull(0, 2)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 rows bounded by limit, got %d", len(got))
	}
}

func TestSchemaLinksEventsToBlobsByDigest(t *testing.T) {
	if !strings.Contains(schema, "blob_sha256") {
		t.Error("schema.sql missing events.blob_sha256")
	}
}

func TestInsertWithBlobAcceptsEmptyContent(t *testing.T) {
	s := openTestStore(t)
	emptyDigest := blobDigest(nil)
	outcome, _, err := s.InsertWithBlob(mustEvent(t, evt1ID, `{"a":1}`), nil, emptyDigest, "application/octet-stream")
	if err != nil {
		t.Fatalf("InsertWithBlob: %v", err)
	}
	if outcome != Accepted {
		t.Fatalf("want Accepted, got %v", outcome)
	}
	var size int64
	if err := s.db.QueryRow(`SELECT size_bytes FROM blobs WHERE sha256 = decode($1, 'hex')`, emptyDigest).Scan(&size); err != nil {
		t.Fatalf("select empty blob: %v", err)
	}
	if size != 0 {
		t.Fatalf("want empty blob size 0, got %d", size)
	}
}
func TestInsertWithBlobDigestMismatchIsRejected(t *testing.T) {
	s := &Store{}
	outcome, _, err := s.InsertWithBlob(mustEvent(t, evt1ID, `{"a":1}`), []byte("hello blob"), "0000000000000000000000000000000000000000000000000000000000000000", "text/plain")
	if outcome != Rejected {
		t.Fatalf("want Rejected, got %v", outcome)
	}
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("want a *ValidationError, got %T: %v", err, err)
	}
}

func blobDigest(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func TestInsertWithBlobAcceptsFreshEventAndBlob(t *testing.T) {
	s := openTestStore(t)
	content := []byte("hello blob")
	digest := blobDigest(content)

	outcome, seq, err := s.InsertWithBlob(mustEvent(t, evt1ID, `{"a":1}`), content, digest, "text/plain")
	if err != nil {
		t.Fatalf("InsertWithBlob: %v", err)
	}
	if outcome != Accepted {
		t.Fatalf("want Accepted, got %v", outcome)
	}
	if seq <= 0 {
		t.Fatalf("want positive sequence, got %d", seq)
	}

	var gotBlobDigest []byte
	if err := s.db.QueryRow(`SELECT blob_sha256 FROM events WHERE id = $1`, evt1ID).Scan(&gotBlobDigest); err != nil {
		t.Fatalf("select blob_sha256: %v", err)
	}
	if hex.EncodeToString(gotBlobDigest) != digest {
		t.Fatalf("want event.blob_sha256 %s, got %x", digest, gotBlobDigest)
	}

	var blobCount int
	var blobContent []byte
	var mediaType string
	if err := s.db.QueryRow(`SELECT count(*) FROM blobs WHERE sha256 = $1`, gotBlobDigest).Scan(&blobCount); err != nil {
		t.Fatalf("count blobs: %v", err)
	}
	if blobCount != 1 {
		t.Fatalf("want exactly 1 blob row, got %d", blobCount)
	}
	if err := s.db.QueryRow(`SELECT content, media_type FROM blobs WHERE sha256 = $1`, gotBlobDigest).Scan(&blobContent, &mediaType); err != nil {
		t.Fatalf("select blob: %v", err)
	}
	if string(blobContent) != string(content) {
		t.Fatalf("want blob content %q, got %q", content, blobContent)
	}
	if mediaType != "text/plain" {
		t.Fatalf("want media_type text/plain, got %q", mediaType)
	}
}

func TestPullIncludesBlobProvenanceAndReadBlob(t *testing.T) {
	s := openTestStore(t)
	content := []byte("projectable body")
	digest := blobDigest(content)

	if outcome, _, err := s.InsertWithBlob(mustEvent(t, evt1ID, `{"a":1}`), content, digest, "text/plain"); err != nil || outcome != Accepted {
		t.Fatalf("InsertWithBlob: outcome=%v err=%v", outcome, err)
	}
	rows, err := s.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(rows) != 1 || rows[0].BlobSHA256 != digest {
		t.Fatalf("pulled blob provenance = %#v, want %s", rows, digest)
	}

	blob, ok, err := s.ReadBlob(digest)
	if err != nil {
		t.Fatalf("ReadBlob: %v", err)
	}
	if !ok {
		t.Fatal("want canonical blob")
	}
	if string(blob.Content) != string(content) || blob.MediaType != "text/plain" || blob.Size != int64(len(content)) {
		t.Fatalf("blob = %#v, want original bytes and metadata", blob)
	}
}

func TestReadLatestDocumentReturnsNewestExactBody(t *testing.T) {
	s := openTestStore(t)
	source := "example/test/diary"
	for _, doc := range []struct {
		id, body string
	}{
		{evt1ID, "older"},
		{evt2ID, "newer"},
	} {
		content, err := json.Marshal(map[string]string{"source": source})
		if err != nil {
			t.Fatal(err)
		}
		ev, err := event.New(doc.id, "document.filed", "tester", time.Now().UTC(), content, nil)
		if err != nil {
			t.Fatal(err)
		}
		if outcome, _, err := s.InsertWithBlob(ev, []byte(doc.body), blobDigest([]byte(doc.body)), "text/plain"); err != nil || outcome != Accepted {
			t.Fatalf("InsertWithBlob(%q): outcome=%v err=%v", doc.body, outcome, err)
		}
	}

	doc, ok, err := s.ReadLatestDocument(source, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("want latest document")
	}
	if doc.EventID != evt2ID || string(doc.Body) != "newer" || doc.Source != source {
		t.Fatalf("document = %#v, want newest exact source body", doc)
	}
	if _, _, err := s.ReadLatestDocument(source, 4); err == nil {
		t.Fatal("expected body larger than max bytes to fail")
	}
}

func TestListLatestDocumentsReturnsCurrentSourcesAfterExclusiveCursor(t *testing.T) {
	s := openTestStore(t)
	for _, doc := range []struct {
		id, source, body string
	}{
		{evt1ID, "example/drawers/alpha", "alpha-old"},
		{evt2ID, "example/drawers/beta", "beta"},
		{evt3ID, "example/drawers/alpha", "alpha-new"},
	} {
		content, err := json.Marshal(map[string]string{"source": doc.source})
		if err != nil {
			t.Fatal(err)
		}
		ev, err := event.New(doc.id, "document.filed", "tester", time.Now().UTC(), content, nil)
		if err != nil {
			t.Fatal(err)
		}
		body := []byte(doc.body)
		if outcome, _, err := s.InsertWithBlob(ev, body, blobDigest(body), "text/plain"); err != nil || outcome != Accepted {
			t.Fatalf("InsertWithBlob(%q): outcome=%v err=%v", doc.source, outcome, err)
		}
	}

	docs, err := s.ListLatestDocuments(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(ListLatestDocuments) = %d, want 2", len(docs))
	}
	if docs[0].Source != "example/drawers/beta" || docs[0].Sequence != 2 || docs[1].Source != "example/drawers/alpha" || docs[1].Sequence != 3 {
		t.Fatalf("ListLatestDocuments = %#v, want beta@2 then current alpha@3", docs)
	}
	for _, doc := range docs {
		if doc.EventID == "" || doc.BlobSHA256 == "" || doc.MediaType != "text/plain" || len(doc.Body) != 0 {
			t.Fatalf("listed document = %#v, want provenance-only metadata", doc)
		}
	}

	afterBeta, err := s.ListLatestDocuments(docs[0].Sequence, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterBeta) != 1 || afterBeta[0].Source != "example/drawers/alpha" || afterBeta[0].Sequence != 3 {
		t.Fatalf("post-cursor list = %#v, want alpha@3", afterBeta)
	}
}

func TestValidateDocumentListRequestRejectsInvalidCursorOrLimit(t *testing.T) {
	for _, request := range []struct {
		after int64
		limit int
	}{
		{-1, 1},
		{0, 0},
		{0, 501},
	} {
		if err := validateDocumentListRequest(request.after, request.limit); err == nil {
			t.Fatalf("validateDocumentListRequest(%d, %d) succeeded, want invalid request error", request.after, request.limit)
		}
	}
}

func TestInsertWithBlobRetryAfterPartialUploadIsAlreadyPresent(t *testing.T) {
	// Simulates a crash between the canonical store accepting an
	// event+blob and the client recording that acceptance locally: the
	// retry must be idempotent and must not duplicate the blob.
	s := openTestStore(t)
	content := []byte("hello blob")
	digest := blobDigest(content)
	ev := mustEvent(t, evt1ID, `{"a":1}`)

	_, firstSeq, err := s.InsertWithBlob(ev, content, digest, "text/plain")
	if err != nil {
		t.Fatalf("InsertWithBlob: %v", err)
	}

	outcome, seq, err := s.InsertWithBlob(ev, content, digest, "text/plain")
	if err != nil {
		t.Fatalf("InsertWithBlob retry: %v", err)
	}
	if outcome != AlreadyPresent {
		t.Fatalf("want AlreadyPresent, got %v", outcome)
	}
	if seq != firstSeq {
		t.Fatalf("want same sequence %d on retry, got %d", firstSeq, seq)
	}

	var blobCount int
	if err := s.db.QueryRow(`SELECT count(*) FROM blobs`).Scan(&blobCount); err != nil {
		t.Fatalf("count blobs: %v", err)
	}
	if blobCount != 1 {
		t.Fatalf("want exactly 1 blob row after retry, got %d", blobCount)
	}
}

func TestInsertWithBlobDuplicateContentAcrossEventsIsDeduped(t *testing.T) {
	s := openTestStore(t)
	content := []byte("shared blob bytes")
	digest := blobDigest(content)

	if _, _, err := s.InsertWithBlob(mustEvent(t, evt1ID, `{"a":1}`), content, digest, "text/plain"); err != nil {
		t.Fatalf("InsertWithBlob 1: %v", err)
	}
	if _, _, err := s.InsertWithBlob(mustEvent(t, evt2ID, `{"a":2}`), content, digest, "text/plain"); err != nil {
		t.Fatalf("InsertWithBlob 2: %v", err)
	}

	var blobCount int
	if err := s.db.QueryRow(`SELECT count(*) FROM blobs`).Scan(&blobCount); err != nil {
		t.Fatalf("count blobs: %v", err)
	}
	if blobCount != 1 {
		t.Fatalf("want exactly 1 deduped blob row, got %d", blobCount)
	}
}

func TestInsertWithBlobChangedContentIsConflict(t *testing.T) {
	s := openTestStore(t)
	at := time.Now().UTC()
	ev := mustEventAt(t, evt1ID, at, `{"a":1}`)
	first := []byte("original blob")

	if _, _, err := s.InsertWithBlob(ev, first, blobDigest(first), "text/plain"); err != nil {
		t.Fatalf("InsertWithBlob: %v", err)
	}

	second := []byte("different blob")
	outcome, _, err := s.InsertWithBlob(ev, second, blobDigest(second), "text/plain")
	if err != nil {
		t.Fatalf("InsertWithBlob retry: %v", err)
	}
	if outcome != Conflict {
		t.Fatalf("want Conflict, got %v", outcome)
	}

	var gotBlobDigest []byte
	if err := s.db.QueryRow(`SELECT blob_sha256 FROM events WHERE id = $1`, evt1ID).Scan(&gotBlobDigest); err != nil {
		t.Fatalf("select blob_sha256: %v", err)
	}
	if hex.EncodeToString(gotBlobDigest) != blobDigest(first) {
		t.Fatal("original event's blob link must be untouched")
	}
}

func TestInsertWithBlobChangedMediaTypeIsConflict(t *testing.T) {
	s := openTestStore(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	content := []byte("same blob")
	digest := blobDigest(content)

	if outcome, _, err := s.InsertWithBlob(ev, content, digest, "text/plain"); err != nil || outcome != Accepted {
		t.Fatalf("first InsertWithBlob: outcome=%v err=%v", outcome, err)
	}
	outcome, _, err := s.InsertWithBlob(ev, content, digest, "text/markdown")
	if err != nil {
		t.Fatalf("retry InsertWithBlob: %v", err)
	}
	if outcome != Conflict {
		t.Fatalf("want Conflict for changed media type, got %v", outcome)
	}
}

func TestDocumentsKeepMediaTypePerEventWhenBytesAreDeduped(t *testing.T) {
	s := openTestStore(t)
	body := []byte("same blob")
	digest := blobDigest(body)
	for _, doc := range []struct {
		id, source, mediaType string
	}{
		{evt1ID, "example/drawers/plain", "text/plain"},
		{evt2ID, "example/drawers/markdown", "text/markdown"},
	} {
		content, err := json.Marshal(map[string]string{"source": doc.source})
		if err != nil {
			t.Fatal(err)
		}
		ev, err := event.New(doc.id, "document.filed", "tester", time.Now().UTC(), content, nil)
		if err != nil {
			t.Fatal(err)
		}
		if outcome, _, err := s.InsertWithBlob(ev, body, digest, doc.mediaType); err != nil || outcome != Accepted {
			t.Fatalf("InsertWithBlob(%q): outcome=%v err=%v", doc.source, outcome, err)
		}
	}

	for _, want := range []struct{ source, mediaType string }{
		{"example/drawers/plain", "text/plain"},
		{"example/drawers/markdown", "text/markdown"},
	} {
		doc, ok, err := s.ReadLatestDocument(want.source, 1024)
		if err != nil || !ok || doc.MediaType != want.mediaType || string(doc.Body) != string(body) {
			t.Fatalf("ReadLatestDocument(%q) = %#v, ok=%v, err=%v", want.source, doc, ok, err)
		}
	}

	var blobCount int
	if err := s.db.QueryRow(`SELECT count(*) FROM blobs`).Scan(&blobCount); err != nil {
		t.Fatal(err)
	}
	if blobCount != 1 {
		t.Fatalf("blob count = %d, want 1", blobCount)
	}
}

func TestPullAllowsSequenceGaps(t *testing.T) {
	s := openTestStore(t)

	_, seq1, err := s.Insert(mustEvent(t, evt1ID, `{"a":1}`))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, _, err := s.Insert(mustEvent(t, evt2ID, `{"a":1}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	_, seq3, err := s.Insert(mustEvent(t, evt3ID, `{"a":1}`))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Simulate a gap (e.g. from a deleted or externally rolled-back row)
	// by removing the middle event directly.
	if _, err := s.db.Exec(`DELETE FROM events WHERE id = $1`, evt2ID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	got, err := s.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 rows around gap, got %d", len(got))
	}
	if got[0].Sequence != seq1 || got[1].Sequence != seq3 {
		t.Fatalf("want sequences [%d, %d], got [%d, %d]", seq1, seq3, got[0].Sequence, got[1].Sequence)
	}
}

// TestOpenAcceptsConfiguredTransport asserts the store accepts the transport
// selected by its deployment. Private Compose and loopback deployments may use
// plaintext PostgreSQL with a restricted authenticated role.
func TestOpenAcceptsConfiguredTransport(t *testing.T) {
	dsn := os.Getenv("DURO_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("DURO_POSTGRES_TEST_DSN not set; skipping PostgreSQL integration test")
	}

	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open rejected configured transport: %v", err)
	}
	t.Cleanup(func() { store.Close() })
}
