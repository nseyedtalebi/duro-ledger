package local

import (
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nseyedtalebi/duro-ledger/pkg/event"
)

// evt1ID is a well-formed UUID fixture: event.Validate now rejects
// non-UUID provided IDs since canonical events.id is UUID.
const evt1ID = "22222222-2222-2222-2222-222222222222"

func mustEvent(t *testing.T, id, content string) event.Event {
	t.Helper()
	ev, err := event.New(id, "test.type", "tester", time.Now().UTC(), json.RawMessage(content), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	return ev
}

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "local.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestEnqueueAndPending(t *testing.T) {
	s := open(t)

	fresh, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !fresh {
		t.Fatal("expected fresh enqueue")
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want 1 pending, got %d", len(pending))
	}
	if pending[0].Event.ID != evt1ID {
		t.Fatalf("want id evt-1, got %s", pending[0].Event.ID)
	}
	if string(pending[0].Event.Content) != `{"a":1}` {
		t.Fatalf("want content roundtrip, got %s", pending[0].Event.Content)
	}
}

func TestEnqueueRoundTripsResourceURIAndChangedURIConflicts(t *testing.T) {
	s := open(t)
	at := time.Date(2026, 9, 12, 20, 0, 0, 0, time.UTC)
	first, err := event.New(evt1ID, "document.filed", "tester", at, json.RawMessage(`{"source":"cura/drawers/example"}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	first.ResourceURI = "cura://palace/wing/room/example"
	if err := first.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(first); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pending, err := s.Pending()
	if err != nil || len(pending) != 1 || pending[0].Event.ResourceURI != first.ResourceURI {
		t.Fatalf("pending resource URI = %#v, err=%v", pending, err)
	}

	changed := first
	changed.ResourceURI = "cura://palace/wing/room/correction"
	if _, err := s.Enqueue(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed resource URI error = %v, want ErrConflict", err)
	}
}

func TestEnqueueResourceURISurvivesCloseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "local.sqlite")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := event.NewWithResourceURI(evt1ID, "document.filed", "tester", time.Date(2026, 9, 12, 20, 0, 0, 0, time.UTC), json.RawMessage(`{"source":"cura/drawers/example"}`), nil, "cura://palace/wing/room/example")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(ev); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	pending, err := s.Pending()
	if err != nil || len(pending) != 1 || pending[0].Event.ResourceURI != ev.ResourceURI {
		t.Fatalf("reopened pending = %#v, err=%v", pending, err)
	}
}

func TestOpenMigratesExistingQueueWithEmptyResourceURI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE local_events (
		id TEXT PRIMARY KEY, occurred_at TEXT NOT NULL, event_type TEXT NOT NULL, actor TEXT NOT NULL,
		content TEXT NOT NULL, refs TEXT NOT NULL, local_created INTEGER NOT NULL,
		remote_sequence INTEGER, synced_at TEXT, last_error TEXT
	)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO local_events(id, occurred_at, event_type, actor, content, refs, local_created)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, evt1ID, "2026-09-12T20:00:00Z", "document.filed", "legacy", `{"source":"legacy"}`, `{}`, 1); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open legacy queue: %v", err)
	}
	defer s.Close()
	pending, err := s.Pending()
	if err != nil || len(pending) != 1 || pending[0].Event.ResourceURI != "" {
		t.Fatalf("legacy pending event = %#v, err=%v", pending, err)
	}
	var resourceURI string
	if err := s.db.QueryRow(`SELECT resource_uri FROM local_events WHERE id = ?`, evt1ID).Scan(&resourceURI); err != nil || resourceURI != "" {
		t.Fatalf("resource URI migration = %q, err=%v", resourceURI, err)
	}
}

func TestEnqueueDuplicateSameContentIsNoop(t *testing.T) {
	s := open(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)

	if _, err := s.Enqueue(ev); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	fresh, err := s.Enqueue(ev)
	if err != nil {
		t.Fatalf("expected no error retrying identical event, got %v", err)
	}
	if fresh {
		t.Fatal("expected fresh=false on duplicate retry")
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want 1 pending after duplicate retry, got %d", len(pending))
	}
}

func TestEnqueueConflictOnChangedContent(t *testing.T) {
	s := open(t)

	if _, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	_, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":2}`))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestEnqueueConflictOnChangedOccurredAt(t *testing.T) {
	s := open(t)

	first, err := event.New(evt1ID, "test.type", "tester", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), json.RawMessage(`{"a":1}`), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	if _, err := s.Enqueue(first); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	second, err := event.New(evt1ID, "test.type", "tester", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), json.RawMessage(`{"a":1}`), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	if _, err := s.Enqueue(second); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict for changed occurred_at, got %v", err)
	}
}

func TestMarkAcceptedRemovesFromPending(t *testing.T) {
	s := open(t)
	if _, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := s.MarkAccepted(evt1ID, 42); err != nil {
		t.Fatalf("MarkAccepted: %v", err)
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want 0 pending after accept, got %d", len(pending))
	}
}

func TestMarkAcceptedReclaimsStagedBlob(t *testing.T) {
	s := open(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	if _, err := s.EnqueueWithBlob(ev, []byte("large artifact"), "application/octet-stream"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}

	if err := s.MarkAccepted(ev.ID, 42); err != nil {
		t.Fatalf("MarkAccepted: %v", err)
	}

	if _, ok, err := s.PendingBlob(ev.ID); err != nil || ok {
		t.Fatalf("PendingBlob after acceptance = ok:%v err:%v, want absent", ok, err)
	}
}

func TestEnqueueWithBlobRefKeepsOnlyMetadataLocally(t *testing.T) {
	s := open(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	digest := "cb1ad2119d8fafb69566510ee712661f45c9d44a9b8f6a43cbed3bd0ecb27f3c"
	if _, err := s.EnqueueWithBlobRef(ev, digest, 14, "application/octet-stream"); err != nil {
		t.Fatalf("EnqueueWithBlobRef: %v", err)
	}

	blob, ok, err := s.PendingBlob(ev.ID)
	if err != nil || !ok {
		t.Fatalf("PendingBlob = ok:%v err:%v, want reference", ok, err)
	}
	if !blob.External || blob.SHA256 != digest || blob.Size != 14 || len(blob.Content) != 0 {
		t.Fatalf("PendingBlob = %+v, want external metadata-only reference", blob)
	}
}

func TestMarkAcceptedUnknownID(t *testing.T) {
	s := open(t)
	if err := s.MarkAccepted("nope", 1); err == nil {
		t.Fatal("expected error for unknown id")
	}
}

func TestRemoteSequenceReturnsOnlyRecordedSequence(t *testing.T) {
	s := open(t)
	if _, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, ok, err := s.RemoteSequence(evt1ID); err != nil || ok {
		t.Fatalf("pending remote sequence = ok:%v err:%v, want absent", ok, err)
	}
	if err := s.MarkAccepted(evt1ID, 42); err != nil {
		t.Fatalf("MarkAccepted: %v", err)
	}
	sequence, ok, err := s.RemoteSequence(evt1ID)
	if err != nil || !ok || sequence != 42 {
		t.Fatalf("remote sequence = %d, ok:%v err:%v, want 42", sequence, ok, err)
	}
}

func TestMarkRejectedRetainsInPending(t *testing.T) {
	s := open(t)
	if _, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := s.MarkRejected(evt1ID, "validation failed"); err != nil {
		t.Fatalf("MarkRejected: %v", err)
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want 1 pending after reject, got %d", len(pending))
	}
	if pending[0].LastError != "validation failed" {
		t.Fatalf("want last_error retained, got %q", pending[0].LastError)
	}
}

func TestPersistenceAcrossCloseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "local.sqlite")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	pending, err := s2.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want 1 pending after reopen, got %d", len(pending))
	}
}

// mustEventAt is mustEvent with an explicit occurredAt, so two contents can
// be compared for the same event ID without occurredAt drift (time.Now()
// called twice) masking the comparison under test as an unrelated conflict.
func mustEventAt(t *testing.T, id string, occurredAt time.Time, content string) event.Event {
	t.Helper()
	ev, err := event.New(id, "test.type", "tester", occurredAt, json.RawMessage(content), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	return ev
}

func TestEnqueueDuplicateWithReorderedKeysAndWhitespaceIsNoop(t *testing.T) {
	s := open(t)
	at := time.Now().UTC()
	if _, err := s.Enqueue(mustEventAt(t, evt1ID, at, `{"a":1,"b":2}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	fresh, err := s.Enqueue(mustEventAt(t, evt1ID, at, `{ "b" : 2 ,   "a" : 1 }`))
	if err != nil {
		t.Fatalf("expected no error for reordered/whitespace-differing duplicate, got %v", err)
	}
	if fresh {
		t.Fatal("expected fresh=false for semantically identical duplicate")
	}
}

func TestEnqueueConflictOnDifferentArrayOrder(t *testing.T) {
	s := open(t)
	at := time.Now().UTC()
	if _, err := s.Enqueue(mustEventAt(t, evt1ID, at, `{"a":[1,2,3]}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	_, err := s.Enqueue(mustEventAt(t, evt1ID, at, `{"a":[3,2,1]}`))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict for reordered array, got %v", err)
	}
}

func TestEnqueueConflictOnLargeIntegerDistinction(t *testing.T) {
	// 9007199254740992 and 9007199254740993 both round to the same
	// float64, so a naive json.Unmarshal-based comparison would treat
	// this differing content as a duplicate.
	s := open(t)
	at := time.Now().UTC()
	if _, err := s.Enqueue(mustEventAt(t, evt1ID, at, `{"n":9007199254740992}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	_, err := s.Enqueue(mustEventAt(t, evt1ID, at, `{"n":9007199254740993}`))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict for distinct large integers, got %v", err)
	}
}

func TestEnqueueErrorsOnCorruptStoredContent(t *testing.T) {
	s := open(t)
	if _, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Simulate a tampered/corrupted row bypassing Store's own writes.
	if _, err := s.db.Exec(`UPDATE local_events SET content = ? WHERE id = ?`, `{"a":`, evt1ID); err != nil {
		t.Fatalf("corrupt row: %v", err)
	}

	fresh, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`))
	if err == nil {
		t.Fatal("expected error for corrupt stored content, got nil")
	}
	if errors.Is(err, ErrConflict) {
		t.Fatal("corrupt stored content should not be silently reported as an ordinary conflict")
	}
	if fresh {
		t.Fatal("expected fresh=false on corrupt stored content")
	}
}

func TestPendingOrdersExactSecondBeforeSubSecond(t *testing.T) {
	s := open(t)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	idExact := "44444444-4444-4444-4444-444444444444"
	idSub := "55555555-5555-5555-5555-555555555555"

	// idSub is enqueued as having happened chronologically after idExact,
	// but idExact's local_created has no sub-second fraction while idSub's
	// does. RFC3339Nano text sorts "." before "Z" ('.' < 'Z'), so a naive
	// lexicographic ORDER BY would put idSub first.
	insertAt(t, s, idExact, base)
	insertAt(t, s, idSub, base.Add(500*time.Millisecond))

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("want 2 pending, got %d", len(pending))
	}
	if pending[0].Event.ID != idExact || pending[1].Event.ID != idSub {
		t.Fatalf("want FIFO order [%s, %s], got [%s, %s]", idExact, idSub, pending[0].Event.ID, pending[1].Event.ID)
	}
}

func TestPendingTiesPreserveInsertionOrder(t *testing.T) {
	s := open(t)
	same := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	idHigh := "88888888-8888-8888-8888-888888888888" // sorts after idLow lexicographically
	idLow := "11111111-1111-1111-1111-111111111111"

	// Both rows share the same local_created, so "id ASC" and "rowid ASC"
	// disagree: id ASC would put idLow first, but idHigh was inserted
	// first and SQLite's rowid preserves that insertion (FIFO) order.
	insertAt(t, s, idHigh, same)
	insertAt(t, s, idLow, same)

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("want 2 pending, got %d", len(pending))
	}
	if pending[0].Event.ID != idHigh || pending[1].Event.ID != idLow {
		t.Fatalf("want insertion order [%s, %s], got [%s, %s]", idHigh, idLow, pending[0].Event.ID, pending[1].Event.ID)
	}
}

// fakeCommitFailDriver is a minimal database/sql driver whose transactions
// always fail Commit. It exists to make an otherwise hard-to-trigger SQLite
// failure mode (a successful write statement followed by a failing commit)
// deterministic and portable for TestEnqueueFreshInsertReturnsErrorOnCommitFailure.
type fakeCommitFailDriver struct{}

func (fakeCommitFailDriver) Open(string) (driver.Conn, error) { return fakeConn{}, nil }

type fakeConn struct{}

func (fakeConn) Prepare(query string) (driver.Stmt, error) { return fakeStmt{}, nil }
func (fakeConn) Close() error                              { return nil }
func (fakeConn) Begin() (driver.Tx, error)                 { return fakeTx{}, nil }

type fakeStmt struct{}

func (fakeStmt) Close() error                                    { return nil }
func (fakeStmt) NumInput() int                                   { return -1 }
func (fakeStmt) Exec(args []driver.Value) (driver.Result, error) { return driver.RowsAffected(1), nil }
func (fakeStmt) Query(args []driver.Value) (driver.Rows, error)  { return fakeRows{}, nil }

// fakeRows always reports zero rows, so QueryRow surfaces sql.ErrNoRows and
// Enqueue takes its fresh-insert path.
type fakeRows struct{}

func (fakeRows) Columns() []string              { return nil }
func (fakeRows) Close() error                   { return nil }
func (fakeRows) Next(dest []driver.Value) error { return io.EOF }

type fakeTx struct{}

func (fakeTx) Commit() error   { return errors.New("simulated commit failure") }
func (fakeTx) Rollback() error { return nil }

var registerFakeCommitFailDriver = sync.OnceFunc(func() {
	sql.Register("commitfail", fakeCommitFailDriver{})
})

func TestEnqueueFreshInsertReturnsErrorOnCommitFailure(t *testing.T) {
	registerFakeCommitFailDriver()
	db, err := sql.Open("commitfail", "ignored")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	s := &Store{db: db}

	fresh, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`))
	if err == nil {
		t.Fatal("expected error when commit fails")
	}
	if fresh {
		t.Fatal("expected fresh=false when commit fails")
	}
}

func TestMarkRejectedDoesNotTouchAcceptedRow(t *testing.T) {
	s := open(t)
	if _, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := s.MarkAccepted(evt1ID, 1); err != nil {
		t.Fatalf("MarkAccepted: %v", err)
	}

	if err := s.MarkRejected(evt1ID, "late rejection"); err == nil {
		t.Fatal("expected error marking an already-accepted event rejected")
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("accepted event must stay out of Pending, got %d", len(pending))
	}

	var lastError sql.NullString
	if err := s.db.QueryRow(`SELECT last_error FROM local_events WHERE id = ?`, evt1ID).Scan(&lastError); err != nil {
		t.Fatalf("query last_error: %v", err)
	}
	if lastError.Valid {
		t.Fatalf("want last_error untouched on accepted row, got %q", lastError.String)
	}
}

func insertAt(t *testing.T, s *Store, id string, created time.Time) {
	t.Helper()
	ev := mustEvent(t, id, `{"a":1}`)
	_, err := s.db.Exec(
		`INSERT INTO local_events(id, occurred_at, event_type, actor, content, refs, local_created)
		 VALUES (?,?,?,?,?,?,?)`,
		ev.ID, ev.OccurredAt.UTC().Format(timeLayout), ev.EventType, ev.Actor,
		string(ev.Content), string(ev.Refs), encodeLocalCreated(created),
	)
	if err != nil {
		t.Fatalf("insertAt: %v", err)
	}
}

func TestApplyRemoteBatchInsertsAndAdvancesCursor(t *testing.T) {
	s := open(t)
	ev1 := mustEvent(t, evt1ID, `{"a":1}`)
	idB := "66666666-6666-6666-6666-666666666666"
	ev2 := mustEvent(t, idB, `{"a":2}`)

	err := s.ApplyRemoteBatch([]RemoteEvent{{Event: ev1, Sequence: 1}, {Event: ev2, Sequence: 2}}, 2)
	if err != nil {
		t.Fatalf("ApplyRemoteBatch: %v", err)
	}

	seq, err := s.Cursor()
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if seq != 2 {
		t.Fatalf("want cursor 2, got %d", seq)
	}

	// Applied rows are already synchronized, so they must not appear as
	// pending outbound work.
	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want 0 pending after apply, got %d", len(pending))
	}
}

func TestApplyRemoteBatchDoesNotOverwritePendingContent(t *testing.T) {
	// A row this node originally created and pushed itself comes back on
	// pull with the same ID. ApplyRemoteBatch must not touch its stored
	// content, only backfill sync bookkeeping if still pending.
	s := open(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	if _, err := s.Enqueue(ev); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := s.ApplyRemoteBatch([]RemoteEvent{{Event: ev, Sequence: 5}}, 5); err != nil {
		t.Fatalf("ApplyRemoteBatch: %v", err)
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want row marked synced by apply, got %d still pending", len(pending))
	}

	var content string
	if err := s.db.QueryRow(`SELECT content FROM local_events WHERE id = ?`, evt1ID).Scan(&content); err != nil {
		t.Fatalf("query: %v", err)
	}
	if content != `{"a":1}` {
		t.Fatalf("content must be untouched, got %s", content)
	}
}

func TestApplyRemoteBatchCanonicalizesActorForSelfPushedEvent(t *testing.T) {
	s := open(t)
	local := mustEvent(t, evt1ID, `{"a":1}`)
	if _, err := s.Enqueue(local); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	remote := local
	remote.Actor = "canonical_role"

	if err := s.ApplyRemoteBatch([]RemoteEvent{{Event: remote, Sequence: 5}}, 5); err != nil {
		t.Fatalf("ApplyRemoteBatch: %v", err)
	}
	var actor string
	if err := s.db.QueryRow(`SELECT actor FROM local_events WHERE id = ?`, evt1ID).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != remote.Actor {
		t.Fatalf("actor = %q, want canonical %q", actor, remote.Actor)
	}
}

func TestApplyRemoteBatchRejectsCollisionAndRollsBackBatch(t *testing.T) {
	// A local pending row and an incoming canonical event share an ID but
	// disagree on content: ApplyRemoteBatch must not silently mark the
	// local row synced (which would bless mismatched content as if it
	// matched canonical), and must roll back the whole batch and cursor.
	s := open(t)
	if err := s.SetCursor(1); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	at := time.Now().UTC()
	local := mustEventAt(t, evt1ID, at, `{"a":1}`)
	if _, err := s.Enqueue(local); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	remote := mustEventAt(t, evt1ID, at, `{"a":2}`)
	err := s.ApplyRemoteBatch([]RemoteEvent{{Event: remote, Sequence: 5}}, 5)
	if err == nil {
		t.Fatal("expected error for colliding local/remote payload")
	}

	seq, err := s.Cursor()
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if seq != 1 {
		t.Fatalf("want cursor left at 1 after rejected batch, got %d", seq)
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want local row still pending, got %d", len(pending))
	}
	if string(pending[0].Event.Content) != `{"a":1}` {
		t.Fatalf("local content must be untouched, got %s", pending[0].Event.Content)
	}
}

func TestApplyRemoteBatchSelfEventSurvivesTimestampTruncation(t *testing.T) {
	// A node pushes an event with full nanosecond occurred_at precision,
	// then pulls it back after PostgreSQL's TIMESTAMPTZ truncated it to
	// microseconds. ApplyRemoteBatch must recognize this as the same event
	// (backfilling sync bookkeeping) rather than reporting a payload
	// collision, since the two only differ in precision below what
	// canonical storage keeps.
	s := open(t)
	at := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	local := mustEventAt(t, evt1ID, at, `{"a":1}`)
	if _, err := s.Enqueue(local); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	remote := local
	remote.OccurredAt = at.Truncate(time.Microsecond)
	if err := s.ApplyRemoteBatch([]RemoteEvent{{Event: remote, Sequence: 1}}, 1); err != nil {
		t.Fatalf("ApplyRemoteBatch: %v", err)
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want row marked synced despite timestamp truncation, got %d still pending", len(pending))
	}
}

func TestApplyRemoteBatchRollsBackCursorOnInvalidRow(t *testing.T) {
	s := open(t)
	if err := s.SetCursor(1); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	invalid := RemoteEvent{Event: event.Event{ID: evt1ID}, Sequence: 2} // missing required fields
	if err := s.ApplyRemoteBatch([]RemoteEvent{invalid}, 2); err == nil {
		t.Fatal("expected error applying invalid remote event")
	}

	seq, err := s.Cursor()
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if seq != 1 {
		t.Fatalf("want cursor left at 1 after failed batch, got %d", seq)
	}
}

func TestEnqueueWithBlobStoresEventAndBlobTogether(t *testing.T) {
	s := open(t)
	content := []byte("hello blob")

	fresh, err := s.EnqueueWithBlob(mustEvent(t, evt1ID, `{"a":1}`), content, "text/plain")
	if err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}
	if !fresh {
		t.Fatal("expected fresh enqueue")
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want 1 pending event, got %d", len(pending))
	}

	blob, ok, err := s.PendingBlob(evt1ID)
	if err != nil {
		t.Fatalf("PendingBlob: %v", err)
	}
	if !ok {
		t.Fatal("want blob present")
	}
	if string(blob.Content) != string(content) {
		t.Fatalf("want content %q, got %q", content, blob.Content)
	}
	if blob.MediaType != "text/plain" {
		t.Fatalf("want media type text/plain, got %q", blob.MediaType)
	}
	if blob.Size != int64(len(content)) {
		t.Fatalf("want size %d, got %d", len(content), blob.Size)
	}
	sum := sha256.Sum256(content)
	if blob.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("want digest %x, got %s", sum, blob.SHA256)
	}
}

func TestEnqueueWithBlobAcceptsEmptyContent(t *testing.T) {
	s := open(t)
	if fresh, err := s.EnqueueWithBlob(mustEvent(t, evt1ID, `{"a":1}`), nil, "text/plain"); err != nil || !fresh {
		t.Fatalf("want fresh empty blob enqueue, fresh=%v err=%v", fresh, err)
	}
	blob, ok, err := s.PendingBlob(evt1ID)
	if err != nil {
		t.Fatalf("PendingBlob: %v", err)
	}
	if !ok || blob.Size != 0 || len(blob.Content) != 0 {
		t.Fatalf("want empty staged blob, ok=%v size=%d content=%q", ok, blob.Size, blob.Content)
	}
}

func TestEnqueueWithBlobDuplicateRetryIsNoop(t *testing.T) {
	s := open(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	content := []byte("hello blob")

	if _, err := s.EnqueueWithBlob(ev, content, "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}
	fresh, err := s.EnqueueWithBlob(ev, content, "text/plain")
	if err != nil {
		t.Fatalf("EnqueueWithBlob retry: %v", err)
	}
	if fresh {
		t.Fatal("expected fresh=false on duplicate retry")
	}

	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM local_blobs WHERE event_id = ?`, evt1ID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("want exactly 1 blob row, got %d", count)
	}
}

func TestEnqueueWithBlobRetryAfterAcceptanceIsNoop(t *testing.T) {
	s := open(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	content := []byte("large artifact")
	if _, err := s.EnqueueWithBlob(ev, content, "application/octet-stream"); err != nil {
		t.Fatalf("first EnqueueWithBlob: %v", err)
	}
	if err := s.MarkAccepted(ev.ID, 42); err != nil {
		t.Fatalf("MarkAccepted: %v", err)
	}

	fresh, err := s.EnqueueWithBlob(ev, content, "application/octet-stream")
	if err != nil || fresh {
		t.Fatalf("retry after acceptance = fresh:%v err:%v, want no-op", fresh, err)
	}
}

func TestEnqueueWithBlobChangedBytesConflict(t *testing.T) {
	s := open(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	if _, err := s.EnqueueWithBlob(ev, []byte("first"), "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}
	if _, err := s.EnqueueWithBlob(ev, []byte("second"), "text/plain"); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict for changed blob bytes, got %v", err)
	}
}

func TestEnqueueWithBlobChangedMediaTypeConflict(t *testing.T) {
	s := open(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	content := []byte("same blob")
	if _, err := s.EnqueueWithBlob(ev, content, "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}
	if _, err := s.EnqueueWithBlob(ev, content, "text/markdown"); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict for changed blob media type, got %v", err)
	}
}

func TestPendingBlobReturnsFalseWhenNoBlob(t *testing.T) {
	s := open(t)
	if _, err := s.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_, ok, err := s.PendingBlob(evt1ID)
	if err != nil {
		t.Fatalf("PendingBlob: %v", err)
	}
	if ok {
		t.Fatal("want ok=false when no blob was staged")
	}
}

func TestPendingBlobDetectsCorruption(t *testing.T) {
	s := open(t)
	if _, err := s.EnqueueWithBlob(mustEvent(t, evt1ID, `{"a":1}`), []byte("hello blob"), "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}
	// Simulate storage-level corruption bypassing Store's own writes.
	if _, err := s.db.Exec(`UPDATE local_blobs SET content = ? WHERE event_id = ?`, []byte("tampered"), evt1ID); err != nil {
		t.Fatalf("corrupt row: %v", err)
	}

	_, _, err := s.PendingBlob(evt1ID)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("want ErrDigestMismatch, got %v", err)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	s := open(t)

	seq, err := s.Cursor()
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if seq != 0 {
		t.Fatalf("want default cursor 0, got %d", seq)
	}

	if err := s.SetCursor(7); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	seq, err = s.Cursor()
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if seq != 7 {
		t.Fatalf("want cursor 7, got %d", seq)
	}
}
