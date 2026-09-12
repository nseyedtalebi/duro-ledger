package sync

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nseyedtalebi/duro-ledger/pkg/blob"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/local"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

const evt1ID = "22222222-2222-2222-2222-222222222222"
const evt2ID = "33333333-3333-3333-3333-333333333333"
const evt3ID = "44444444-4444-4444-4444-444444444444"
const testAdvisoryLockKey = 918273645

func openLocal(t *testing.T) *local.Store {
	t.Helper()
	s, err := local.Open(filepath.Join(t.TempDir(), "local.sqlite"))
	if err != nil {
		t.Fatalf("local.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// openRemote skips clearly when DURO_POSTGRES_TEST_DSN is unset, and
// otherwise opens a Store against a freshly truncated events table.
func openRemote(t *testing.T) *postgres.Store {
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
	if err := postgres.Initialize(dsn); err != nil {
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
		t.Fatalf("postgres.Initialize: %v", err)
	}
	if _, err := raw.Exec(`TRUNCATE events, blobs RESTART IDENTITY`); err != nil {
		raw.Close()
		t.Fatalf("truncate events: %v", err)
	}

	s, err := postgres.Open(dsn)
	if err != nil {
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
		t.Fatalf("postgres.Open: %v", err)
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

func TestPushAcceptsFreshPendingRows(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)

	if _, err := loc.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := loc.Enqueue(mustEvent(t, evt2ID, `{"a":2}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	res, err := Push(loc, rem, 10)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if res.Accepted != 2 {
		t.Fatalf("want 2 accepted, got %+v", res)
	}

	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want 0 pending after push, got %d", len(pending))
	}

	pulled, err := rem.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pulled) != 2 {
		t.Fatalf("want 2 canonical rows, got %d", len(pulled))
	}
}

func TestPushUploadsStagedBlob(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	content := []byte("canonical body")

	if _, err := loc.EnqueueWithBlob(ev, content, "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}
	if _, err := Push(loc, rem, 10); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// A same-ID retry including its staged blob is idempotent only when Push
	// actually used InsertWithBlob; a plain Insert leaves no blob link and
	// classifies this retry as a conflict.
	outcome, _, err := rem.InsertWithBlob(ev, content, "", "text/plain")
	if err != nil {
		t.Fatalf("InsertWithBlob retry: %v", err)
	}
	if outcome != postgres.AlreadyPresent {
		t.Fatalf("want blob-backed retry to be already_present, got %s", outcome)
	}
}

func TestPushRetryAfterCrashIsIdempotent(t *testing.T) {
	// Simulate a crash between the remote accepting an event and the
	// local queue recording that acceptance: the event already exists
	// canonically, but the local row is still pending.
	loc := openLocal(t)
	rem := openRemote(t)

	ev := mustEvent(t, evt1ID, `{"a":1}`)
	if _, _, err := rem.Insert(ev); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}
	if _, err := loc.Enqueue(ev); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	res, err := Push(loc, rem, 10)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if res.AlreadyPresent != 1 || res.Accepted != 0 {
		t.Fatalf("want 1 already_present, got %+v", res)
	}

	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want row marked synced, got %d pending", len(pending))
	}

	pulled, err := rem.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pulled) != 1 {
		t.Fatalf("want exactly 1 canonical row, no duplicate, got %d", len(pulled))
	}
}

func TestPushConflictRetainsPendingRowWithReason(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)

	at := time.Now().UTC()
	original, err := event.New(evt1ID, "test.type", "tester", at, json.RawMessage(`{"a":1}`), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	if _, _, err := rem.Insert(original); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}

	changed, err := event.New(evt1ID, "test.type", "tester", at, json.RawMessage(`{"a":2}`), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	if _, err := loc.Enqueue(changed); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	res, err := Push(loc, rem, 10)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if res.Conflict != 1 {
		t.Fatalf("want 1 conflict, got %+v", res)
	}

	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("conflicting row must stay pending, got %d", len(pending))
	}
	if pending[0].LastError == "" {
		t.Fatal("want a recorded conflict reason")
	}

	pulled, err := rem.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pulled) != 1 || string(pulled[0].Event.Content) != `{"a": 1}` {
		t.Fatalf("canonical row must be untouched, got %+v", pulled)
	}
}

func TestPushRespectsBoundedLimit(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)

	for _, id := range []string{evt1ID, evt2ID, evt3ID} {
		if _, err := loc.Enqueue(mustEvent(t, id, `{"a":1}`)); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	res, err := Push(loc, rem, 2)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if res.Accepted != 2 {
		t.Fatalf("want 2 accepted (bounded by limit), got %+v", res)
	}

	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want 1 row left pending, got %d", len(pending))
	}
}

func TestPullAppliesEventsAndAdvancesCursor(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)

	var lastSeq int64
	for _, id := range []string{evt1ID, evt2ID, evt3ID} {
		_, seq, err := rem.Insert(mustEvent(t, id, `{"a":1}`))
		if err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
		lastSeq = seq
	}

	n, err := Pull(rem, loc, 2)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if n != 3 {
		t.Fatalf("want 3 events applied, got %d", n)
	}

	cursor, err := loc.Cursor()
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if cursor != lastSeq {
		t.Fatalf("want cursor %d, got %d", lastSeq, cursor)
	}

	// Applied rows are already synchronized, not outbound work.
	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want 0 pending after pull, got %d", len(pending))
	}
}

func TestPullResumesFromExistingCursor(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)

	_, seq1, err := rem.Insert(mustEvent(t, evt1ID, `{"a":1}`))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, _, err := rem.Insert(mustEvent(t, evt2ID, `{"a":1}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Simulate a prior pull that already committed through seq1 (the
	// crash-window boundary: further rows must not be skipped or
	// re-fetched incorrectly from here).
	if err := loc.SetCursor(seq1); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	n, err := Pull(rem, loc, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 new event applied, got %d", n)
	}

	// A second pull with nothing new must be a safe no-op.
	n, err = Pull(rem, loc, 10)
	if err != nil {
		t.Fatalf("second Pull: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 events on empty pull, got %d", n)
	}
}

// failingBlobStore is a package-level fake blob.Store whose Put always
// fails, for exercising PushWithBlobStore's backend-write-failure path
// without needing a real backend to break.
type failingBlobStore struct{}

func (failingBlobStore) Put(digest string, content []byte) error {
	return fmt.Errorf("failingBlobStore: refusing to write %s", digest)
}
func (failingBlobStore) Read(digest string, maxBytes int64) ([]byte, error) {
	return nil, blob.ErrNotFound
}
func (failingBlobStore) Verify(digest string, size int64) error {
	return fmt.Errorf("failingBlobStore: refusing to verify %s", digest)
}
func (failingBlobStore) Kind() string { return "failing" }

func openFilesystemBlobStore(t *testing.T) *blob.FilesystemStore {
	t.Helper()
	s, err := blob.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatalf("blob.NewFilesystemStore: %v", err)
	}
	return s
}

func TestPushWithBlobStoreWritesToBackendAndLinksEvent(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)
	backend := openFilesystemBlobStore(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	content := []byte("filesystem-backed canonical body")

	if _, err := loc.EnqueueWithBlob(ev, content, "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}

	res, err := PushWithBlobStore(loc, rem, backend, 10)
	if err != nil {
		t.Fatalf("PushWithBlobStore: %v", err)
	}
	if res.Accepted != 1 {
		t.Fatalf("want 1 accepted, got %+v", res)
	}

	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want 0 pending after push, got %d", len(pending))
	}

	digest := digestOf(content)
	got, err := backend.Read(digest, 1024)
	if err != nil {
		t.Fatalf("backend.Read: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("backend content = %q, want %q", got, content)
	}

	pulled, err := rem.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pulled) != 1 || pulled[0].BlobSHA256 != digest {
		t.Fatalf("canonical row = %#v, want blob linked by digest %s", pulled, digest)
	}
}

func TestPushWithPrestagedFilesystemBlobLinksAndReclaimsLocalMetadata(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)
	backend := openFilesystemBlobStore(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	source := filepath.Join(t.TempDir(), "run-output.bin")
	content := make([]byte, 2<<20)
	for i := range content {
		content[i] = byte(i)
	}
	if err := os.WriteFile(source, content, 0o600); err != nil {
		t.Fatal(err)
	}
	digest, size, err := backend.PutFile(source)
	if err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	if _, err := loc.EnqueueWithBlobRef(ev, digest, size, "application/octet-stream"); err != nil {
		t.Fatalf("EnqueueWithBlobRef: %v", err)
	}

	res, err := PushWithBlobStore(loc, rem, backend, 10)
	if err != nil {
		t.Fatalf("PushWithBlobStore: %v", err)
	}
	if res.Accepted != 1 {
		t.Fatalf("want 1 accepted, got %+v", res)
	}
	if _, ok, err := loc.PendingBlob(ev.ID); err != nil || ok {
		t.Fatalf("PendingBlob after acceptance = ok:%v err:%v, want reclaimed", ok, err)
	}
	if err := backend.Verify(digest, size); err != nil {
		t.Fatalf("backend.Verify: %v", err)
	}

	pulled, err := rem.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pulled) != 1 || pulled[0].BlobSHA256 != digest {
		t.Fatalf("canonical row = %#v, want filesystem blob link %s", pulled, digest)
	}
}

func digestOf(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func TestPushWithBlobStoreExactRetryIsAlreadyPresent(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)
	backend := openFilesystemBlobStore(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	content := []byte("retried filesystem-backed body")

	if _, err := loc.EnqueueWithBlob(ev, content, "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}
	if _, err := PushWithBlobStore(loc, rem, backend, 10); err != nil {
		t.Fatalf("PushWithBlobStore: %v", err)
	}

	// Simulate a crash between the remote accepting the push and the local
	// queue recording it: the row is still pending, and the exact same
	// blob/event is pushed again.
	digest := digestOf(content)
	outcome, _, err := rem.InsertWithBlobRef(ev, digest, int64(len(content)), "text/plain")
	if err != nil {
		t.Fatalf("InsertWithBlobRef retry: %v", err)
	}
	if outcome != postgres.AlreadyPresent {
		t.Fatalf("want exact retry to be already_present, got %s", outcome)
	}
}

func TestPushWithBlobStoreChangedBodyIsConflict(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)
	backend := openFilesystemBlobStore(t)

	at := time.Now().UTC()
	original, err := event.New(evt1ID, "test.type", "tester", at, json.RawMessage(`{"a":1}`), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	originalContent := []byte("original body")
	if _, _, err := rem.InsertWithBlobRef(original, digestOf(originalContent), int64(len(originalContent)), "text/plain"); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}

	changed, err := event.New(evt1ID, "test.type", "tester", at, json.RawMessage(`{"a":1}`), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	changedContent := []byte("changed body")
	if _, err := loc.EnqueueWithBlob(changed, changedContent, "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}

	res, err := PushWithBlobStore(loc, rem, backend, 10)
	if err != nil {
		t.Fatalf("PushWithBlobStore: %v", err)
	}
	if res.Conflict != 1 {
		t.Fatalf("want 1 conflict, got %+v", res)
	}

	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 || pending[0].LastError == "" {
		t.Fatalf("changed-body row must stay pending with a recorded reason, got %#v", pending)
	}
}

func TestPushWithBlobStoreBackendWriteFailureRetainsQueue(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)
	ev := mustEvent(t, evt1ID, `{"a":1}`)
	content := []byte("never makes it to the backend")

	if _, err := loc.EnqueueWithBlob(ev, content, "text/plain"); err != nil {
		t.Fatalf("EnqueueWithBlob: %v", err)
	}

	_, err := PushWithBlobStore(loc, rem, failingBlobStore{}, 10)
	if err == nil {
		t.Fatal("want error when the backend write fails")
	}

	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want the row retained pending after a backend write failure, got %d", len(pending))
	}

	pulled, err := rem.Pull(0, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pulled) != 0 {
		t.Fatalf("want no canonical row after a failed backend write, got %d", len(pulled))
	}
}

func TestPullDoesNotDuplicateSelfPushedEvent(t *testing.T) {
	loc := openLocal(t)
	rem := openRemote(t)

	if _, err := loc.Enqueue(mustEvent(t, evt1ID, `{"a":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := Push(loc, rem, 10); err != nil {
		t.Fatalf("Push: %v", err)
	}

	if _, err := Pull(rem, loc, 10); err != nil {
		t.Fatalf("Pull: %v", err)
	}

	pending, err := loc.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want 0 pending, got %d", len(pending))
	}
}
