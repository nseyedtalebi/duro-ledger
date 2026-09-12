package integration

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nseyedtalebi/duro-ledger/pkg/blob"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/local"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
	ledgerSync "github.com/nseyedtalebi/duro-ledger/pkg/sync"
)

const acceptanceAdvisoryLockKey = 918273645

type acceptanceStore struct {
	canonical *postgres.Store
	admin     *sql.DB
}

func openAcceptanceStore(t *testing.T) *acceptanceStore {
	t.Helper()
	dsn := os.Getenv("DURO_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("DURO_POSTGRES_TEST_DSN not set")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	admin.SetMaxOpenConns(1)
	if _, err := admin.Exec(`SELECT pg_advisory_lock($1)`, acceptanceAdvisoryLockKey); err != nil {
		admin.Close()
		t.Fatalf("acquire acceptance lock: %v", err)
	}
	if err := postgres.Initialize(dsn); err != nil {
		_, _ = admin.Exec(`SELECT pg_advisory_unlock($1)`, acceptanceAdvisoryLockKey)
		admin.Close()
		t.Fatalf("postgres.Initialize: %v", err)
	}
	canonical, err := postgres.Open(dsn)
	if err != nil {
		_, _ = admin.Exec(`SELECT pg_advisory_unlock($1)`, acceptanceAdvisoryLockKey)
		admin.Close()
		t.Fatalf("postgres.Open: %v", err)
	}
	t.Cleanup(func() {
		canonical.Close()
		_, _ = admin.Exec(`SELECT pg_advisory_unlock($1)`, acceptanceAdvisoryLockKey)
		admin.Close()
	})
	return &acceptanceStore{canonical: canonical, admin: admin}
}

func (s *acceptanceStore) reset(t *testing.T) {
	t.Helper()
	if _, err := s.admin.Exec(`TRUNCATE events, blobs RESTART IDENTITY`); err != nil {
		t.Fatalf("reset canonical store: %v", err)
	}
}

func acceptanceEvent(t *testing.T, id, content string) event.Event {
	t.Helper()
	ev, err := event.New(id, "acceptance.test", "integration", time.Unix(1, 0).UTC(), json.RawMessage(content), nil)
	if err != nil {
		t.Fatalf("event.New: %v", err)
	}
	return ev
}

func TestAcceptanceGate(t *testing.T) {
	s := openAcceptanceStore(t)

	t.Run("1_offline_append_survives_restart", func(t *testing.T) {
		s.reset(t)
		path := t.TempDir() + "/local.sqlite"
		ev := acceptanceEvent(t, uuid.NewString(), `{"case":1}`)
		loc, err := local.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if fresh, err := loc.Enqueue(ev); err != nil || !fresh {
			t.Fatalf("enqueue: fresh=%v err=%v", fresh, err)
		}
		loc.Close()
		loc, err = local.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer loc.Close()
		pending, err := loc.Pending()
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 1 || pending[0].Event.ID != ev.ID {
			t.Fatalf("want one durable pending event, got %+v", pending)
		}
	})

	t.Run("2_upload_creates_one_canonical_event", func(t *testing.T) {
		s.reset(t)
		loc := openLocal(t)
		defer loc.Close()
		if _, err := loc.Enqueue(acceptanceEvent(t, uuid.NewString(), `{"case":2}`)); err != nil {
			t.Fatal(err)
		}
		result, err := ledgerSync.Push(loc, s.canonical, 0)
		if err != nil || result.Accepted != 1 {
			t.Fatalf("push: result=%+v err=%v", result, err)
		}
		rows, err := s.canonical.Pull(0, 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("want one canonical event, rows=%d err=%v", len(rows), err)
		}
	})

	t.Run("3_repeated_upload_is_already_present", func(t *testing.T) {
		s.reset(t)
		ev := acceptanceEvent(t, uuid.NewString(), `{"case":3}`)
		first, firstSeq, err := s.canonical.Insert(ev)
		if err != nil || first != postgres.Accepted {
			t.Fatalf("first insert: outcome=%v seq=%d err=%v", first, firstSeq, err)
		}
		second, secondSeq, err := s.canonical.Insert(ev)
		if err != nil || second != postgres.AlreadyPresent || secondSeq != firstSeq {
			t.Fatalf("retry: outcome=%v seq=%d err=%v", second, secondSeq, err)
		}
		rows, err := s.canonical.Pull(0, 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("want one canonical row after retry, rows=%d err=%v", len(rows), err)
		}
	})

	t.Run("4_changed_payload_is_conflict_without_overwrite", func(t *testing.T) {
		s.reset(t)
		id := uuid.NewString()
		original := acceptanceEvent(t, id, `{"value":"original"}`)
		changed := acceptanceEvent(t, id, `{"value":"changed"}`)
		if outcome, _, err := s.canonical.Insert(original); err != nil || outcome != postgres.Accepted {
			t.Fatalf("original insert: outcome=%v err=%v", outcome, err)
		}
		outcome, _, err := s.canonical.Insert(changed)
		if err != nil || outcome != postgres.Conflict {
			t.Fatalf("changed insert: outcome=%v err=%v", outcome, err)
		}
		rows, err := s.canonical.Pull(0, 10)
		if err != nil || len(rows) != 1 || string(rows[0].Event.Content) != `{"value": "original"}` {
			t.Fatalf("canonical overwrite detected: rows=%+v err=%v", rows, err)
		}
	})

	t.Run("5_invalid_submission_is_not_canonical", func(t *testing.T) {
		s.reset(t)
		invalid := event.Event{ID: uuid.NewString()}
		outcome, _, err := s.canonical.Insert(invalid)
		if outcome != postgres.Rejected || err == nil {
			t.Fatalf("want rejected invalid event, outcome=%v err=%v", outcome, err)
		}
		rows, err := s.canonical.Pull(0, 10)
		if err != nil || len(rows) != 0 {
			t.Fatalf("invalid event entered canonical storage: rows=%d err=%v", len(rows), err)
		}
	})

	t.Run("6_event_and_blob_are_atomic", func(t *testing.T) {
		s.reset(t)
		bad := acceptanceEvent(t, uuid.NewString(), `{"case":6,"bad":true}`)
		badOutcome, _, badErr := s.canonical.InsertWithBlob(bad, []byte("blob"), "0000000000000000000000000000000000000000000000000000000000000000", "text/plain")
		if badOutcome != postgres.Rejected || badErr == nil {
			t.Fatalf("want digest rejection, outcome=%v err=%v", badOutcome, badErr)
		}
		var blobs int
		if err := s.admin.QueryRow(`SELECT count(*) FROM blobs`).Scan(&blobs); err != nil {
			t.Fatal(err)
		}
		if blobs != 0 {
			t.Fatalf("digest rejection left canonical blob rows: %d", blobs)
		}
		good := acceptanceEvent(t, uuid.NewString(), `{"case":6,"good":true}`)
		content := []byte("blob")
		sum := fmt.Sprintf("%x", sha256Bytes(content))
		outcome, _, err := s.canonical.InsertWithBlob(good, content, sum, "text/plain")
		if err != nil || outcome != postgres.Accepted {
			t.Fatalf("valid event/blob insert: outcome=%v err=%v", outcome, err)
		}
		var events int
		if err := s.admin.QueryRow(`SELECT count(*) FROM events`).Scan(&events); err != nil {
			t.Fatal(err)
		}
		if events != 1 {
			t.Fatalf("want one atomic event, got %d", events)
		}
	})

	t.Run("7_fresh_sqlite_replica_pulls_canonical_set", func(t *testing.T) {
		s.reset(t)
		for i := 0; i < 3; i++ {
			if outcome, _, err := s.canonical.Insert(acceptanceEvent(t, uuid.NewString(), fmt.Sprintf(`{"case":7,"n":%d}`, i))); err != nil || outcome != postgres.Accepted {
				t.Fatalf("insert %d: outcome=%v err=%v", i, outcome, err)
			}
		}
		loc := openLocal(t)
		defer loc.Close()
		applied, err := ledgerSync.Pull(s.canonical, loc, 2)
		if err != nil || applied != 3 {
			t.Fatalf("pull: applied=%d err=%v", applied, err)
		}
		cursor, err := loc.Cursor()
		if err != nil || cursor <= 0 {
			t.Fatalf("cursor: %d err=%v", cursor, err)
		}
	})

	t.Run("8_concurrent_inserts_receive_unique_sequences", func(t *testing.T) {
		s.reset(t)
		const n = 12
		var wg sync.WaitGroup
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				outcome, _, err := s.canonical.Insert(acceptanceEvent(t, uuid.NewString(), fmt.Sprintf(`{"case":8,"n":%d}`, i)))
				if err != nil || outcome != postgres.Accepted {
					errs <- fmt.Errorf("insert %d: outcome=%v err=%v", i, outcome, err)
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		var count, distinct int
		if err := s.admin.QueryRow(`SELECT count(*), count(DISTINCT sequence) FROM events`).Scan(&count, &distinct); err != nil {
			t.Fatal(err)
		}
		if count != n || distinct != n {
			t.Fatalf("want %d unique canonical sequences, count=%d distinct=%d", n, count, distinct)
		}
	})

	t.Run("9_retry_after_client_crash_is_lossless", func(t *testing.T) {
		s.reset(t)
		loc := openLocal(t)
		defer loc.Close()
		ev := acceptanceEvent(t, uuid.NewString(), `{"case":9}`)
		if _, err := loc.Enqueue(ev); err != nil {
			t.Fatal(err)
		}
		if outcome, _, err := s.canonical.Insert(ev); err != nil || outcome != postgres.Accepted {
			t.Fatalf("simulate remote acceptance: outcome=%v err=%v", outcome, err)
		}
		result, err := ledgerSync.Push(loc, s.canonical, 0)
		if err != nil || result.AlreadyPresent != 1 {
			t.Fatalf("retry: result=%+v err=%v", result, err)
		}
		pending, err := loc.Pending()
		if err != nil || len(pending) != 0 {
			t.Fatalf("retry left pending loss/duplicate: pending=%d err=%v", len(pending), err)
		}
	})

	t.Run("10_ordinary_ingest_cannot_mutate_canonical", func(t *testing.T) {
		dsn := os.Getenv("DURO_INGEST_TEST_DSN")
		if dsn == "" {
			t.Skip("DURO_INGEST_TEST_DSN not set; provision a restricted ingest role to run this case")
		}
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, statement := range []string{"UPDATE events SET actor = actor", "DELETE FROM events"} {
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			_, execErr := tx.Exec(statement)
			_ = tx.Rollback()
			if execErr == nil {
				t.Errorf("ordinary ingest role unexpectedly executed %s", statement)
			}
		}
	})

	t.Run("11_replay_from_sequence_zero_matches_incremental_projection", func(t *testing.T) {
		s.reset(t)
		for i := 0; i < 5; i++ {
			if _, _, err := s.canonical.Insert(acceptanceEvent(t, uuid.NewString(), fmt.Sprintf(`{"case":11,"n":%d}`, i))); err != nil {
				t.Fatal(err)
			}
		}
		incremental := map[string]string{}
		var cursor int64
		for {
			rows, err := s.canonical.Pull(cursor, 2)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) == 0 {
				break
			}
			applyProjection(incremental, rows)
			cursor = rows[len(rows)-1].Sequence
		}
		replayRows, err := s.canonical.Pull(0, 100)
		if err != nil {
			t.Fatal(err)
		}
		replayed := map[string]string{}
		applyProjection(replayed, replayRows)
		if !reflect.DeepEqual(incremental, replayed) {
			t.Fatalf("incremental and sequence-zero projections differ: incremental=%v replayed=%v", incremental, replayed)
		}
	})

	t.Run("12_filesystem_backend_writes_retries_and_reads", func(t *testing.T) {
		s.reset(t)
		backend := openAcceptanceFilesystemStore(t)
		content := []byte("filesystem acceptance blob")
		ev := acceptanceEvent(t, uuid.NewString(), `{"case":12}`)

		first := openLocal(t)
		if fresh, err := first.EnqueueWithBlob(ev, content, "text/plain"); err != nil || !fresh {
			t.Fatalf("first enqueue: fresh=%v err=%v", fresh, err)
		}
		staged, ok, err := first.PendingBlob(ev.ID)
		if err != nil || !ok {
			t.Fatalf("first pending blob: ok=%v err=%v", ok, err)
		}
		result, err := ledgerSync.PushWithBlobStore(first, s.canonical, backend, 0)
		if err != nil || result.Accepted != 1 {
			t.Fatalf("first filesystem push: result=%+v err=%v", result, err)
		}
		got, err := backend.Read(staged.SHA256, int64(len(content)))
		if err != nil || !bytes.Equal(got, content) {
			t.Fatalf("filesystem blob read: got=%q err=%v", got, err)
		}

		var contentIsNull bool
		if err := s.admin.QueryRow(
			`SELECT b.content IS NULL FROM events e JOIN blobs b ON b.sha256 = e.blob_sha256 WHERE e.id = $1`, ev.ID,
		).Scan(&contentIsNull); err != nil || !contentIsNull {
			t.Fatalf("canonical blob reference: content_is_null=%v err=%v", contentIsNull, err)
		}

		retry := openLocal(t)
		if fresh, err := retry.EnqueueWithBlob(ev, content, "text/plain"); err != nil || !fresh {
			t.Fatalf("retry enqueue: fresh=%v err=%v", fresh, err)
		}
		result, err = ledgerSync.PushWithBlobStore(retry, s.canonical, backend, 0)
		if err != nil || result.AlreadyPresent != 1 {
			t.Fatalf("exact retry: result=%+v err=%v", result, err)
		}
		changed := openLocal(t)
		if fresh, err := changed.EnqueueWithBlob(ev, []byte("changed filesystem acceptance blob"), "text/plain"); err != nil || !fresh {
			t.Fatalf("changed retry enqueue: fresh=%v err=%v", fresh, err)
		}
		result, err = ledgerSync.PushWithBlobStore(changed, s.canonical, backend, 0)
		if err != nil || result.Conflict != 1 {
			t.Fatalf("changed retry: result=%+v err=%v", result, err)
		}
		rows, err := s.canonical.Pull(0, 10)
		if err != nil || len(rows) != 1 || rows[0].Event.ID != ev.ID || rows[0].BlobSHA256 != staged.SHA256 {
			t.Fatalf("canonical readback: rows=%+v err=%v", rows, err)
		}
	})
}

func openAcceptanceFilesystemStore(t *testing.T) *blob.FilesystemStore {
	t.Helper()
	root := os.Getenv("DURO_FILESYSTEM_BLOB_ROOT")
	if root == "" {
		t.Skip("DURO_FILESYSTEM_BLOB_ROOT not set")
	}
	if !filepath.IsAbs(root) {
		t.Fatalf("DURO_FILESYSTEM_BLOB_ROOT must be absolute, got %q", root)
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Fatalf("DURO_FILESYSTEM_BLOB_ROOT must be an existing directory: %v", err)
	}
	testRoot, err := os.MkdirTemp(root, ".duro-acceptance-")
	if err != nil {
		t.Fatalf("create filesystem acceptance root: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(testRoot); err != nil {
			t.Errorf("remove filesystem acceptance root %s: %v", testRoot, err)
		}
	})
	backend, err := blob.NewFilesystemStore(testRoot)
	if err != nil {
		t.Fatalf("open filesystem acceptance store: %v", err)
	}
	return backend
}

func openLocal(t *testing.T) *local.Store {
	t.Helper()
	loc, err := local.Open(t.TempDir() + "/local.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { loc.Close() })
	return loc
}

func applyProjection(dst map[string]string, rows []postgres.PulledEvent) {
	for _, row := range rows {
		dst[row.Event.ID] = string(row.Event.Content)
	}
}

func sha256Bytes(content []byte) []byte {
	sum := sha256.Sum256(content)
	return sum[:]
}
