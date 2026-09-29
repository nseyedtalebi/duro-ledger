package artifact

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nseyedtalebi/duro-ledger/internal/pgtest"
	"github.com/nseyedtalebi/duro-ledger/pkg/cas"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

// fakeAppender stands in for the ledger so append failures and lost-commit
// outcomes can be tested without breaking a real database connection.
type fakeAppender struct {
	calls []event.New
	err   error
}

func (f *fakeAppender) Append(n event.New) (postgres.StoredEvent, error) {
	f.calls = append(f.calls, n)
	if f.err != nil {
		return postgres.StoredEvent{}, f.err
	}
	return postgres.StoredEvent{ID: "fake", EventType: n.EventType, Content: n.ContentOrDefault(), Refs: n.RefsOrDefault()}, nil
}

func newStore(t *testing.T) *cas.Store {
	t.Helper()
	s, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	return s
}

func sourceFile(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func identityOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func decodeContent(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decoding observation content %s: %v", raw, err)
	}
	return m
}

// TestPutObservationAgainstLiveLedger is the contract's exact
// observation-event readback, through a real PostgreSQL ledger.
func TestPutObservationAgainstLiveLedger(t *testing.T) {
	dsn := pgtest.NewDatabase(t)
	if err := postgres.Initialize(dsn); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	ledger, err := postgres.Open(dsn)
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	defer ledger.Close()

	store := newStore(t)
	data := bytes.Repeat([]byte("observed"), 1024)
	src := sourceFile(t, data)
	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname: %v", err)
	}

	result, err := Put(store, ledger, src)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if result.Digest != identityOf(data) || result.Size != int64(len(data)) {
		t.Errorf("Put = %s, %d bytes", result.Digest, result.Size)
	}
	if result.Event.EventType != EventType {
		t.Errorf("event_type = %q, want %q", result.Event.EventType, EventType)
	}
	if string(result.Event.Refs) != "{}" {
		t.Errorf("refs = %s, want {}", result.Event.Refs)
	}
	content := decodeContent(t, result.Event.Content)
	want := map[string]any{"digest": result.Digest, "size_bytes": float64(len(data)), "source_host": host, "source_path": src}
	for k, v := range want {
		if content[k] != v {
			t.Errorf("content[%q] = %v, want %v", k, content[k], v)
		}
	}
	if len(content) != len(want) {
		t.Errorf("content has %d keys (%v), want exactly %v", len(content), content, want)
	}

	// A repeat put reuses the bytes and appends another observation event.
	second, err := Put(store, ledger, src)
	if err != nil {
		t.Fatalf("repeat Put: %v", err)
	}
	if second.Digest != result.Digest || second.Size != result.Size {
		t.Errorf("repeat put receipt differs: %s %d", second.Digest, second.Size)
	}
	if second.Event.ID == result.Event.ID {
		t.Error("repeat put reused the observation event")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT id::text, event_type, actor, content::text, refs::text FROM public.events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var id, typ, actor, gotContent, gotRefs string
		if err := rows.Scan(&id, &typ, &actor, &gotContent, &gotRefs); err != nil {
			t.Fatal(err)
		}
		seen++
		if typ != EventType || gotRefs != "{}" {
			t.Errorf("row %s: type %q refs %s", id, typ, gotRefs)
		}
		if actor != result.Event.Actor {
			t.Errorf("row %s: actor %q, want %q", id, actor, result.Event.Actor)
		}
		for _, want := range []string{
			fmt.Sprintf(`"digest": %q`, result.Digest),
			fmt.Sprintf(`"size_bytes": %d`, result.Size),
			fmt.Sprintf(`"source_host": %q`, host),
			fmt.Sprintf(`"source_path": %q`, src),
		} {
			if !strings.Contains(gotContent, want) {
				t.Errorf("row %s content %s missing %s", id, gotContent, want)
			}
		}
	}
	if seen != 2 {
		t.Errorf("ledger holds %d observation events, want 2", seen)
	}
}

func TestPutUsesAbsoluteSourcePath(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "rel.bin"), []byte("relative"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)

	fake := &fakeAppender{}
	result, err := Put(newStore(t), fake, "rel.bin")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	content := decodeContent(t, result.Event.Content)
	path, _ := content["source_path"].(string)
	if !filepath.IsAbs(path) {
		t.Errorf("source_path = %q, want an absolute path", path)
	}
	if host, _ := content["source_host"].(string); host == "" {
		t.Error("source_host is empty; the observing host must always be recorded")
	}
}

// TestPutRejectsNonUTF8SourcePath covers a source path that JSON cannot
// represent exactly. Unix filenames are arbitrary bytes and encoding/json
// silently substitutes U+FFFD for invalid ones, so such a put must be
// refused before anything is ingested: no blob, no event, no partial
// outcome. (APFS and most modern filesystems refuse to create such a name,
// so the invalid bytes are supplied by the caller rather than by a fixture
// file on disk.)
func TestPutRejectsNonUTF8SourcePath(t *testing.T) {
	root := t.TempDir()
	store, err := cas.Open(root)
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	t.Chdir(t.TempDir())

	fake := &fakeAppender{}
	_, err = Put(store, fake, "invalid-\xff\xfe.bin")
	if err == nil {
		t.Fatal("Put of a non-UTF-8 source path = nil, want error")
	}
	var pe *PutError
	if errors.As(err, &pe) {
		t.Errorf("rejection reported as a partial outcome: %v", err)
	}
	if !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("error = %v, want it to name the encoding problem", err)
	}
	if len(fake.calls) != 0 {
		t.Errorf("ledger was called %d times, want 0", len(fake.calls))
	}
	// No bytes were ingested: the store holds no blob at all.
	var blobs int
	if err := filepath.WalkDir(filepath.Join(root, "sha256"), func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			blobs++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if blobs != 0 {
		t.Errorf("store holds %d blobs after a rejected put, want 0", blobs)
	}
}

func TestPutEventAppendFailure(t *testing.T) {
	for _, tc := range []struct {
		name        string
		err         error
		wantOutcome string
	}{
		{"confirmed rejection", &postgres.AppendError{Outcome: postgres.OutcomeNotCommitted, Err: errors.New("permission denied")}, "not_committed"},
		{"lost confirmation", &postgres.AppendError{Outcome: postgres.OutcomeUnknown, Err: errors.New("connection reset")}, "unknown"},
		// An appender that does not classify its failure has given no
		// evidence that nothing was committed, so the conservative outcome
		// applies.
		{"untyped failure", errors.New("something else"), "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			data := []byte("stored but unrecorded: " + tc.name)
			src := sourceFile(t, data)
			fake := &fakeAppender{err: tc.err}

			_, err := Put(store, fake, src)
			var pe *PutError
			if !errors.As(err, &pe) {
				t.Fatalf("Put = %v, want *PutError", err)
			}
			if pe.Digest != identityOf(data) || pe.Size != int64(len(data)) {
				t.Errorf("PutError reports %s / %d bytes", pe.Digest, pe.Size)
			}
			if !pe.ArtifactStored {
				t.Error("artifact_stored = false, want true")
			}
			if pe.EventOutcome != tc.wantOutcome {
				t.Errorf("event_outcome = %q, want %q", pe.EventOutcome, tc.wantOutcome)
			}
			if !errors.Is(err, tc.err) {
				t.Error("PutError does not unwrap to the append failure")
			}
			// The bytes are retained and verifiable: retrying the append is
			// the caller's decision, and the artifact must not be lost.
			digest := strings.TrimPrefix(pe.Digest, "sha256:")
			if err := store.Verify(digest, pe.Size); err != nil {
				t.Errorf("stored artifact was not retained: %v", err)
			}
		})
	}
}

func TestPutStorageFailureAppendsNothing(t *testing.T) {
	fake := &fakeAppender{}
	if _, err := Put(newStore(t), fake, filepath.Join(t.TempDir(), "absent.bin")); err == nil {
		t.Fatal("Put of a missing source = nil, want error")
	}
	var pe *PutError
	if _, err := Put(newStore(t), fake, t.TempDir()); err == nil {
		t.Fatal("Put of a directory = nil, want error")
	} else if errors.As(err, &pe) {
		t.Error("a storage failure must not be reported as a partial outcome")
	}
	if len(fake.calls) != 0 {
		t.Errorf("appended %d events despite storage failure", len(fake.calls))
	}
}

// TestPutWriteFailureAppendsNothing is the storage-side write failure: the
// bytes cannot even be staged, so no observation event may be appended.
func TestPutWriteFailureAppendsNothing(t *testing.T) {
	dir := t.TempDir()
	store, err := cas.Open(dir)
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	staging := filepath.Join(dir, ".tmp")
	if err := os.Chmod(staging, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(staging, 0o755) })

	fake := &fakeAppender{}
	if _, err := Put(store, fake, sourceFile(t, []byte("cannot be staged"))); err == nil {
		t.Fatal("Put with an unwritable staging directory = nil, want error")
	}
	if len(fake.calls) != 0 {
		t.Errorf("appended %d events after a write failure", len(fake.calls))
	}
	entries, err := os.ReadDir(filepath.Join(dir, "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("a failed put published something: %v", entries)
	}
}

func TestGetWrappersRejectMalformedIdentity(t *testing.T) {
	store := newStore(t)
	data := []byte("round trip")
	if _, err := Put(store, &fakeAppender{}, sourceFile(t, data)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	identity := identityOf(data)

	dest := filepath.Join(t.TempDir(), "out.bin")
	if err := GetToFile(store, identity, dest); err != nil {
		t.Fatalf("GetToFile: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("round trip mismatch: %v", err)
	}
	var buf bytes.Buffer
	if err := GetToWriter(store, identity, &buf); err != nil || !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("GetToWriter: %v", err)
	}
	for _, bad := range []string{strings.TrimPrefix(identity, "sha256:"), "sha256:nope", ""} {
		if err := GetToWriter(store, bad, &buf); !errors.Is(err, cas.ErrInvalidDigest) {
			t.Errorf("GetToWriter(%q) = %v, want ErrInvalidDigest", bad, err)
		}
		if err := GetToFile(store, bad, filepath.Join(t.TempDir(), "x")); !errors.Is(err, cas.ErrInvalidDigest) {
			t.Errorf("GetToFile(%q) = %v, want ErrInvalidDigest", bad, err)
		}
	}
}
