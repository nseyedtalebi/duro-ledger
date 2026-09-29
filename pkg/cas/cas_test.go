package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func mustOpen(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func writeSource(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	return path
}

func hexDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// tempFiles lists the store's staging directory, which must be empty after
// every handled failure and every completed operation.
func tempFiles(t *testing.T, s *Store) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.root, ".tmp"))
	if err != nil {
		t.Fatalf("reading .tmp: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestIdentityParsing(t *testing.T) {
	good := strings.Repeat("ab", 32)
	if got, err := ParseIdentity("sha256:" + good); err != nil || got != good {
		t.Errorf("ParseIdentity(valid) = %q, %v", got, err)
	}
	if FormatIdentity(good) != "sha256:"+good {
		t.Errorf("FormatIdentity did not prefix")
	}
	for _, bad := range []string{
		good,                              // bare hex, no prefix
		"sha256:" + strings.ToUpper(good), // uppercase
		"sha256:" + good[:63],             // short
		"sha256:" + good + "a",            // long
		"sha256:" + strings.Repeat("zz", 32),
		"sha1:" + good,
		"", "sha256:",
	} {
		if _, err := ParseIdentity(bad); !errors.Is(err, ErrInvalidDigest) {
			t.Errorf("ParseIdentity(%q) = %v, want ErrInvalidDigest", bad, err)
		}
	}
}

func TestWriteFileRoundTripAndDuplicate(t *testing.T) {
	s := mustOpen(t)
	data := bytes.Repeat([]byte("duro"), 5000)
	src := writeSource(t, data)

	digest, size, deduped, err := s.WriteFile(src)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if digest != hexDigest(data) || size != int64(len(data)) || deduped {
		t.Fatalf("WriteFile = %s, %d, %v", digest, size, deduped)
	}
	if !s.Has(digest) {
		t.Fatal("Has(digest) = false after put")
	}
	// Newly created ancestor directories and the blob itself must be
	// durable; at minimum they must exist as regular files/dirs.
	if info, err := os.Lstat(s.Path(digest)); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("stored path: %v %v", info, err)
	}
	if err := s.Verify(digest, size); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	// Duplicate put of identical bytes: verified and reused.
	_, _, deduped2, err := s.WriteFile(writeSource(t, data))
	if err != nil || !deduped2 {
		t.Fatalf("duplicate WriteFile = %v, deduped=%v", err, deduped2)
	}

	out := filepath.Join(t.TempDir(), "out.bin")
	if err := s.GetToFile(digest, out); err != nil {
		t.Fatalf("GetToFile: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("round trip mismatch (%d bytes, err %v)", len(got), err)
	}
	// Existing destinations are never silently overwritten.
	if err := s.GetToFile(digest, out); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("GetToFile over existing = %v, want ErrDestinationExists", err)
	}
	var buf bytes.Buffer
	if err := s.GetToWriter(digest, &buf); err != nil || !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("GetToWriter = %v", err)
	}
	if names := tempFiles(t, s); len(names) != 0 {
		t.Errorf("staging dir not empty: %v", names)
	}
}

func TestMissingAndMalformedGet(t *testing.T) {
	s := mustOpen(t)
	absent := hexDigest([]byte("never stored"))
	if err := s.GetToWriter(absent, io.Discard); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetToWriter(absent) = %v, want ErrNotFound", err)
	}
	if err := s.GetToFile(absent, filepath.Join(t.TempDir(), "x")); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetToFile(absent) = %v, want ErrNotFound", err)
	}
	if err := s.GetToWriter("nothex", io.Discard); !errors.Is(err, ErrInvalidDigest) {
		t.Errorf("GetToWriter(malformed) = %v, want ErrInvalidDigest", err)
	}
	if err := s.Verify(absent, -1); err == nil {
		t.Error("Verify with negative size = nil, want error")
	}
}

// corrupt overwrites a stored blob's bytes in place, keeping its length, to
// stand in for on-disk bit rot.
func corrupt(t *testing.T, s *Store, digest string, replacement []byte) {
	t.Helper()
	path := s.Path(digest)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := os.WriteFile(path, replacement, 0o444); err != nil {
		t.Fatalf("corrupting: %v", err)
	}
}

func TestCorruptStoredArtifact(t *testing.T) {
	s := mustOpen(t)
	data := []byte("original bytes")
	src := writeSource(t, data)
	digest, size, _, err := s.WriteFile(src)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	rot := []byte("corrupted byte")
	if len(rot) != len(data) {
		t.Fatalf("test setup: replacement must be the same length")
	}
	corrupt(t, s, digest, rot)

	if err := s.Verify(digest, size); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Verify(corrupt) = %v, want ErrCorrupt", err)
	}
	// A repeat put of the original bytes must report the corruption and
	// leave the existing artifact exactly as it is.
	if _, _, _, err := s.WriteFile(src); !errors.Is(err, ErrCorrupt) {
		t.Errorf("WriteFile onto corrupt digest = %v, want ErrCorrupt", err)
	}
	onDisk, err := os.ReadFile(s.Path(digest))
	if err != nil || !bytes.Equal(onDisk, rot) {
		t.Errorf("existing artifact was modified: %q (%v)", onDisk, err)
	}
	// Get refuses to publish unverified bytes, but a stream may already
	// have emitted them.
	dest := filepath.Join(t.TempDir(), "out")
	if err := s.GetToFile(digest, dest); !errors.Is(err, ErrCorrupt) {
		t.Errorf("GetToFile(corrupt) = %v, want ErrCorrupt", err)
	}
	if _, err := os.Lstat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("corrupt GetToFile published %s", dest)
	}
	var buf bytes.Buffer
	if err := s.GetToWriter(digest, &buf); !errors.Is(err, ErrCorrupt) {
		t.Errorf("GetToWriter(corrupt) = %v, want ErrCorrupt", err)
	}
	if !bytes.Equal(buf.Bytes(), rot) {
		t.Error("stream output should have delivered the (unverified) bytes it read")
	}
	if names := tempFiles(t, s); len(names) != 0 {
		t.Errorf("staging dir not empty: %v", names)
	}
}

func TestConcurrentSameDigestPuts(t *testing.T) {
	s := mustOpen(t)
	data := bytes.Repeat([]byte("concurrent"), 100_000) // ~1 MiB
	want := hexDigest(data)
	src := writeSource(t, data)

	const n = 8
	var wg sync.WaitGroup
	results := make([]bool, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			digest, size, deduped, err := s.WriteFile(src)
			if err == nil && (digest != want || size != int64(len(data))) {
				err = fmt.Errorf("digest/size mismatch: %s %d", digest, size)
			}
			results[i], errs[i] = deduped, err
		}()
	}
	wg.Wait()

	fresh := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("put %d: %v", i, errs[i])
		}
		if !results[i] {
			fresh++
		}
	}
	if fresh != 1 {
		t.Errorf("%d puts reported storing fresh bytes, want exactly 1", fresh)
	}
	stored, err := os.ReadFile(s.Path(want))
	if err != nil || !bytes.Equal(stored, data) {
		t.Fatalf("stored bytes wrong after concurrent puts (%v)", err)
	}
	if names := tempFiles(t, s); len(names) != 0 {
		t.Errorf("staging dir not empty: %v", names)
	}
}

// syncRecorder records which handles a Store actually fsynced, so tests can
// assert durability instead of mere existence, and can fail or pause a
// chosen sync deterministically.
type syncRecorder struct {
	mu     sync.Mutex
	paths  map[string]int
	failOn func(path string) error // optional
	hook   func(path string)       // optional, runs before the sync
}

func (r *syncRecorder) sync(f *os.File) error {
	if r.hook != nil {
		r.hook(f.Name())
	}
	r.mu.Lock()
	if r.paths == nil {
		r.paths = map[string]int{}
	}
	r.paths[f.Name()]++
	r.mu.Unlock()
	if r.failOn != nil {
		return r.failOn(f.Name())
	}
	return nil
}

func (r *syncRecorder) assertSynced(t *testing.T, what string, paths ...string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range paths {
		if r.paths[p] == 0 {
			t.Errorf("%s: %s was never fsynced (synced: %v)", what, p, r.paths)
		}
	}
}

// durabilityChain is every handle a put must make durable: the blob itself
// and each directory entry leading to it, up to and including the store
// root (whose own entry was made durable by Open).
func durabilityChain(s *Store, digest string) []string {
	blob := s.Path(digest)
	leaf := filepath.Dir(blob)
	shard := filepath.Dir(leaf)
	return []string{blob, leaf, shard, filepath.Dir(shard), s.root}
}

// TestPutSyncsBlobAndAncestorChain covers the case where an earlier attempt
// (or another process) created the shard hierarchy but never made it
// durable: a put that finds the directories already present must still
// fsync the blob and the whole chain before reporting success -- on the
// fresh put and on the deduplicating one.
func TestPutSyncsBlobAndAncestorChain(t *testing.T) {
	base := mustOpen(t)
	data := []byte("durability is not existence")
	src := writeSource(t, data)
	digest := hexDigest(data)
	// Stand in for a previous attempt that created ancestors and crashed
	// before syncing them.
	if err := os.MkdirAll(filepath.Dir(base.Path(digest)), 0o755); err != nil {
		t.Fatal(err)
	}

	fresh := &syncRecorder{}
	if _, _, deduped, err := base.withSyncFile(fresh.sync).WriteFile(src); err != nil || deduped {
		t.Fatalf("fresh put = %v, deduped=%v", err, deduped)
	}
	fresh.assertSynced(t, "fresh put", durabilityChain(base, digest)...)

	dup := &syncRecorder{}
	if _, _, deduped, err := base.withSyncFile(dup.sync).WriteFile(src); err != nil || !deduped {
		t.Fatalf("duplicate put = %v, deduped=%v", err, deduped)
	}
	dup.assertSynced(t, "duplicate put", durabilityChain(base, digest)...)
}

// TestSymlinkedRootResolvesToCanonicalRoot covers a store root reached
// through a symlink. filepath.Abs is purely lexical, so without resolution
// every path (and every fsync) would name the link rather than the
// directories the blobs are actually written into -- and the durability
// assertions themselves would target the wrong entries.
func TestSymlinkedRootResolvesToCanonicalRoot(t *testing.T) {
	base := t.TempDir()
	actual := filepath.Join(base, "actual")
	if err := os.Mkdir(actual, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(actual, link); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(actual)
	if err != nil {
		t.Fatal(err)
	}

	s, err := Open(link)
	if err != nil {
		t.Fatalf("Open through a symlink: %v", err)
	}
	if s.root != canonical {
		t.Errorf("root = %q, want the canonical path %q", s.root, canonical)
	}

	data := []byte("stored through a symlinked root")
	src := writeSource(t, data)
	rec := &syncRecorder{}
	digest, size, _, err := s.withSyncFile(rec.sync).WriteFile(src)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if digest != hexDigest(data) {
		t.Errorf("digest = %s, want %s", digest, hexDigest(data))
	}
	if !strings.HasPrefix(s.Path(digest), canonical+string(filepath.Separator)) {
		t.Errorf("blob path %q is not under the canonical root %q", s.Path(digest), canonical)
	}
	// The recorder saw the real directories, not the link.
	rec.assertSynced(t, "put through a symlinked root", durabilityChain(s, digest)...)
	if err := s.Verify(digest, size); err != nil {
		t.Errorf("Verify: %v", err)
	}

	out := filepath.Join(t.TempDir(), "restored.bin")
	if err := s.GetToFile(digest, out); err != nil {
		t.Fatalf("GetToFile: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("roundtrip through a symlinked root returned different bytes")
	}
}

func TestAncestorSyncFailureThenRetry(t *testing.T) {
	base := mustOpen(t)
	data := []byte("ancestor sync failure")
	src := writeSource(t, data)
	digest := hexDigest(data)

	failing := &syncRecorder{failOn: func(path string) error {
		if filepath.Base(path) == "sha256" {
			return errors.New("simulated ancestor fsync failure")
		}
		return nil
	}}
	if _, _, _, err := base.withSyncFile(failing.sync).WriteFile(src); err == nil {
		t.Fatal("put with a failing ancestor fsync = nil, want error")
	}

	// The retry must re-establish durability for the whole chain even though
	// every directory (and possibly the blob) already exists.
	retry := &syncRecorder{}
	_, size, _, err := base.withSyncFile(retry.sync).WriteFile(src)
	if err != nil {
		t.Fatalf("retry after ancestor sync failure: %v", err)
	}
	retry.assertSynced(t, "retry", durabilityChain(base, digest)...)
	if err := base.Verify(digest, size); err != nil {
		t.Errorf("Verify after retry: %v", err)
	}
}

// TestConcurrentDedupSyncsIndependently pins the creator inside its own
// ancestor sync. The concurrent put that finds the blob already linked must
// verify it and establish durability itself rather than assuming the
// creator already did.
func TestConcurrentDedupSyncsIndependently(t *testing.T) {
	base := mustOpen(t)
	data := []byte("two putters, one digest")
	src := writeSource(t, data)
	digest := hexDigest(data)
	leaf := filepath.Dir(base.Path(digest))

	paused := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	creator := &syncRecorder{hook: func(path string) {
		if path == leaf {
			once.Do(func() {
				close(paused)
				<-release
			})
		}
	}}
	creatorDone := make(chan error, 1)
	go func() {
		_, _, _, err := base.withSyncFile(creator.sync).WriteFile(src)
		creatorDone <- err
	}()
	<-paused // the blob is linked; its parent entry is not yet durable

	second := &syncRecorder{}
	_, size, deduped, err := base.withSyncFile(second.sync).WriteFile(src)
	if err != nil {
		t.Fatalf("concurrent duplicate put: %v", err)
	}
	if !deduped {
		t.Error("concurrent put did not report reusing the existing bytes")
	}
	second.assertSynced(t, "concurrent duplicate put", durabilityChain(base, digest)...)
	if err := base.Verify(digest, size); err != nil {
		t.Errorf("Verify: %v", err)
	}

	close(release)
	if err := <-creatorDone; err != nil {
		t.Errorf("creator put: %v", err)
	}
}

// shortWriter accepts after bytes, then short-writes (or fails) exactly
// once, to exercise the write-error paths deterministically.
type shortWriter struct {
	after   int
	written int
	err     error // nil means "short write with no error"
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if w.written >= w.after {
		if w.err != nil {
			return 0, w.err
		}
		return len(p) - 1, nil // short write, no error: io.ErrShortWrite
	}
	w.written += len(p)
	return len(p), nil
}

func TestWriteErrorsSurface(t *testing.T) {
	data := bytes.Repeat([]byte("w"), 128<<10)

	if _, _, err := hashingCopy(&shortWriter{after: 64 << 10}, bytes.NewReader(data)); !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("hashingCopy with a short write = %v, want io.ErrShortWrite", err)
	}
	if _, _, err := hashingCopy(&shortWriter{after: 64 << 10, err: syscall.ENOSPC}, bytes.NewReader(data)); !errors.Is(err, syscall.ENOSPC) {
		t.Errorf("hashingCopy out of space = %v, want ENOSPC", err)
	}

	// The same failures through a real get: bytes may already have been
	// emitted, and the error must still be returned.
	s := mustOpen(t)
	digest, _, _, err := s.WriteFile(writeSource(t, data))
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	full := &shortWriter{after: 32 << 10, err: syscall.ENOSPC}
	if err := s.GetToWriter(digest, full); !errors.Is(err, syscall.ENOSPC) {
		t.Errorf("GetToWriter onto a full device = %v, want ENOSPC", err)
	}
	if full.written == 0 {
		t.Error("test setup: expected some bytes to be written before the failure")
	}
	if err := s.GetToWriter(digest, &shortWriter{after: 32 << 10}); !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("GetToWriter with a short write = %v, want io.ErrShortWrite", err)
	}
}

func TestSourceSizeMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(t *testing.T, path string)
	}{
		{"source grows after stat", func(t *testing.T, path string) {
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := f.Write(bytes.Repeat([]byte("x"), 4096)); err != nil {
				t.Fatal(err)
			}
		}},
		{"source shrinks after stat", func(t *testing.T, path string) {
			if err := os.Truncate(path, 1024); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := mustOpen(t)
			path := writeSource(t, bytes.Repeat([]byte("g"), 64<<10))
			// afterStat runs between WriteFile's stat and its read, so the
			// mid-ingestion change is deterministic rather than a race.
			s := base.withAfterStat(func() { tc.change(t, path) })
			if _, _, _, err := s.WriteFile(path); !errors.Is(err, ErrSizeMismatch) {
				t.Fatalf("WriteFile = %v, want ErrSizeMismatch", err)
			}
			if names := tempFiles(t, base); len(names) != 0 {
				t.Errorf("staging dir not empty after failure: %v", names)
			}
			entries, err := os.ReadDir(filepath.Join(base.root, "sha256"))
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Errorf("a size-mismatched put published something: %v", entries)
			}
		})
	}
}

type errReader struct {
	data  []byte
	after int
	n     int
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.n >= r.after {
		return 0, errors.New("simulated read interruption")
	}
	n := copy(p, r.data[r.n:min(r.n+1024, len(r.data))])
	r.n += n
	return n, nil
}

func TestInterruptedSourceRead(t *testing.T) {
	s := mustOpen(t)
	src := &errReader{data: bytes.Repeat([]byte("i"), 8192), after: 2048}
	if _, _, _, err := s.Write(src); err == nil || !strings.Contains(err.Error(), "interruption") {
		t.Fatalf("Write(interrupted) = %v, want the read error", err)
	}
	if names := tempFiles(t, s); len(names) != 0 {
		t.Errorf("staging dir not empty after interruption: %v", names)
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "sha256"))
	if err != nil {
		t.Fatalf("reading store: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("interrupted put published something: %v", entries)
	}
}

func TestStagingWriteFailure(t *testing.T) {
	s := mustOpen(t)
	tmpDir := filepath.Join(s.root, ".tmp")
	if err := os.Chmod(tmpDir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(tmpDir, 0o755) })
	if _, _, _, err := s.WriteFile(writeSource(t, []byte("nope"))); err == nil {
		t.Fatal("WriteFile with unwritable staging dir = nil, want error")
	}
}

func TestSyncFailurePreventsPublication(t *testing.T) {
	base := mustOpen(t)
	failing := base.withSyncFile(func(*os.File) error { return errors.New("simulated fsync failure") })
	data := []byte("needs durability")
	if _, _, _, err := failing.WriteFile(writeSource(t, data)); err == nil {
		t.Fatal("WriteFile with failing fsync = nil, want error")
	}
	if base.Has(hexDigest(data)) {
		t.Error("artifact was published despite an fsync failure")
	}
	if names := tempFiles(t, base); len(names) != 0 {
		t.Errorf("staging dir not empty after fsync failure: %v", names)
	}
}

func TestGetToFileUnwritableDestination(t *testing.T) {
	s := mustOpen(t)
	data := []byte("read only destination")
	digest, _, _, err := s.WriteFile(writeSource(t, data))
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
	if err := s.GetToFile(digest, filepath.Join(dir, "out")); err == nil {
		t.Fatal("GetToFile into unwritable directory = nil, want error")
	}
}

type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

// TestBoundedMemoryAcrossSizes records allocation per operation at fixed
// concurrency as artifact size grows by 16x. Working memory must stay
// bounded by the copy buffer, not the artifact.
func TestBoundedMemoryAcrossSizes(t *testing.T) {
	const concurrency = 4
	var first float64
	for _, size := range []int{1 << 20, 4 << 20, 16 << 20} {
		s := mustOpen(t)
		data := bytes.Repeat([]byte("m"), size)
		srcs := make([]string, concurrency)
		for i := range srcs {
			// Distinct digests so each goroutine stores its own artifact.
			srcs[i] = writeSource(t, append(append([]byte{}, data...), byte(i)))
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)

		var wg sync.WaitGroup
		for i := range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				digest, _, _, err := s.WriteFile(srcs[i])
				if err != nil {
					t.Errorf("WriteFile: %v", err)
					return
				}
				if err := s.GetToWriter(digest, &countingWriter{}); err != nil {
					t.Errorf("GetToWriter: %v", err)
				}
			}()
		}
		wg.Wait()
		runtime.ReadMemStats(&after)

		perOp := float64(after.TotalAlloc-before.TotalAlloc) / float64(2*concurrency)
		t.Logf("artifact size %8d bytes, concurrency %d: %.0f bytes allocated per put+get", size, concurrency, perOp)
		if first == 0 {
			first = perOp
		}
		// Bounded means bounded: 16x the bytes must not cost anywhere near
		// 16x the allocation. A generous 4x ceiling still fails loudly if
		// any path starts buffering whole artifacts.
		if perOp > 4*first+float64(1<<20) {
			t.Errorf("allocation per operation grew with artifact size: %.0f bytes at %d, %.0f at the smallest size", perOp, size, first)
		}
	}
}
