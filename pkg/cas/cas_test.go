package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteGetDedup(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("hello, content-addressed world")
	digest1, size1, deduped1, err := store.Write(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if deduped1 {
		t.Fatal("first write should not be deduped")
	}
	if size1 != int64(len(content)) {
		t.Fatalf("size = %d, want %d", size1, len(content))
	}

	digest2, _, deduped2, err := store.Write(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if !deduped2 {
		t.Fatal("second write of identical content should be deduped")
	}
	if digest1 != digest2 {
		t.Fatalf("digests differ: %s != %s", digest1, digest2)
	}

	f, err := store.Get(digest1)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("read back %q, want %q", got, content)
	}
}

func TestReadOnly(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	digest, _, _, err := store.Write(bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(store.Path(digest))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o200 != 0 {
		t.Fatalf("blob is writable: mode %v", info.Mode())
	}
}

func TestPathSharding(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	digest := "abcdef0123456789"
	want := filepath.Join(store.root, "sha256", "ab", "cd", digest)
	if got := store.Path(digest); got != want {
		t.Fatalf("Path = %s, want %s", got, want)
	}
}

func TestWriteFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	if err := os.WriteFile(src, []byte("from a file"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	digest, size, deduped, err := store.WriteFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if deduped {
		t.Fatal("should not be deduped")
	}
	if size != 11 {
		t.Fatalf("size = %d, want 11", size)
	}
	if !store.Has(digest) {
		t.Fatal("store should have digest after write")
	}
}

func TestWriteExpectedRejectsMismatch(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wrongDigest := strings.Repeat("0", 64)
	_, _, err = store.WriteExpected(bytes.NewReader([]byte("hello")), wrongDigest)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("err = %v, want ErrDigestMismatch", err)
	}
	if store.Has(wrongDigest) {
		t.Fatal("mismatched digest must not be stored under the wanted digest")
	}
}

func TestWriteExpectedAcceptsMatch(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello, content-addressed world")
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])

	size, deduped, err := store.WriteExpected(bytes.NewReader(content), digest)
	if err != nil {
		t.Fatal(err)
	}
	if deduped {
		t.Fatal("first write should not be deduped")
	}
	if size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", size, len(content))
	}
	if !store.Has(digest) {
		t.Fatal("store should have digest after WriteExpected")
	}
}

func TestWriteExpectedRejectsExistingCorruptBlob(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("original exact bytes")
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	if _, _, err := store.WriteExpected(bytes.NewReader(content), digest); err != nil {
		t.Fatal(err)
	}
	path := store.Path(digest)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt! exact bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.WriteExpected(bytes.NewReader(content), digest); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestWriteExpectedRejectsInvalidDigest(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.WriteExpected(bytes.NewReader([]byte("hello")), "not-hex"); !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("err = %v, want ErrInvalidDigest", err)
	}
}

func TestReadVerifiedMissing(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadVerified(strings.Repeat("0", 64), 1<<20); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestReadVerifiedRoundTrip(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("verified round trip")
	digest, _, _, err := store.Write(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.ReadVerified(digest, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("got %q, want %q", got, content)
	}
}

func TestReadVerifiedRejectsOversized(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	digest, _, _, err := store.Write(bytes.NewReader([]byte("more than one byte")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadVerified(digest, 1); err == nil {
		t.Fatal("want error for over-limit read")
	}
}

func TestReadVerifiedDetectsCorruption(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	digest, _, _, err := store.Write(bytes.NewReader([]byte("original content")))
	if err != nil {
		t.Fatal(err)
	}
	path := store.Path(digest)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupted content, different length"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadVerified(digest, 1<<20); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}
