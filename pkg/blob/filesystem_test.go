package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func digestOf(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func openTestFilesystemStore(t *testing.T) *FilesystemStore {
	t.Helper()
	s, err := NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFilesystemStore: %v", err)
	}
	return s
}

func TestFilesystemStorePutThenRead(t *testing.T) {
	s := openTestFilesystemStore(t)
	content := []byte("hello blob")
	digest := digestOf(content)

	if err := s.Put(digest, content); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Read(digest, 1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("Read = %q, want %q", got, content)
	}
}

func TestFilesystemStorePutFileStreamsAndVerifies(t *testing.T) {
	s := openTestFilesystemStore(t)
	source := filepath.Join(t.TempDir(), "run-output.bin")
	content := make([]byte, 2<<20)
	for i := range content {
		content[i] = byte(i)
	}
	if err := os.WriteFile(source, content, 0o600); err != nil {
		t.Fatal(err)
	}

	digest, size, err := s.PutFile(source)
	if err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	if size != int64(len(content)) {
		t.Fatalf("PutFile size = %d, want %d", size, len(content))
	}
	if err := s.Verify(digest, size); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestFilesystemStoreKind(t *testing.T) {
	s := openTestFilesystemStore(t)
	if s.Kind() != "filesystem" {
		t.Fatalf("Kind() = %q, want filesystem", s.Kind())
	}
}

func TestFilesystemStorePutRejectsDigestMismatch(t *testing.T) {
	s := openTestFilesystemStore(t)
	wrongDigest := digestOf([]byte("something else"))

	if err := s.Put(wrongDigest, []byte("hello blob")); err == nil {
		t.Fatal("want error on digest mismatch, got nil")
	}
	if _, err := s.Read(wrongDigest, 1024); err == nil {
		t.Fatal("mismatched content must never become readable under the claimed digest")
	}
}

func TestFilesystemStoreReadMissingBlobIsErrNotFound(t *testing.T) {
	s := openTestFilesystemStore(t)
	digest := digestOf([]byte("never written"))

	if _, err := s.Read(digest, 1024); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read missing blob: got %v, want ErrNotFound", err)
	}
}

func TestFilesystemStoreReadRejectsOversizedBlob(t *testing.T) {
	s := openTestFilesystemStore(t)
	content := []byte("this content is definitely over the tiny limit")
	digest := digestOf(content)
	if err := s.Put(digest, content); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if _, err := s.Read(digest, 4); err == nil {
		t.Fatal("want error when blob exceeds maxBytes, got nil")
	}
}

func TestFilesystemStoreReadDetectsCorruption(t *testing.T) {
	s := openTestFilesystemStore(t)
	content := []byte("hello blob")
	digest := digestOf(content)
	if err := s.Put(digest, content); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Corrupt the stored bytes directly on disk, bypassing the store's
	// write path, to simulate on-disk bitrot or an operator mistake.
	path := s.cas.Path(digest)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := os.WriteFile(path, []byte("corrupted!"), 0o644); err != nil {
		t.Fatalf("corrupt stored blob: %v", err)
	}

	if _, err := s.Read(digest, 1024); err == nil {
		t.Fatal("want error reading corrupted blob, got nil")
	}
}

func TestFilesystemStorePutIsIdempotentDedup(t *testing.T) {
	s := openTestFilesystemStore(t)
	content := []byte("hello blob")
	digest := digestOf(content)

	if err := s.Put(digest, content); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := s.Put(digest, content); err != nil {
		t.Fatalf("retry Put: %v", err)
	}
	got, err := s.Read(digest, 1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("Read = %q, want %q", got, content)
	}
}

func TestNewFilesystemStoreCreatesRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "blobs")
	if _, err := NewFilesystemStore(root); err != nil {
		t.Fatalf("NewFilesystemStore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "sha256")); err != nil {
		t.Fatalf("want sha256 dir created under root: %v", err)
	}
}
