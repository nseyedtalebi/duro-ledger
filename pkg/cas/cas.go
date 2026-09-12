// Package cas implements a content-addressed blob store: files are written
// under a directory keyed by their SHA-256 digest, deduplicated on write,
// and made read-only once stored.
package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrInvalidDigest is returned when a caller-supplied digest is not a
// well-formed lowercase 64-hex SHA-256 digest.
var ErrInvalidDigest = errors.New("cas: invalid digest")

// ErrDigestMismatch is returned by WriteExpected when the bytes read from
// src do not hash to the digest the caller declared in advance.
var ErrDigestMismatch = errors.New("cas: digest mismatch")

// ErrNotFound is returned by ReadVerified when no blob is stored under digest.
var ErrNotFound = errors.New("cas: blob not found")

// ErrCorrupt is returned by ReadVerified when the bytes on disk no longer
// match their recorded size or hash.
var ErrCorrupt = errors.New("cas: stored content failed verification")

// ValidDigest reports whether digest is a well-formed lowercase 64-hex
// SHA-256 digest.
func ValidDigest(digest string) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	for _, c := range digest {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// Store is a content-addressed store rooted at a directory on disk.
type Store struct {
	root string
}

// Open prepares (creating if needed) a content-addressed store rooted at dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "sha256"), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, ".tmp"), 0o755); err != nil {
		return nil, err
	}
	return &Store{root: dir}, nil
}

// Path returns the on-disk path a blob with the given hex digest would
// occupy, whether or not it currently exists. Two levels of 2-hex-char
// sharding (65,536 leaf directories) keep any single directory small even
// at millions of stored blobs.
func (s *Store) Path(digest string) string {
	// A caller-supplied digest may be malformed; pad rather than panic on
	// the slice below. Has/Size/Get then just report "not found" for it
	// instead of crashing.
	shard := digest
	if len(shard) < 4 {
		shard += strings.Repeat("0", 4-len(shard))
	}
	return filepath.Join(s.root, "sha256", shard[:2], shard[2:4], digest)
}

// Has reports whether a blob with the given digest is already stored.
func (s *Store) Has(digest string) bool {
	_, err := os.Stat(s.Path(digest))
	return err == nil
}

// Size returns the byte size of a stored blob.
func (s *Store) Size(digest string) (int64, error) {
	info, err := os.Stat(s.Path(digest))
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// stageTemp streams src into a fresh temp file under ROOT/.tmp, hashing as
// it goes, and syncs the temp file before returning so its bytes are
// durable on disk before any rename makes them visible under a digest path.
func (s *Store) stageTemp(src io.Reader) (tmpPath, digest string, size int64, err error) {
	tmp, err := os.CreateTemp(filepath.Join(s.root, ".tmp"), "")
	if err != nil {
		return "", "", 0, err
	}
	tmpPath = tmp.Name()

	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, h), src)
	if copyErr == nil {
		copyErr = tmp.Sync()
	}
	closeErr := tmp.Close()
	if copyErr != nil {
		return tmpPath, "", 0, copyErr
	}
	if closeErr != nil {
		return tmpPath, "", 0, closeErr
	}
	return tmpPath, hex.EncodeToString(h.Sum(nil)), n, nil
}

// commit renames a staged temp file into its digest path, deduplicating if
// a verified blob with that digest is already stored, and syncs the containing
// directory afterward so the rename survives a crash.
func (s *Store) commit(tmpPath, digest string, size int64) (bool, error) {
	if s.Has(digest) {
		content, err := s.ReadVerified(digest, size)
		if err != nil {
			return false, err
		}
		if int64(len(content)) != size {
			return false, fmt.Errorf("%w: %s stored size %d, incoming size %d", ErrCorrupt, digest, len(content), size)
		}
		return true, nil
	}
	final := s.Path(digest)
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	if err := os.Rename(tmpPath, final); err != nil {
		return false, err
	}
	_ = os.Chmod(final, 0o444) // best-effort; dedup relies on presence, not perms
	if err := syncDir(dir); err != nil {
		return false, err
	}
	return false, nil
}

// syncDir fsyncs dir so a preceding rename is durable against a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Write streams src into the store, hashing as it goes, and returns the hex
// digest and byte size. deduped is true if a blob with that digest was
// already present, in which case the existing copy is kept and src's bytes
// are discarded after hashing.
func (s *Store) Write(src io.Reader) (digest string, size int64, deduped bool, err error) {
	tmpPath, digest, size, err := s.stageTemp(src)
	defer os.Remove(tmpPath) // no-op once renamed into place
	if err != nil {
		return "", 0, false, err
	}
	deduped, err = s.commit(tmpPath, digest, size)
	if err != nil {
		return "", 0, false, err
	}
	return digest, size, deduped, nil
}

// WriteExpected is Write, but the caller declares in advance the digest src
// must hash to. Content whose computed digest disagrees is rejected with
// ErrDigestMismatch and never made visible under any digest path -- a
// caller that already knows what digest bytes are supposed to produce (a
// blob backend verifying local staging) must fail rather than silently
// storing mismatched content.
func (s *Store) WriteExpected(src io.Reader, wantDigest string) (size int64, deduped bool, err error) {
	if !ValidDigest(wantDigest) {
		return 0, false, fmt.Errorf("%w: %q", ErrInvalidDigest, wantDigest)
	}
	tmpPath, digest, size, err := s.stageTemp(src)
	defer os.Remove(tmpPath) // no-op once renamed into place
	if err != nil {
		return 0, false, err
	}
	if digest != wantDigest {
		return 0, false, fmt.Errorf("%w: want %s, computed %s", ErrDigestMismatch, wantDigest, digest)
	}
	deduped, err = s.commit(tmpPath, digest, size)
	if err != nil {
		return 0, false, err
	}
	return size, deduped, nil
}

// ReadVerified returns the bytes stored at digest, bounded by maxBytes and
// verified against the recorded size and digest before being returned, so a
// caller never receives silently corrupted canonical content.
func (s *Store) ReadVerified(digest string, maxBytes int64) ([]byte, error) {
	if !ValidDigest(digest) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, digest)
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("cas: max bytes must be positive")
	}
	info, err := os.Stat(s.Path(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if info.Size() > maxBytes {
		return nil, fmt.Errorf("cas: blob %s exceeds %d-byte read limit", digest, maxBytes)
	}
	f, err := s.Get(digest)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	content, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if int64(len(content)) != info.Size() {
		return nil, fmt.Errorf("%w: %s size changed while reading", ErrCorrupt, digest)
	}
	sum := sha256.Sum256(content)
	if hex.EncodeToString(sum[:]) != digest {
		return nil, fmt.Errorf("%w: %s content does not match digest", ErrCorrupt, digest)
	}
	return content, nil
}

// Verify streams a stored blob through SHA-256 without retaining its bytes in
// memory. It is for linking an externally staged blob into canonical metadata.
func (s *Store) Verify(digest string, size int64) error {
	if !ValidDigest(digest) {
		return fmt.Errorf("%w: %q", ErrInvalidDigest, digest)
	}
	if size < 0 {
		return fmt.Errorf("cas: size must not be negative")
	}
	info, err := os.Stat(s.Path(digest))
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if info.Size() != size {
		return fmt.Errorf("%w: %s stored size %d, expected %d", ErrCorrupt, digest, info.Size(), size)
	}
	f, err := s.Get(digest)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return fmt.Errorf("%w: %s content does not match digest", ErrCorrupt, digest)
	}
	return nil
}

// WriteFile is a convenience wrapper over Write for an on-disk source file.
func (s *Store) WriteFile(path string) (digest string, size int64, deduped bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, false, err
	}
	defer f.Close()
	return s.Write(f)
}

// Get opens a stored blob for reading.
func (s *Store) Get(digest string) (*os.File, error) {
	return os.Open(s.Path(digest))
}
