package postgres

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

const artifactDigest = "1c8bfe8f801d79745c4631d09fff36c82aa37fc4cce4fc946683d7b336b63032"
const otherArtifactDigest = "2c8bfe8f801d79745c4631d09fff36c82aa37fc4cce4fc946683d7b336b63032"

// TestSchemaCatalogsLocatorObservations pins the catalog's data design to the
// embedded schema: verification time lives on the existing blobs row, and
// locator observations are one many-to-one table keyed by blob digest.
func TestSchemaCatalogsLocatorObservations(t *testing.T) {
	for _, want := range []string{
		"ALTER TABLE blobs ADD COLUMN IF NOT EXISTS last_verified_at TIMESTAMPTZ",
		"CREATE TABLE IF NOT EXISTS blob_locator_observations",
		"blob_sha256 BYTEA NOT NULL REFERENCES blobs(sha256)",
		"locator     TEXT NOT NULL CHECK (locator <> '')",
		"PRIMARY KEY (blob_sha256, locator)",
	} {
		if !strings.Contains(schema, want) {
			t.Errorf("schema.sql missing %q", want)
		}
	}
}

func TestEscapeLikeLiteralNeutralizesWildcards(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"file:///data/", `file:///data/`},
		{"file:///a%b", `file:///a\%b`},
		{"file:///a_b", `file:///a\_b`},
		{"file:///a\\b", "file:///a\\\\b"},
		{"%_\\", "\\%\\_\\\\"},
	} {
		if got := escapeLikeLiteral(tc.in); got != tc.want {
			t.Errorf("escapeLikeLiteral(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestDecodeDigestRequiresCanonicalSpelling(t *testing.T) {
	if _, err := decodeDigest(artifactDigest); err != nil {
		t.Fatalf("canonical digest rejected: %v", err)
	}
	for _, bad := range []string{
		"",
		strings.ToUpper(artifactDigest),
		artifactDigest[:63],
		artifactDigest + "0",
		strings.Repeat("z", 64),
	} {
		if _, err := decodeDigest(bad); err == nil {
			t.Errorf("decodeDigest(%q) = nil error, want rejection", bad)
		}
	}
}

// TestCatalogArtifactRejectsInvalidMetadata covers the validation that must
// happen before any write, so a bad call never half-catalogs an artifact.
func TestCatalogArtifactRejectsInvalidMetadata(t *testing.T) {
	s := openTestStore(t)
	for _, tc := range []struct {
		name     string
		digest   string
		size     int64
		locators []string
	}{
		{"uppercase digest", strings.ToUpper(artifactDigest), 3, nil},
		{"negative size", artifactDigest, -1, nil},
		{"relative locator", artifactDigest, 3, []string{"data/run-1/output.bin"}},
		{"empty locator", artifactDigest, 3, []string{""}},
		{"locator with space", artifactDigest, 3, []string{"file:///a b"}},
	} {
		if _, err := s.CatalogArtifact(tc.digest, tc.size, tc.locators, time.Time{}); err == nil {
			t.Errorf("%s: CatalogArtifact = nil error, want rejection", tc.name)
		}
	}
	var cataloged int
	if err := s.db.QueryRow(`SELECT count(*) FROM blobs`).Scan(&cataloged); err != nil {
		t.Fatal(err)
	}
	if cataloged != 0 {
		t.Fatalf("rejected calls cataloged %d blobs, want 0", cataloged)
	}
}

// TestCatalogArtifactIsRetrySafe is the partial-success case: a rerun after the
// catalog row is missing re-creates it, and a rerun over a complete catalog row
// reports it was already there rather than duplicating or overwriting.
func TestCatalogArtifactIsRetrySafe(t *testing.T) {
	s := openTestStore(t)
	locator := "file:///data/run-1/output.bin"

	fresh, err := s.CatalogArtifact(artifactDigest, 11, []string{locator}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !fresh {
		t.Fatal("first CatalogArtifact reported an existing row")
	}
	var firstObserved time.Time
	if err := s.db.QueryRow(`SELECT observed_at FROM blob_locator_observations WHERE locator = $1`, locator).Scan(&firstObserved); err != nil {
		t.Fatal(err)
	}

	fresh, err = s.CatalogArtifact(artifactDigest, 11, []string{locator}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if fresh {
		t.Fatal("second CatalogArtifact reported a fresh row")
	}

	var observations int
	var secondObserved time.Time
	if err := s.db.QueryRow(`SELECT count(*), max(observed_at) FROM blob_locator_observations WHERE locator = $1`, locator).Scan(&observations, &secondObserved); err != nil {
		t.Fatal(err)
	}
	if observations != 1 {
		t.Fatalf("repeated locator produced %d observations, want 1", observations)
	}
	if !secondObserved.After(firstObserved) {
		t.Fatalf("observed_at = %s, want refresh after %s", secondObserved, firstObserved)
	}
}

// TestCatalogArtifactRejectsImmutableSizeConflict: digest and size are content
// identity. A conflicting size is an error, and the cataloged row is untouched.
func TestCatalogArtifactRejectsImmutableSizeConflict(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.CatalogArtifact(artifactDigest, 11, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CatalogArtifact(artifactDigest, 12, []string{"file:///data/conflict.bin"}, time.Time{}); err == nil {
		t.Fatal("conflicting size = nil error, want rejection")
	}
	var size int64
	var observations int
	if err := s.db.QueryRow(`SELECT b.size_bytes, count(o.locator) FROM blobs b
		LEFT JOIN blob_locator_observations o ON o.blob_sha256 = b.sha256 GROUP BY b.size_bytes`).Scan(&size, &observations); err != nil {
		t.Fatal(err)
	}
	if size != 11 || observations != 0 {
		t.Fatalf("after conflict: size %d with %d observations, want 11 with 0", size, observations)
	}
}

func TestMarkBlobVerifiedRecordsOneDigest(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.CatalogArtifact(artifactDigest, 11, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CatalogArtifact(otherArtifactDigest, 12, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	verifiedAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := s.MarkBlobVerified(artifactDigest, verifiedAt); err != nil {
		t.Fatal(err)
	}
	var stored time.Time
	if err := s.db.QueryRow(`SELECT last_verified_at FROM blobs WHERE sha256 = decode($1,'hex')`, artifactDigest).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Equal(verifiedAt) {
		t.Fatalf("last_verified_at = %s, want %s", stored, verifiedAt)
	}
	// Verification is per-artifact: the other cataloged blob is untouched.
	var otherVerified sql.NullTime
	if err := s.db.QueryRow(`SELECT last_verified_at FROM blobs WHERE sha256 = decode($1,'hex')`, otherArtifactDigest).Scan(&otherVerified); err != nil {
		t.Fatal(err)
	}
	if otherVerified.Valid {
		t.Fatalf("second artifact verified at %s, want untouched", otherVerified.Time)
	}
	if err := s.MarkBlobVerified(strings.Repeat("a", 64), verifiedAt); err == nil {
		t.Fatal("marking an uncataloged digest verified = nil error, want failure")
	}
}

// TestListLocatorsPrefixIsLiteral: the prefix is matched literally, so a
// locator containing LIKE metacharacters cannot be used to widen a query.
func TestListLocatorsPrefixIsLiteral(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.CatalogArtifact(artifactDigest, 11, []string{
		"file:///data/a%25b/kept.bin",
		"file:///data/ax25b/other.bin",
		"file:///data/a_c/kept.bin",
		"file:///data/abc/other.bin",
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		prefix string
		want   []string
	}{
		{"file:///data/a%25b/", []string{"file:///data/a%25b/kept.bin"}},
		{"file:///data/a_c/", []string{"file:///data/a_c/kept.bin"}},
		{"file:///data/a", []string{"file:///data/a%25b/kept.bin", "file:///data/a_c/kept.bin", "file:///data/abc/other.bin", "file:///data/ax25b/other.bin"}},
	} {
		got, err := s.ListLocators(tc.prefix)
		if err != nil {
			t.Fatalf("prefix %q: %v", tc.prefix, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("prefix %q matched %d locators, want %d: %+v", tc.prefix, len(got), len(tc.want), got)
		}
		for i, want := range tc.want {
			if got[i].Locator != want {
				t.Errorf("prefix %q result %d = %q, want %q", tc.prefix, i, got[i].Locator, want)
			}
			if got[i].SHA256 != artifactDigest || got[i].Size != 11 || got[i].ObservedAt.IsZero() {
				t.Errorf("prefix %q result %d = %+v, want canonical digest/size/observation", tc.prefix, i, got[i])
			}
			if got[i].LastVerifiedAt != nil {
				t.Errorf("prefix %q result %d reported verification without one", tc.prefix, i)
			}
		}
	}

	verifiedAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := s.MarkBlobVerified(artifactDigest, verifiedAt); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListLocators("")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("empty prefix matched %d locators, want all 4", len(got))
	}
	if got[0].LastVerifiedAt == nil || !got[0].LastVerifiedAt.Equal(verifiedAt) {
		t.Fatalf("last verified time = %v, want %s", got[0].LastVerifiedAt, verifiedAt)
	}
}
