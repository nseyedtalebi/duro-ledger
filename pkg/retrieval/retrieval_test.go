package retrieval

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

const testAdvisoryLockKey = 918273645

func TestProjectReplaysTextDocumentsWithCanonicalProvenance(t *testing.T) {
	dsn := os.Getenv("DURO_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("DURO_POSTGRES_TEST_DSN not set")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(`SELECT pg_advisory_lock($1)`, testAdvisoryLockKey); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	defer func() {
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
	}()
	if err := postgres.Initialize(dsn); err != nil {
		t.Fatal(err)
	}

	store, err := postgres.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := raw.Exec(`TRUNCATE events, blobs RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}

	alphaID := uuid.NewString()
	betaID := uuid.NewString()
	for _, doc := range []struct {
		id, source, body string
	}{
		{alphaID, "notes/alpha", "alpha"},
		{betaID, "notes/beta", "beta"},
	} {
		content, err := json.Marshal(map[string]string{"source": doc.source})
		if err != nil {
			t.Fatal(err)
		}
		ev, err := event.New(doc.id, "document.filed", "tester", time.Unix(1, 0), content, nil)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(doc.body))
		if outcome, _, err := store.InsertWithBlob(ev, []byte(doc.body), fmtDigest(sum), "text/plain"); err != nil || outcome != postgres.Accepted {
			t.Fatalf("InsertWithBlob(%s): outcome=%v err=%v", doc.source, outcome, err)
		}
	}

	projection, err := Project(context.Background(), store, func(_ context.Context, text string) ([]float32, error) {
		switch text {
		case "alpha":
			return []float32{1, 0}, nil
		case "beta":
			return []float32{0, 1}, nil
		default:
			t.Fatalf("unexpected embedding input %q", text)
			return nil, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := projection.Search(context.Background(), "alpha", 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []Match{
		{Sequence: 1, EventID: alphaID, BlobSHA256: fmtDigest(sha256.Sum256([]byte("alpha"))), Source: "notes/alpha", Chunk: 0},
		{Sequence: 2, EventID: betaID, BlobSHA256: fmtDigest(sha256.Sum256([]byte("beta"))), Source: "notes/beta", Chunk: 0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Search() = %#v, want %#v", got, want)
	}
}

func fmtDigest(sum [sha256.Size]byte) string {
	return fmt.Sprintf("%x", sum)
}
