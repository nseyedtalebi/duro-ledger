// Package retrieval rebuilds a disposable semantic view of canonical text
// documents. It never writes Duro's canonical event or blob store.
package retrieval

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	chromem "github.com/philippgille/chromem-go"

	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

const replayBatchSize = 500

// Match identifies a returned retrieval chunk by its canonical provenance.
type Match struct {
	Sequence   int64
	EventID    string
	BlobSHA256 string
	Source     string
	Chunk      int
}

// Projection is an in-memory, rebuildable view of canonical text documents.
type Projection struct {
	collection *chromem.Collection
}

// Project replays canonical document.filed events into an embedded vector
// collection. The caller owns the embedding function and its model contract;
// Duro owns only ordered events, exact bodies, and their provenance.
func Project(ctx context.Context, source *postgres.Store, embed chromem.EmbeddingFunc) (*Projection, error) {
	if source == nil {
		return nil, fmt.Errorf("retrieval: nil canonical store")
	}
	if embed == nil {
		return nil, fmt.Errorf("retrieval: nil embedding function")
	}
	db := chromem.NewDB()
	collection, err := db.CreateCollection("documents", nil, embed)
	if err != nil {
		return nil, fmt.Errorf("retrieval: create collection: %w", err)
	}

	var after int64
	for {
		rows, err := source.Pull(after, replayBatchSize)
		if err != nil {
			return nil, fmt.Errorf("retrieval: pull canonical events: %w", err)
		}
		if len(rows) == 0 {
			return &Projection{collection: collection}, nil
		}
		for _, row := range rows {
			if row.Event.EventType != "document.filed" || row.BlobSHA256 == "" {
				continue
			}
			blob, ok, err := source.ReadBlob(row.BlobSHA256)
			if err != nil {
				return nil, fmt.Errorf("retrieval: read blob for event %s: %w", row.Event.ID, err)
			}
			if !ok {
				return nil, fmt.Errorf("retrieval: document event %s references missing blob %s", row.Event.ID, row.BlobSHA256)
			}
			if !strings.HasPrefix(blob.MediaType, "text/") {
				continue
			}
			if !utf8.Valid(blob.Content) {
				return nil, fmt.Errorf("retrieval: document event %s has invalid UTF-8 text blob", row.Event.ID)
			}
			var content struct {
				Source string `json:"source"`
			}
			if err := json.Unmarshal(row.Event.Content, &content); err != nil || content.Source == "" {
				return nil, fmt.Errorf("retrieval: document event %s has no source", row.Event.ID)
			}

			// ponytail: one blob is one chunk; add a versioned splitter only when
			// retrieval probes show long documents need it.
			if err := collection.Add(ctx,
				[]string{fmt.Sprintf("%d:0", row.Sequence)},
				[][]float32{nil},
				[]map[string]string{{
					"sequence":    strconv.FormatInt(row.Sequence, 10),
					"event_id":    row.Event.ID,
					"blob_sha256": row.BlobSHA256,
					"source":      content.Source,
					"chunk":       "0",
				}},
				[]string{string(blob.Content)},
			); err != nil {
				return nil, fmt.Errorf("retrieval: index event %s: %w", row.Event.ID, err)
			}
		}
		after = rows[len(rows)-1].Sequence
		if len(rows) < replayBatchSize {
			return &Projection{collection: collection}, nil
		}
	}
}

// Search returns semantic matches with their canonical event/blob provenance.
func (p *Projection) Search(ctx context.Context, query string, limit int) ([]Match, error) {
	if p == nil || p.collection == nil {
		return nil, fmt.Errorf("retrieval: nil projection")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("retrieval: limit must be positive")
	}
	results, err := p.collection.Query(ctx, query, limit, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("retrieval: query: %w", err)
	}
	matches := make([]Match, len(results))
	for i, result := range results {
		sequence, err := strconv.ParseInt(result.Metadata["sequence"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("retrieval: parse projected sequence: %w", err)
		}
		chunk, err := strconv.Atoi(result.Metadata["chunk"])
		if err != nil {
			return nil, fmt.Errorf("retrieval: parse projected chunk: %w", err)
		}
		matches[i] = Match{
			Sequence: sequence, EventID: result.Metadata["event_id"], BlobSHA256: result.Metadata["blob_sha256"],
			Source: result.Metadata["source"], Chunk: chunk,
		}
	}
	return matches, nil
}
