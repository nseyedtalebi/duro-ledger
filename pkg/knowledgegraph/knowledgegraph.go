// Package knowledgegraph builds a disposable graph view from knowledge.fact events.
package knowledgegraph

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

const replayBatchSize = 500

type key struct {
	subject   string
	predicate string
	object    string
}

// Triple is a projected fact with its canonical provenance.
type Triple struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	Sequence  int64  `json:"sequence"`
	EventID   string `json:"event_id"`
}

// InvalidatedTriple retains the last assertion and the invalidation event for
// an otherwise hidden key.
type InvalidatedTriple struct {
	Subject       string `json:"subject"`
	Predicate     string `json:"predicate"`
	Object        string `json:"object"`
	LastAssertion Triple `json:"last_assertion"`
	Invalidation  Triple `json:"invalidation"`
}

// Projection is an in-memory, rebuildable knowledge-graph view.
type Projection struct {
	triples      map[key]Triple
	invalidated  map[key]InvalidatedTriple
	seen         map[string]struct{}
	lastSequence int64
}

// New returns an empty projection.
func New() *Projection {
	return &Projection{
		triples:     make(map[key]Triple),
		invalidated: make(map[key]InvalidatedTriple),
		seen:        make(map[string]struct{}),
	}
}

// Apply consumes canonical rows in ascending sequence order. Reapplying the
// same rows is safe; duplicate facts retain their earliest provenance.
func (p *Projection) Apply(rows []postgres.PulledEvent) error {
	if p == nil {
		return fmt.Errorf("knowledgegraph: nil projection")
	}
	if p.triples == nil {
		p.triples = make(map[key]Triple)
	}
	if p.invalidated == nil {
		p.invalidated = make(map[key]InvalidatedTriple)
	}
	if p.seen == nil {
		p.seen = make(map[string]struct{})
	}
	sequence := p.lastSequence
	for _, row := range rows {
		if _, ok := p.seen[row.Event.ID]; ok {
			continue
		}
		if row.Sequence < sequence {
			return fmt.Errorf("knowledgegraph: rows must be in ascending sequence order")
		}
		sequence = row.Sequence
	}
	for _, row := range rows {
		if _, ok := p.seen[row.Event.ID]; ok {
			continue
		}
		if row.Event.EventType != "knowledge.fact" && row.Event.EventType != "knowledge.fact.invalidated" {
			p.seen[row.Event.ID] = struct{}{}
			if row.Sequence > p.lastSequence {
				p.lastSequence = row.Sequence
			}
			continue
		}
		var fact struct {
			Subject   string `json:"subject"`
			Predicate string `json:"predicate"`
			Object    string `json:"object"`
		}
		if err := json.Unmarshal(row.Event.Content, &fact); err != nil {
			return fmt.Errorf("knowledgegraph: event %s content: %w", row.Event.ID, err)
		}
		if fact.Subject == "" || fact.Predicate == "" || fact.Object == "" {
			return fmt.Errorf("knowledgegraph: event %s requires subject, predicate, and object", row.Event.ID)
		}
		p.seen[row.Event.ID] = struct{}{}
		if row.Sequence > p.lastSequence {
			p.lastSequence = row.Sequence
		}
		k := key{fact.Subject, fact.Predicate, fact.Object}
		candidate := Triple{Subject: fact.Subject, Predicate: fact.Predicate, Object: fact.Object, Sequence: row.Sequence, EventID: row.Event.ID}
		if row.Event.EventType == "knowledge.fact.invalidated" {
			prior, ok := p.invalidated[k]
			if ok && candidate.Sequence <= prior.Invalidation.Sequence {
				continue
			}
			if visible, ok := p.triples[k]; ok {
				prior = InvalidatedTriple{Subject: fact.Subject, Predicate: fact.Predicate, Object: fact.Object, LastAssertion: visible}
			}
			prior.Invalidation = candidate
			p.invalidated[k] = prior
			delete(p.triples, k)
			continue
		}
		if prior, ok := p.invalidated[k]; ok {
			if candidate.Sequence <= prior.Invalidation.Sequence {
				continue
			}
			delete(p.invalidated, k)
		}
		if prior, ok := p.triples[k]; !ok || candidate.Sequence < prior.Sequence {
			p.triples[k] = candidate
		}
	}
	return nil
}

// Invalidated returns hidden keys with their last assertion and invalidation
// provenance. Banishment is not implemented in this projection yet.
func (p *Projection) Invalidated() []InvalidatedTriple {
	if p == nil {
		return nil
	}
	out := make([]InvalidatedTriple, 0, len(p.invalidated))
	for _, triple := range p.invalidated {
		out = append(out, triple)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Invalidation.Sequence != out[j].Invalidation.Sequence {
			return out[i].Invalidation.Sequence < out[j].Invalidation.Sequence
		}
		return out[i].Invalidation.EventID < out[j].Invalidation.EventID
	})
	return out
}

// Project replays the canonical ledger from sequence zero into a fresh view.
func Project(ctx context.Context, source *postgres.Store) (*Projection, error) {
	if source == nil {
		return nil, fmt.Errorf("knowledgegraph: nil canonical store")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	projection := New()
	var after int64
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		rows, err := source.Pull(after, replayBatchSize)
		if err != nil {
			return nil, fmt.Errorf("knowledgegraph: pull canonical events: %w", err)
		}
		if len(rows) == 0 {
			return projection, nil
		}
		if err := projection.Apply(rows); err != nil {
			return nil, err
		}
		after = rows[len(rows)-1].Sequence
		if len(rows) < replayBatchSize {
			return projection, nil
		}
	}
}

// Query returns matching triples. An empty filter field is a wildcard.
func (p *Projection) Query(subject, predicate, object string) []Triple {
	if p == nil {
		return nil
	}
	out := make([]Triple, 0, len(p.triples))
	for _, triple := range p.triples {
		if subject != "" && triple.Subject != subject {
			continue
		}
		if predicate != "" && triple.Predicate != predicate {
			continue
		}
		if object != "" && triple.Object != object {
			continue
		}
		out = append(out, triple)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence < out[j].Sequence
		}
		return out[i].EventID < out[j].EventID
	})
	return out
}
