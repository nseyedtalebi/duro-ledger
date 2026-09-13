// Package event defines the minimal event value Duro carries between the
// local SQLite queue and the PostgreSQL canonical store. It has no
// hash-chain fields and no event-type-specific structs: Content and Refs
// are opaque, validated JSON objects, and the boundary rules here are the
// only shape Duro enforces.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

// MaxJSONBytes bounds the size of Content and Refs, each checked
// independently.
const MaxJSONBytes = 1 << 20 // 1 MiB

// Event is one entry in the ledger: an operational envelope plus flexible
// JSON content and references.
type Event struct {
	ID         string
	OccurredAt time.Time
	EventType  string
	Actor      string
	Content    json.RawMessage
	Refs       json.RawMessage
	// ResourceURI is optional generic event-time logical resource metadata.
	// Duro preserves it exactly but never dereferences or interprets it.
	ResourceURI string
}

// New builds a validated Event. If id is empty, a UUID is generated. If
// occurredAt is the zero time, it defaults to now (UTC). Nil or empty
// content/refs default to an empty JSON object.
func New(id, eventType, actor string, occurredAt time.Time, content, refs json.RawMessage) (Event, error) {
	return NewWithResourceURI(id, eventType, actor, occurredAt, content, refs, "")
}

// NewWithResourceURI builds a validated Event with optional exact event-time
// logical resource metadata. Existing callers can keep using New.
func NewWithResourceURI(id, eventType, actor string, occurredAt time.Time, content, refs json.RawMessage, resourceURI string) (Event, error) {
	if id == "" {
		id = uuid.NewString()
	}
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	if len(content) == 0 {
		content = json.RawMessage(`{}`)
	}
	if len(refs) == 0 {
		refs = json.RawMessage(`{}`)
	}

	ev := Event{ID: id, OccurredAt: occurredAt, EventType: eventType, Actor: actor, Content: content, Refs: refs, ResourceURI: resourceURI}
	if err := ev.Validate(); err != nil {
		return Event{}, err
	}
	return ev, nil
}

// Validate checks the boundary rules: non-empty ID/EventType/Actor, and
// Content/Refs that are each a JSON object within MaxJSONBytes.
func (e Event) Validate() error {
	if e.ID == "" {
		return errors.New("event: id is required")
	}
	parsed, err := uuid.Parse(e.ID)
	if err != nil {
		return fmt.Errorf("event: id must be a UUID: %w", err)
	}
	// uuid.Parse accepts non-canonical forms (uppercase, no hyphens, urn:uuid:
	// prefix, braces). Require the exact canonical lowercase hyphenated form
	// so two textually different IDs can never refer to the same event.
	if parsed.String() != e.ID {
		return fmt.Errorf("event: id must be a canonical lowercase UUID, got %q", e.ID)
	}
	if e.EventType == "" {
		return errors.New("event: event_type is required")
	}
	if e.Actor == "" {
		return errors.New("event: actor is required")
	}
	if err := validateObjectJSON("content", e.Content); err != nil {
		return err
	}
	if err := validateObjectJSON("refs", e.Refs); err != nil {
		return err
	}
	if e.ResourceURI != "" {
		parsed, err := url.ParseRequestURI(e.ResourceURI)
		if err != nil || !parsed.IsAbs() || strings.IndexFunc(e.ResourceURI, unicode.IsSpace) >= 0 {
			return fmt.Errorf("event: resource_uri must be an absolute URI")
		}
	}
	return nil
}

// validateObjectJSON requires b to decode as a JSON object no larger than
// MaxJSONBytes. Decoding into map[string]json.RawMessage rejects malformed
// JSON, scalars, and arrays alike.
func validateObjectJSON(name string, b json.RawMessage) error {
	if len(b) > MaxJSONBytes {
		return fmt.Errorf("event: %s exceeds %d bytes", name, MaxJSONBytes)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("event: %s must be a JSON object: %w", name, err)
	}
	if obj == nil {
		// json.Unmarshal accepts the literal `null` for a map without
		// error, leaving obj nil. That is not an object.
		return fmt.Errorf("event: %s must be a JSON object, got null", name)
	}
	return nil
}
