// Package event defines the minimal event shape Duro appends to its
// PostgreSQL ledger: an event type plus two opaque JSON objects. Identity
// (id), time (received_at), and authority (actor) are database-owned and
// never set by this package; see pkg/postgres for the stored, authoritative
// form.
package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaxEventTypeBytes bounds EventType's UTF-8 byte length. EventType is
// stored verbatim in a TEXT column (no normalization occurs), so this raw
// byte count is the same quantity PostgreSQL's own CHECK constraint
// enforces.
//
// MaxJSONBytes does NOT exist here: Content/Refs are stored as JSONB, which
// PostgreSQL normalizes (canonical formatting, last-value-wins on duplicate
// keys) before the 1 MiB limit is measured. A raw byte count of the
// caller's JSON text is not the same quantity, and rejecting on it client-
// side would both wrongly reject compact-but-verbose input that normalizes
// well under the limit and wrongly accept padded input that normalizes
// over it. PostgreSQL's CHECK on octet_length(content::text) is the sole
// authority for that limit.
const MaxEventTypeBytes = 255

// New is the caller-supplied input to Append: an event type and two
// optional JSON objects. A nil Content/Refs (the field left entirely
// unset) defaults to {}. A non-nil but otherwise-empty or explicitly-null
// value is not defaulted -- it is invalid input and Validate rejects it.
type New struct {
	EventType string
	Content   json.RawMessage
	Refs      json.RawMessage
}

// Validate checks the boundary rules client-side, before any database round
// trip: nonblank (including under Unicode whitespace) EventType, valid
// UTF-8, within MaxEventTypeBytes bytes; and Content/Refs that are each nil
// (omitted, defaulting to {}) or a JSON object. It does not enforce
// Content/Refs's 1 MiB limit -- see MaxEventTypeBytes's doc comment --
// PostgreSQL is authoritative there. This exists to reject obviously-
// invalid input before opening a transaction, not to replace the database
// constraints.
func (n New) Validate() error {
	if !utf8.ValidString(n.EventType) {
		return fmt.Errorf("event: event_type must be valid UTF-8")
	}
	if strings.TrimSpace(n.EventType) == "" {
		return fmt.Errorf("event: event_type is required (nonblank)")
	}
	if len(n.EventType) > MaxEventTypeBytes {
		return fmt.Errorf("event: event_type exceeds %d UTF-8 bytes", MaxEventTypeBytes)
	}
	if err := validateObject("content", n.Content); err != nil {
		return err
	}
	if err := validateObject("refs", n.Refs); err != nil {
		return err
	}
	return nil
}

// ContentOrDefault returns n.Content, or a literal {} if it was left nil
// (omitted). A non-nil Content that is empty or explicitly null is not
// defaulted; Validate rejects it instead.
func (n New) ContentOrDefault() json.RawMessage {
	if n.Content == nil {
		return json.RawMessage(`{}`)
	}
	return n.Content
}

// RefsOrDefault returns n.Refs, or a literal {} if it was left nil
// (omitted). A non-nil Refs that is empty or explicitly null is not
// defaulted; Validate rejects it instead.
func (n New) RefsOrDefault() json.RawMessage {
	if n.Refs == nil {
		return json.RawMessage(`{}`)
	}
	return n.Refs
}

// ValidID reports whether id is a canonical hyphenated UUIDv7: 8-4-4-4-12
// hex groups (case-insensitive), version nibble 7, and the RFC 9562 variant
// (the high bits of the first nibble after the third hyphen are 10). This is
// the sole validation of a caller-supplied event id, shared by the CLI and
// pkg/postgres, so an id is rejected -- including any SQL-injection payload,
// which cannot match this shape -- before it ever reaches a query.
func ValidID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := 0; i < len(id); i++ {
		switch i {
		case 8, 13, 18, 23:
			if id[i] != '-' {
				return false
			}
		default:
			if !isHex(id[i]) {
				return false
			}
		}
	}
	if id[14] != '7' {
		return false
	}
	switch id[19] {
	case '8', '9', 'a', 'A', 'b', 'B':
	default:
		return false
	}
	return true
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// validateObject requires b to be nil (caller omitted it; the caller will
// default it) or to decode as a JSON object. An explicit JSON null, a
// non-nil empty value, a scalar, or an array is rejected.
func validateObject(name string, b json.RawMessage) error {
	if b == nil {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("event: %s must be a JSON object: %w", name, err)
	}
	if obj == nil {
		// json.Unmarshal accepts the literal `null` for a map without
		// error, leaving obj nil. Explicit null is invalid.
		return fmt.Errorf("event: %s must be a JSON object, got null", name)
	}
	return nil
}
