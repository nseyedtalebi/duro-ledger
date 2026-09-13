package event

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// validID is a well-formed UUID used as test fixture wherever a valid
// provided ID is needed but its exact value is not the point of the test.
const validID = "11111111-1111-1111-1111-111111111111"

func TestNewValidEventGeneratesID(t *testing.T) {
	ev, err := New("", "example.note.created", "writer", time.Time{}, json.RawMessage(`{"text":"hi"}`), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if ev.ID == "" {
		t.Fatal("expected a generated ID")
	}
	if ev.OccurredAt.IsZero() {
		t.Fatal("expected OccurredAt to default to now")
	}
}

func TestNewPreservesProvidedID(t *testing.T) {
	ev, err := New(validID, "example.note.created", "writer", time.Now(), json.RawMessage(`{}`), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if ev.ID != validID {
		t.Fatalf("want ID %s, got %s", validID, ev.ID)
	}
}

func TestNewWithResourceURIPreservesAbsoluteURI(t *testing.T) {
	const resourceURI = "cura://palace/wing/room/drawer-id"
	ev, err := NewWithResourceURI(validID, "document.filed", "writer", time.Now(), json.RawMessage(`{}`), nil, resourceURI)
	if err != nil {
		t.Fatalf("NewWithResourceURI: %v", err)
	}
	if ev.ResourceURI != resourceURI {
		t.Fatalf("resource URI = %q, want %q", ev.ResourceURI, resourceURI)
	}
}

func TestNewRejectsMalformedID(t *testing.T) {
	if _, err := New("not-a-uuid", "example.note.created", "writer", time.Now(), nil, nil); err == nil {
		t.Fatal("expected error for malformed provided id")
	}
}

func TestNewDefaultsEmptyContentAndRefsToEmptyObject(t *testing.T) {
	ev, err := New(validID, "example.note.created", "writer", time.Now(), nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if string(ev.Content) != "{}" {
		t.Fatalf("want default content {}, got %s", ev.Content)
	}
	if string(ev.Refs) != "{}" {
		t.Fatalf("want default refs {}, got %s", ev.Refs)
	}
}

func TestNewRejectsMalformedJSON(t *testing.T) {
	if _, err := New(validID, "type", "actor", time.Now(), json.RawMessage(`{"a":}`), nil); err == nil {
		t.Fatal("expected error for malformed content JSON")
	}
}

func TestNewRejectsScalarContent(t *testing.T) {
	if _, err := New(validID, "type", "actor", time.Now(), json.RawMessage(`"just a string"`), nil); err == nil {
		t.Fatal("expected error for scalar content")
	}
}

func TestNewRejectsArrayRefs(t *testing.T) {
	if _, err := New(validID, "type", "actor", time.Now(), nil, json.RawMessage(`[1,2,3]`)); err == nil {
		t.Fatal("expected error for array refs")
	}
}

func TestNewRejectsNullContent(t *testing.T) {
	if _, err := New(validID, "type", "actor", time.Now(), json.RawMessage(`null`), nil); err == nil {
		t.Fatal("expected error for null content")
	}
}

func TestNewRejectsNullRefs(t *testing.T) {
	if _, err := New(validID, "type", "actor", time.Now(), nil, json.RawMessage(`null`)); err == nil {
		t.Fatal("expected error for null refs")
	}
}

func TestNewRejectsOversizedContent(t *testing.T) {
	big := `{"pad":"` + strings.Repeat("x", MaxJSONBytes) + `"}`
	if _, err := New(validID, "type", "actor", time.Now(), json.RawMessage(big), nil); err == nil {
		t.Fatal("expected error for oversized content")
	}
}

func TestNewRequiresIDTypeAndActor(t *testing.T) {
	cases := []struct {
		name, id, typ, actor string
	}{
		{"missing type", validID, "", "actor"},
		{"missing actor", validID, "type", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.id, c.typ, c.actor, time.Now(), nil, nil); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestValidateRejectsNonCanonicalUUID(t *testing.T) {
	// uuid.Parse also accepts a bare 32 hex digit form with no hyphens as
	// the "same" UUID. Lowercasing that form doesn't add the missing
	// hyphens, so it must still be rejected: only canonical hyphenated text
	// is accepted, else two differently-spelled IDs could alias one event.
	noHyphens := strings.ReplaceAll(validID, "-", "")
	ev := Event{
		ID: noHyphens, EventType: "t", Actor: "a",
		OccurredAt: time.Now(), Content: json.RawMessage(`{}`), Refs: json.RawMessage(`{}`),
	}
	if err := ev.Validate(); err == nil {
		t.Fatal("expected error for non-hyphenated (non-canonical) UUID")
	}
}

func TestValidateRejectsUppercaseUUID(t *testing.T) {
	// uuid.Parse also accepts uppercase hex digits as the "same" UUID.
	// Lowercasing the input before comparing would let an uppercase spelling
	// alias a lowercase one; only the exact canonical spelling is accepted.
	const mixedCaseID = "1a111111-1111-1111-1111-111111111111"
	upper := strings.ToUpper(mixedCaseID)
	ev := Event{
		ID: upper, EventType: "t", Actor: "a",
		OccurredAt: time.Now(), Content: json.RawMessage(`{}`), Refs: json.RawMessage(`{}`),
	}
	if err := ev.Validate(); err == nil {
		t.Fatal("expected error for uppercase (non-canonical) UUID")
	}
}

func TestValidateRejectsEmptyID(t *testing.T) {
	ev := Event{ID: "", EventType: "t", Actor: "a", OccurredAt: time.Now(), Content: json.RawMessage(`{}`), Refs: json.RawMessage(`{}`)}
	if err := ev.Validate(); err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestValidateResourceURIRequiresAbsoluteURIAndPreservesExactValue(t *testing.T) {
	for _, resourceURI := range []string{
		"cura://palace/wing/room/drawer-id",
		"rador://deliveries/2026-09-12",
		"urn:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	} {
		ev := Event{ID: validID, EventType: "document.filed", Actor: "writer", OccurredAt: time.Now(), Content: json.RawMessage(`{}`), Refs: json.RawMessage(`{}`), ResourceURI: resourceURI}
		if err := ev.Validate(); err != nil {
			t.Fatalf("Validate(%q): %v", resourceURI, err)
		}
		if ev.ResourceURI != resourceURI {
			t.Fatalf("resource URI changed from %q to %q", resourceURI, ev.ResourceURI)
		}
	}
	for _, resourceURI := range []string{"relative/path", "/absolute/path-without-scheme", "https://example.invalid/a b", "\nhttps://example.invalid", "https://example.invalid/\x00"} {
		ev := Event{ID: validID, EventType: "document.filed", Actor: "writer", OccurredAt: time.Now(), Content: json.RawMessage(`{}`), Refs: json.RawMessage(`{}`), ResourceURI: resourceURI}
		if err := ev.Validate(); err == nil {
			t.Fatalf("Validate(%q) accepted invalid resource URI", resourceURI)
		}
	}
}
