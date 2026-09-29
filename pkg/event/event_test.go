package event

import (
	"encoding/json"
	"strings"
	"testing"
)

// unicodeSpaces is every character Go's unicode.IsSpace accepts. The
// database CHECK constraint trims exactly this set (see schema.sql), so a
// string of only these is blank on both sides of the boundary.
const unicodeSpaces = "\t\n\v\f\r                  　"

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		in      New
		wantErr string // substring; "" means valid
	}{
		{"minimal", New{EventType: "document.tagged"}, ""},
		{"objects", New{EventType: "a", Content: json.RawMessage(`{"k":1}`), Refs: json.RawMessage(`{}`)}, ""},
		{"empty type", New{}, "required"},
		{"ascii blank type", New{EventType: "   \t\n"}, "required"},
		{"unicode blank type", New{EventType: unicodeSpaces}, "required"},
		{"unicode padded type", New{EventType: " x　"}, ""},
		{"invalid utf8 type", New{EventType: string([]byte{0xff, 0xfe})}, "UTF-8"},
		{"255 bytes", New{EventType: strings.Repeat("a", 255)}, ""},
		{"256 bytes", New{EventType: strings.Repeat("a", 256)}, "255"},
		{"255 runes over 255 bytes", New{EventType: strings.Repeat("é", 200)}, "255"},
		{"content null", New{EventType: "a", Content: json.RawMessage(`null`)}, "content must be a JSON object"},
		{"content array", New{EventType: "a", Content: json.RawMessage(`[]`)}, "content must be a JSON object"},
		{"content scalar", New{EventType: "a", Content: json.RawMessage(`3`)}, "content must be a JSON object"},
		{"content empty string", New{EventType: "a", Content: json.RawMessage(``)}, "content must be a JSON object"},
		{"refs null", New{EventType: "a", Refs: json.RawMessage(`null`)}, "refs must be a JSON object"},
		{"refs scalar", New{EventType: "a", Refs: json.RawMessage(`"x"`)}, "refs must be a JSON object"},
		// Oversized JSON is NOT rejected here: jsonb normalization decides
		// that, and only PostgreSQL can measure it.
		{"huge but well-formed content", New{EventType: "a", Content: json.RawMessage(`{"k":"` + strings.Repeat("x", 2<<20) + `"}`)}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.in.Validate()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("Validate() = %v, want nil", err)
			case tt.wantErr != "" && err == nil:
				t.Fatalf("Validate() = nil, want error containing %q", tt.wantErr)
			case tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr):
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultsOnlyForNil(t *testing.T) {
	n := New{EventType: "a"}
	if got := string(n.ContentOrDefault()); got != "{}" {
		t.Errorf("ContentOrDefault() = %q, want {}", got)
	}
	if got := string(n.RefsOrDefault()); got != "{}" {
		t.Errorf("RefsOrDefault() = %q, want {}", got)
	}
	set := New{EventType: "a", Content: json.RawMessage(`{"k":1}`), Refs: json.RawMessage(`{"r":2}`)}
	if got := string(set.ContentOrDefault()); got != `{"k":1}` {
		t.Errorf("ContentOrDefault() = %q, want passthrough", got)
	}
	if got := string(set.RefsOrDefault()); got != `{"r":2}` {
		t.Errorf("RefsOrDefault() = %q, want passthrough", got)
	}
	// A non-nil empty value is not defaulted: it stays invalid input.
	empty := New{EventType: "a", Content: json.RawMessage(``)}
	if got := string(empty.ContentOrDefault()); got != "" {
		t.Errorf("ContentOrDefault() = %q, want empty (not defaulted)", got)
	}
}
