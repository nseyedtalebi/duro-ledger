// Package hermesmemory builds the bounded lexical projection for one Hermes
// deployment scope. Scope prevents accidental cross-profile recall; it is not
// a tenant-isolation or authorization boundary.
//
// ponytail: recall withholds results beyond 10,000 matching events because a
// later redaction can target a retained turn; upgrade with an indexed redaction
// projector or exact per-target redaction query before exposing those results.
package hermesmemory

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

const (
	maxScan      = 10000
	maxPage      = 500
	maxExcerpt   = 512
	maxQuerySize = 1024
)

var eventTypes = []string{"hermes.memory.turn", "hermes.memory.redacted"}

type Source interface {
	ListEventsByTypeAndScope([]string, string, int64, int) ([]postgres.PulledEvent, error)
}

type Result struct {
	EventID        string    `json:"event_id"`
	Sequence       int64     `json:"sequence"`
	OccurredAt     time.Time `json:"occurred_at"`
	ConversationID string    `json:"conversation_id"`
	Excerpt        string    `json:"excerpt"`
}

type RecallResponse struct {
	Results   []Result `json:"results"`
	Truncated bool     `json:"truncated"`
}

type turn struct {
	Result
	text string
}

type Projection struct {
	scope     string
	turns     []turn
	truncated bool
}

func New(scope string) (*Projection, error) {
	if scope == "" {
		return nil, fmt.Errorf("hermesmemory: scope is required")
	}
	return &Projection{scope: scope}, nil
}

// Rebuild replaces the projection from bounded canonical pages.
func (p *Projection) Rebuild(source Source) error {
	p.turns, p.truncated = nil, false
	redacted := make(map[string]struct{})
	var last int64
	scanned := 0
	for scanned < maxScan {
		limit := maxPage
		if remaining := maxScan - scanned; remaining < limit {
			limit = remaining
		}
		rows, err := source.ListEventsByTypeAndScope(eventTypes, p.scope, last, limit)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.Sequence <= last || row.BlobSHA256 != "" {
				return fmt.Errorf("hermesmemory: malformed matching event")
			}
			last = row.Sequence
			if err := row.Event.Validate(); err != nil {
				return fmt.Errorf("hermesmemory: malformed matching event")
			}
			switch row.Event.EventType {
			case "hermes.memory.turn":
				var content struct {
					Scope          string `json:"deployment_scope"`
					ConversationID string `json:"conversation_id"`
					UserText       string `json:"user_text"`
					AssistantText  string `json:"assistant_text"`
				}
				if err := exactContent(row.Event.Content, &content, []string{"deployment_scope", "conversation_id", "user_text", "assistant_text"}); err != nil || content.Scope != p.scope || !validTurn(content.ConversationID, content.UserText, content.AssistantText) {
					return fmt.Errorf("hermesmemory: malformed matching event")
				}
				p.turns = append(p.turns, turn{Result: Result{EventID: row.Event.ID, Sequence: row.Sequence, OccurredAt: row.Event.OccurredAt, ConversationID: content.ConversationID, Excerpt: excerpt(content.UserText + "\n" + content.AssistantText)}, text: content.UserText + "\n" + content.AssistantText})
			case "hermes.memory.redacted":
				var content struct {
					Scope         string `json:"deployment_scope"`
					TargetEventID string `json:"target_event_id"`
				}
				if err := exactContent(row.Event.Content, &content, []string{"deployment_scope", "target_event_id"}); err != nil || content.Scope != p.scope || !canonicalUUID(content.TargetEventID) {
					return fmt.Errorf("hermesmemory: malformed matching event")
				}
				redacted[content.TargetEventID] = struct{}{}
			default:
				return fmt.Errorf("hermesmemory: malformed matching event")
			}
		}
		scanned += len(rows)
		if len(rows) < limit {
			break
		}
	}
	if scanned == maxScan {
		rows, err := source.ListEventsByTypeAndScope(eventTypes, p.scope, last, 1)
		if err != nil {
			return err
		}
		p.truncated = len(rows) != 0
	}
	kept := p.turns[:0]
	for _, candidate := range p.turns {
		if _, hidden := redacted[candidate.EventID]; !hidden {
			kept = append(kept, candidate)
		}
	}
	p.turns = kept
	return nil
}

func (p *Projection) Recall(query string, limit int) (RecallResponse, error) {
	if !utf8.ValidString(query) || len(query) == 0 || len(query) > maxQuerySize || limit < 1 || limit > 10 {
		return RecallResponse{}, fmt.Errorf("hermesmemory: invalid recall query")
	}
	if p.truncated {
		return RecallResponse{Truncated: true}, nil
	}
	terms := words(query)
	if len(terms) == 0 {
		return RecallResponse{Truncated: p.truncated}, nil
	}
	unique := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		unique[term] = struct{}{}
	}
	type scored struct {
		turn
		distinct, occurrences int
	}
	var matches []scored
	for _, candidate := range p.turns {
		counts := make(map[string]int)
		for _, word := range words(candidate.text) {
			if _, wanted := unique[word]; wanted {
				counts[word]++
			}
		}
		if len(counts) == 0 {
			continue
		}
		s := scored{turn: candidate, distinct: len(counts)}
		for _, n := range counts {
			s.occurrences += n
		}
		matches = append(matches, s)
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].distinct != matches[j].distinct {
			return matches[i].distinct > matches[j].distinct
		}
		if matches[i].occurrences != matches[j].occurrences {
			return matches[i].occurrences > matches[j].occurrences
		}
		return matches[i].Sequence > matches[j].Sequence
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	response := RecallResponse{Truncated: p.truncated, Results: make([]Result, len(matches))}
	for i := range matches {
		response.Results[i] = matches[i].Result
	}
	return response, nil
}

func exactContent(raw json.RawMessage, destination any, keys []string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || len(object) != len(keys) {
		return fmt.Errorf("invalid content")
	}
	for _, key := range keys {
		if _, ok := object[key]; !ok {
			return fmt.Errorf("invalid content")
		}
	}
	return json.Unmarshal(raw, destination)
}

func validTurn(conversationID, userText, assistantText string) bool {
	return utf8.ValidString(conversationID) && utf8.ValidString(userText) && utf8.ValidString(assistantText) && len(conversationID) != 0 && len(conversationID) <= 4<<10 && len(userText) <= 16<<10 && len(assistantText) <= 16<<10 && (len(userText) != 0 || len(assistantText) != 0)
}
func canonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value
}
func words(text string) []string {
	var out []string
	var word strings.Builder
	flush := func() {
		if word.Len() > 0 {
			out = append(out, strings.ToLower(word.String()))
			word.Reset()
		}
	}
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			word.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return out
}
func excerpt(text string) string {
	if len(text) <= maxExcerpt {
		return text
	}
	n := maxExcerpt
	for n > 0 && !utf8.RuneStart(text[n]) {
		n--
	}
	return text[:n]
}
