package schema

import (
	"encoding/json"
	"fmt"
)

// CurrentProtocolVersion is the ACP protocol version as a typed schema value.
const CurrentProtocolVersion ProtocolVersion = ProtocolVersion(ProtocolVersionCurrent)

// TextBlock returns a text ContentBlock.
func TextBlock(text string) ContentBlock {
	return ContentBlock{Type: ContentBlockTypeText, Text: &text}
}

// TextValue returns the text payload for a text content block.
func (b ContentBlock) TextValue() (string, bool) {
	if b.Type != ContentBlockTypeText || b.Text == nil {
		return "", false
	}
	return *b.Text, true
}

// ContentBlockValue decodes v into a ContentBlock. It accepts values produced by
// encoding/json when a schema union had to be represented as any.
func ContentBlockValue(v any) (ContentBlock, error) {
	switch v := v.(type) {
	case ContentBlock:
		return v, nil
	case *ContentBlock:
		if v == nil {
			return ContentBlock{}, fmt.Errorf("nil content block")
		}
		return *v, nil
	case json.RawMessage:
		var b ContentBlock
		if err := json.Unmarshal(v, &b); err != nil {
			return ContentBlock{}, err
		}
		return b, nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return ContentBlock{}, err
		}
		var b ContentBlock
		if err := json.Unmarshal(data, &b); err != nil {
			return ContentBlock{}, err
		}
		return b, nil
	}
}

// ContentBlock decodes a SessionUpdate content payload as a ContentBlock.
func (u SessionUpdate) ContentBlock() (ContentBlock, bool) {
	if u.Content == nil {
		return ContentBlock{}, false
	}
	b, err := ContentBlockValue(u.Content)
	return b, err == nil
}

// TextChunk returns the text from a text content chunk update.
func (u SessionUpdate) TextChunk() (string, bool) {
	switch u.SessionUpdate {
	case SessionUpdateUserMessageChunk, SessionUpdateAgentMessageChunk, SessionUpdateAgentThoughtChunk:
		b, ok := u.ContentBlock()
		if !ok {
			return "", false
		}
		return b.TextValue()
	default:
		return "", false
	}
}
