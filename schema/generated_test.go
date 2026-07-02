package schema

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestGeneratedMetadataMatchesMetaJSON(t *testing.T) {
	var meta struct {
		Version         int               `json:"version"`
		AgentMethods    map[string]string `json:"agentMethods"`
		ClientMethods   map[string]string `json:"clientMethods"`
		ProtocolMethods map[string]string `json:"protocolMethods"`
	}
	data, err := os.ReadFile("meta.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}
	if ProtocolVersionCurrent != meta.Version {
		t.Fatalf("ProtocolVersionCurrent = %d, want %d", ProtocolVersionCurrent, meta.Version)
	}
	assertMethods(t, "agent", AgentMethods, meta.AgentMethods)
	assertMethods(t, "client", ClientMethods, meta.ClientMethods)
	assertMethods(t, "protocol", ProtocolMethods, meta.ProtocolMethods)
}

func TestEverySchemaDefinitionHasGeneratedGoType(t *testing.T) {
	var root struct {
		Defs map[string]json.RawMessage `json:"$defs"`
	}
	data, err := os.ReadFile("schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	generated, err := os.ReadFile("generated.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(generated)
	for name := range root.Defs {
		if !strings.Contains(src, "type "+name+" ") {
			t.Fatalf("schema definition %s has no generated Go type", name)
		}
	}
}

func TestGeneratedCoreSchemaJSONShape(t *testing.T) {
	prompt := PromptRequest{SessionID: "s", Prompt: []ContentBlock{TextBlock("hello")}}
	data, err := json.Marshal(prompt)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got["sessionId"] != "s" {
		t.Fatalf("missing sessionId: %s", data)
	}
	blocks := got["prompt"].([]any)
	block := blocks[0].(map[string]any)
	if block["type"] != ContentBlockTypeText || block["text"] != "hello" {
		t.Fatalf("unexpected content block: %s", data)
	}

	var update SessionNotification
	if err := json.Unmarshal([]byte(`{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hi"}}}`), &update); err != nil {
		t.Fatal(err)
	}
	text, ok := update.Update.TextChunk()
	if !ok || text != "hi" {
		t.Fatalf("TextChunk = %q, %v", text, ok)
	}
}

func assertMethods(t *testing.T, label string, generated map[string]Method, meta map[string]string) {
	t.Helper()
	got := map[string]string{}
	for key, method := range generated {
		got[key] = string(method)
	}
	if !reflect.DeepEqual(got, meta) {
		t.Fatalf("%s methods drifted\ngot:  %#v\nwant: %#v", label, got, meta)
	}
}
