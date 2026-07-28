package main

import (
	"encoding/json"
	"testing"
)

// A nil "properties" marshals to JSON null, which strict MCP clients reject for
// the whole tools/list rather than just the offending tool — one argument-less
// tool takes down all of them. Argument-less tools are the only ones exposed to
// this, since every other schema gets a non-nil map from the workspace-override
// merge, so assert over the real table rather than a hand-built schema.
func TestToolSchemasHaveNonNullProperties(t *testing.T) {
	for name, tool := range mcpTools {
		raw, err := json.Marshal(tool.schema)
		if err != nil {
			t.Fatalf("%s: marshal schema: %v", name, err)
		}
		var sch struct {
			Properties json.RawMessage `json:"properties"`
		}
		if err := json.Unmarshal(raw, &sch); err != nil {
			t.Fatalf("%s: unmarshal schema: %v", name, err)
		}
		if string(sch.Properties) == "null" || len(sch.Properties) == 0 {
			t.Errorf("%s: inputSchema.properties is %q, want an object", name, sch.Properties)
		}
	}
}
