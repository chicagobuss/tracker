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

// add_folio_file shipped without a tags argument while create_doc had one, so
// the two document-creating surfaces disagreed about whether tags were settable
// at creation. An agent reaching for the folio one had no way to set a tag and
// no error saying so — tags passed as metadata just went inert. Assert the
// surfaces agree, so a third creation tool cannot quietly omit it either.
func TestCreationToolsAcceptTags(t *testing.T) {
	for _, name := range []string{"create_doc", "add_folio_file"} {
		tool, ok := mcpTools[name]
		if !ok {
			t.Fatalf("%s: not in mcpTools", name)
		}
		props, ok := tool.schema["properties"].(map[string]any)
		if !ok {
			t.Fatalf("%s: schema has no properties map", name)
		}
		tags, ok := props["tags"]
		if !ok {
			t.Errorf("%s: schema has no 'tags' property; tags would silently have to go in metadata, where nothing queries them", name)
			continue
		}
		spec, ok := tags.(map[string]any)
		if !ok {
			t.Errorf("%s: 'tags' is %T, want a schema object", name, tags)
			continue
		}
		if spec["type"] != "array" {
			t.Errorf("%s: 'tags' type = %v, want array", name, spec["type"])
		}
	}
}
