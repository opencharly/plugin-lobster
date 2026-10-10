package pluginlobster

import (
	"testing"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
	"gopkg.in/yaml.v3"
)

// TestYAMLTagsAreLoadBearing proves the `yaml:` tags in params/cue_types_gen.go are LOAD-BEARING, not
// decoration — which is what opencharly/plugin-lobster#6's drift was about.
//
// yaml.v3's default field name is the LOWERCASED Go name, so a generated field only accepts the
// authored camelCase key if it carries the tag. `ResponseSchema` has exactly that shape: the default
// would look for `responseschema`, while the schema's authored key is `responseSchema`. Removing the
// tag from the generated file makes this test fail — which is the point: 28 fields in this package
// have a tag that differs from ToLower(GoName), and none of them would resolve without it.
func TestYAMLTagsAreLoadBearing(t *testing.T) {
	var in params.LobsterInput
	const authored = "responseSchema: the-schema\n"
	if err := yaml.Unmarshal([]byte(authored), &in); err != nil {
		t.Fatalf("unmarshal %q: %v", authored, err)
	}
	if in.ResponseSchema != "the-schema" {
		t.Fatalf("the authored key `responseSchema` did not reach ResponseSchema (got %v).\n"+
			"yaml.v3 defaults a field to its lowercased Go name, so only the generated yaml: tag "+
			"makes the camelCase key resolve — without it the default is `responseschema`.",
			in.ResponseSchema)
	}
}
