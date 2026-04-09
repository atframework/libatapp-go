package modulev2

// Unit tests for matchesDiscoveryFilter / matchesDiscoveryFilterKey.
//
// Mirrors C++ atapp_discovery_test: metadata_filter (test case at line 43).
// The C++ test exercises all built-in metadata keys:
//   namespace_name, api_version, kind, group, service_subset, and labels.
//
// Empty-value entries in the filter map are skipped (value=="" is a wildcard).

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// makeDiscovery is a helper to build an AtappDiscovery with metadata.
func makeDiscovery(id uint64, meta *pb.AtappMetadata) *pb.AtappDiscovery {
	return &pb.AtappDiscovery{
		Id:       id,
		Metadata: meta,
	}
}

// TestMatchesDiscoveryFilter_EmptyFilter verifies that an empty filter matches
// any node including nil info (mirrors C++ empty rule set passes all).
func TestMatchesDiscoveryFilter_EmptyFilter(t *testing.T) {
	assert.True(t, matchesDiscoveryFilter(nil, nil))
	assert.True(t, matchesDiscoveryFilter(nil, map[string]string{}))
	info := makeDiscovery(1, nil)
	assert.True(t, matchesDiscoveryFilter(info, map[string]string{}))
}

// TestMatchesDiscoveryFilter_NilInfo verifies that a non-empty filter on nil
// info returns false.
func TestMatchesDiscoveryFilter_NilInfo(t *testing.T) {
	assert.False(t, matchesDiscoveryFilter(nil, map[string]string{"kind": "GameServer"}))
}

// TestMatchesDiscoveryFilter_EmptyValue verifies that a filter entry with
// value=="" is treated as a wildcard and always passes.
func TestMatchesDiscoveryFilter_EmptyValue(t *testing.T) {
	info := makeDiscovery(1, &pb.AtappMetadata{Kind: "GameServer"})
	// Empty value → skip this key → overall match
	assert.True(t, matchesDiscoveryFilter(info, map[string]string{"kind": ""}))
}

// ── Built-in key: api_version ─────────────────────────────────────────────

// TestMatchesDiscoveryFilterKey_ApiVersion_Match verifies exact api_version match.
func TestMatchesDiscoveryFilterKey_ApiVersion_Match(t *testing.T) {
	meta := &pb.AtappMetadata{ApiVersion: "v1"}
	assert.True(t, matchesDiscoveryFilterKey(meta, "api_version", "v1"))
}

func TestMatchesDiscoveryFilterKey_ApiVersion_Mismatch(t *testing.T) {
	meta := &pb.AtappMetadata{ApiVersion: "v1"}
	assert.False(t, matchesDiscoveryFilterKey(meta, "api_version", "v2"))
}

// ── Built-in key: kind ────────────────────────────────────────────────────

func TestMatchesDiscoveryFilterKey_Kind_Match(t *testing.T) {
	meta := &pb.AtappMetadata{Kind: "GameServer"}
	assert.True(t, matchesDiscoveryFilterKey(meta, "kind", "GameServer"))
}

func TestMatchesDiscoveryFilterKey_Kind_CaseSensitive(t *testing.T) {
	meta := &pb.AtappMetadata{Kind: "GameServer"}
	assert.False(t, matchesDiscoveryFilterKey(meta, "kind", "gameserver"))
}

// ── Built-in key: group ───────────────────────────────────────────────────

func TestMatchesDiscoveryFilterKey_Group_Match(t *testing.T) {
	meta := &pb.AtappMetadata{Group: "atapp"}
	assert.True(t, matchesDiscoveryFilterKey(meta, "group", "atapp"))
}

func TestMatchesDiscoveryFilterKey_Group_Mismatch(t *testing.T) {
	meta := &pb.AtappMetadata{Group: "atapp"}
	assert.False(t, matchesDiscoveryFilterKey(meta, "group", "other"))
}

// ── Built-in key: name ────────────────────────────────────────────────────

func TestMatchesDiscoveryFilterKey_Name_Match(t *testing.T) {
	meta := &pb.AtappMetadata{Name: "my-service"}
	assert.True(t, matchesDiscoveryFilterKey(meta, "name", "my-service"))
}

// ── Built-in key: namespace / namespace_name ─────────────────────────────

func TestMatchesDiscoveryFilterKey_Namespace_Match(t *testing.T) {
	meta := &pb.AtappMetadata{NamespaceName: "production"}
	assert.True(t, matchesDiscoveryFilterKey(meta, "namespace", "production"))
	assert.True(t, matchesDiscoveryFilterKey(meta, "namespace_name", "production"))
}

func TestMatchesDiscoveryFilterKey_Namespace_Mismatch(t *testing.T) {
	meta := &pb.AtappMetadata{NamespaceName: "production"}
	assert.False(t, matchesDiscoveryFilterKey(meta, "namespace_name", "staging"))
}

// ── Built-in key: uid ─────────────────────────────────────────────────────

func TestMatchesDiscoveryFilterKey_UID_Match(t *testing.T) {
	meta := &pb.AtappMetadata{Uid: "abc-123"}
	assert.True(t, matchesDiscoveryFilterKey(meta, "uid", "abc-123"))
}

func TestMatchesDiscoveryFilterKey_UID_Mismatch(t *testing.T) {
	meta := &pb.AtappMetadata{Uid: "abc-123"}
	assert.False(t, matchesDiscoveryFilterKey(meta, "uid", "xyz-999"))
}

// ── Built-in key: service_subset ─────────────────────────────────────────

func TestMatchesDiscoveryFilterKey_ServiceSubset_Match(t *testing.T) {
	meta := &pb.AtappMetadata{ServiceSubset: "canary"}
	assert.True(t, matchesDiscoveryFilterKey(meta, "service_subset", "canary"))
}

func TestMatchesDiscoveryFilterKey_ServiceSubset_Mismatch(t *testing.T) {
	meta := &pb.AtappMetadata{ServiceSubset: "canary"}
	assert.False(t, matchesDiscoveryFilterKey(meta, "service_subset", "stable"))
}

// ── Fallback: labels ──────────────────────────────────────────────────────

func TestMatchesDiscoveryFilterKey_Label_Match(t *testing.T) {
	meta := &pb.AtappMetadata{Labels: map[string]string{"env": "prod", "region": "us-east"}}
	assert.True(t, matchesDiscoveryFilterKey(meta, "env", "prod"))
	assert.True(t, matchesDiscoveryFilterKey(meta, "region", "us-east"))
}

func TestMatchesDiscoveryFilterKey_Label_Mismatch(t *testing.T) {
	meta := &pb.AtappMetadata{Labels: map[string]string{"env": "prod"}}
	assert.False(t, matchesDiscoveryFilterKey(meta, "env", "test"))
}

func TestMatchesDiscoveryFilterKey_Label_Missing(t *testing.T) {
	meta := &pb.AtappMetadata{Labels: map[string]string{"env": "prod"}}
	assert.False(t, matchesDiscoveryFilterKey(meta, "region", "us-east"))
}

func TestMatchesDiscoveryFilterKey_NilMetadata_Builtin(t *testing.T) {
	assert.False(t, matchesDiscoveryFilterKey(nil, "kind", "GameServer"))
}

func TestMatchesDiscoveryFilterKey_NilMetadata_Label(t *testing.T) {
	assert.False(t, matchesDiscoveryFilterKey(nil, "env", "prod"))
}

// ── Multi-key filter composed from both built-in and label keys ───────────

// TestMatchesDiscoveryFilter_MultiKey verifies that ALL filter keys must match
// (conjunction/AND semantics).
func TestMatchesDiscoveryFilter_MultiKey(t *testing.T) {
	info := makeDiscovery(1, &pb.AtappMetadata{
		ApiVersion:    "v1",
		Kind:          "GameServer",
		NamespaceName: "production",
		Labels:        map[string]string{"env": "prod"},
	})

	// All match → true
	assert.True(t, matchesDiscoveryFilter(info, map[string]string{
		"api_version":    "v1",
		"kind":           "GameServer",
		"namespace_name": "production",
		"env":            "prod",
	}))

	// One key mismatches → false
	assert.False(t, matchesDiscoveryFilter(info, map[string]string{
		"api_version": "v1",
		"kind":        "GameServer",
		"env":         "staging", // mismatch
	}))
}
