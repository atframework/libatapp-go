package codec

import (
	"encoding/base64"
	"testing"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// ── Discovery ──────────────────────────────────────────────────────────────

func TestMarshalDiscoveryToJSON_ProtoFieldNames(t *testing.T) {
	discovery := &pb.AtappDiscovery{
		Id:       12345,
		TypeId:   67890,
		TypeName: "test-service",
		Identity: "unique-identity-001",
		Name:     "my-service",
	}

	jsonBytes, err := MarshalDiscoveryToJSON(discovery)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	assert.Contains(t, jsonStr, `"type_id"`)
	assert.Contains(t, jsonStr, `"type_name"`)
	assert.NotContains(t, jsonStr, `"typeId"`)
	assert.NotContains(t, jsonStr, `"typeName"`)
}

func TestMarshalDiscoveryToJSON_EmptyIdentityOmitted(t *testing.T) {
	discovery := &pb.AtappDiscovery{Id: 1, Name: "svc"}

	jsonBytes, err := MarshalDiscoveryToJSON(discovery)
	require.NoError(t, err)

	assert.NotContains(t, string(jsonBytes), `"identity"`)
}

func TestUnmarshalDiscoveryFromJSON_Basic(t *testing.T) {
	jsonStr := `{"id":12345,"name":"test-service","type_id":67890,"identity":"test-id"}`

	var discovery pb.AtappDiscovery
	require.NoError(t, UnmarshalDiscoveryFromJSON([]byte(jsonStr), &discovery))

	assert.Equal(t, uint64(12345), discovery.Id)
	assert.Equal(t, "test-service", discovery.Name)
	assert.Equal(t, uint64(67890), discovery.TypeId)
	assert.Equal(t, "test-id", discovery.Identity)
}

func TestUnmarshalDiscoveryFromJSON_DiscardUnknown(t *testing.T) {
	jsonStr := `{"id":1,"name":"svc","unknown_field":"ignored"}`

	var discovery pb.AtappDiscovery
	require.NoError(t, UnmarshalDiscoveryFromJSON([]byte(jsonStr), &discovery))
	assert.Equal(t, uint64(1), discovery.Id)
}

func TestDiscoveryRoundTrip(t *testing.T) {
	original := &pb.AtappDiscovery{
		Id:       12345,
		Name:     "test-service",
		Hostname: "host-001",
		TypeId:   67890,
		TypeName: "worker",
		Identity: "unique-id-123",
		Version:  "1.0.0",
	}

	jsonBytes, err := MarshalDiscoveryToJSON(original)
	require.NoError(t, err)

	var decoded pb.AtappDiscovery
	require.NoError(t, UnmarshalDiscoveryFromJSON(jsonBytes, &decoded))

	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Name, decoded.Name)
	assert.Equal(t, original.TypeId, decoded.TypeId)
	assert.Equal(t, original.Identity, decoded.Identity)
}

func TestUnmarshalDiscoveryFromPayload_JSONPayload(t *testing.T) {
	jsonPayload := []byte(`{"id":12345,"name":"svc-json","identity":"id-json"}`)

	var out pb.AtappDiscovery
	require.NoError(t, UnmarshalDiscoveryFromPayload(jsonPayload, &out))

	assert.Equal(t, uint64(12345), out.Id)
	assert.Equal(t, "svc-json", out.Name)
	assert.Equal(t, "id-json", out.Identity)
}

func TestUnmarshalDiscoveryFromPayload_Base64ProtoPayload(t *testing.T) {
	original := &pb.AtappDiscovery{Id: 67890, Name: "svc-b64", Identity: "id-b64"}
	rawProto, err := proto.Marshal(original)
	require.NoError(t, err)
	b64Payload := []byte(base64.StdEncoding.EncodeToString(rawProto))

	var out pb.AtappDiscovery
	require.NoError(t, UnmarshalDiscoveryFromPayload(b64Payload, &out))

	assert.Equal(t, original.Id, out.Id)
	assert.Equal(t, original.Name, out.Name)
	assert.Equal(t, original.Identity, out.Identity)
}

func TestUnmarshalDiscoveryFromPayload_InvalidData(t *testing.T) {
	var out pb.AtappDiscovery
	err := UnmarshalDiscoveryFromPayload([]byte("not-valid-json-or-base64!!!"), &out)
	assert.Error(t, err)
}

func TestUnmarshalDiscoveryFromPayload_NilMessage(t *testing.T) {
	err := UnmarshalDiscoveryFromPayload([]byte(`{"id":1}`), nil)
	assert.Error(t, err)
}

func TestDecodeDiscoveryValue_ValidJSON(t *testing.T) {
	payload := []byte(`{"id":1,"name":"svc"}`)
	result := DecodeDiscoveryValue(payload)
	require.NotNil(t, result)
	assert.Equal(t, uint64(1), result.Id)
}

func TestDecodeDiscoveryValue_EmptyInput(t *testing.T) {
	assert.Nil(t, DecodeDiscoveryValue(nil))
	assert.Nil(t, DecodeDiscoveryValue([]byte{}))
}

func TestDecodeDiscoveryValue_InvalidInput(t *testing.T) {
	assert.Nil(t, DecodeDiscoveryValue([]byte("totally-invalid")))
}

// ── Topology ──────────────────────────────────────────────────────────────

func TestMarshalTopologyToJSON_ProtoFieldNames(t *testing.T) {
	topology := &pb.AtappTopologyInfo{
		Id:   12345,
		Name: "my-topo",
	}

	jsonBytes, err := MarshalTopologyToJSON(topology)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	assert.Contains(t, jsonStr, `"name"`)
	assert.NotContains(t, jsonStr, `"typeName"`)
}

func TestTopologyRoundTrip(t *testing.T) {
	original := &pb.AtappTopologyInfo{
		Id:   999,
		Name: "topo-svc",
	}

	jsonBytes, err := MarshalTopologyToJSON(original)
	require.NoError(t, err)

	var decoded pb.AtappTopologyInfo
	require.NoError(t, UnmarshalTopologyFromJSON(jsonBytes, &decoded))

	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Name, decoded.Name)
}

func TestUnmarshalTopologyFromPayload_JSONPayload(t *testing.T) {
	jsonPayload := []byte(`{"id":555,"name":"topo-json"}`)

	var out pb.AtappTopologyInfo
	require.NoError(t, UnmarshalTopologyFromPayload(jsonPayload, &out))

	assert.Equal(t, uint64(555), out.Id)
	assert.Equal(t, "topo-json", out.Name)
}

func TestUnmarshalTopologyFromPayload_Base64ProtoPayload(t *testing.T) {
	original := &pb.AtappTopologyInfo{Id: 888, Name: "topo-b64"}
	rawProto, err := proto.Marshal(original)
	require.NoError(t, err)
	b64Payload := []byte(base64.StdEncoding.EncodeToString(rawProto))

	var out pb.AtappTopologyInfo
	require.NoError(t, UnmarshalTopologyFromPayload(b64Payload, &out))

	assert.Equal(t, original.Id, out.Id)
	assert.Equal(t, original.Name, out.Name)
}

func TestUnmarshalTopologyFromPayload_NilMessage(t *testing.T) {
	err := UnmarshalTopologyFromPayload([]byte(`{"id":1}`), nil)
	assert.Error(t, err)
}

func TestDecodeTopologyValue_ValidJSON(t *testing.T) {
	payload := []byte(`{"id":111,"name":"topo"}`)
	result := DecodeTopologyValue(payload)
	require.NotNil(t, result)
	assert.Equal(t, uint64(111), result.Id)
}

func TestDecodeTopologyValue_EmptyInput(t *testing.T) {
	assert.Nil(t, DecodeTopologyValue(nil))
	assert.Nil(t, DecodeTopologyValue([]byte{}))
}

func TestDecodeTopologyValue_InvalidInput(t *testing.T) {
	assert.Nil(t, DecodeTopologyValue([]byte("totally-invalid")))
}
