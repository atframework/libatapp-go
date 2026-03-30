package codec

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// 该测试函数用于验证相关行为。
func TestMarshalDiscoveryToJSON_ProtoFieldNames(t *testing.T) {
	// Arrange
	discovery := &pb.AtappDiscovery{
		Id:       12345,
		TypeId:   67890,
		TypeName: "test-service",
		Identity: "unique-identity-001",
		Name:     "my-service",
	}

	// Act
	jsonBytes, err := MarshalDiscoveryToJSON(discovery)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	// Assert
	assert.Contains(t, jsonStr, `"type_id"`, "Should use proto field name 'type_id' not 'typeId'")
	assert.Contains(t, jsonStr, `"type_name"`, "Should use proto field name 'type_name' not 'typeName'")
	assert.NotContains(t, jsonStr, `"typeId"`, "Should NOT use camelCase 'typeId'")
	assert.NotContains(t, jsonStr, `"typeName"`, "Should NOT use camelCase 'typeName'")
}

// 该测试函数用于验证相关行为。
func TestMarshalDiscoveryToJSON_IdentityField(t *testing.T) {
	// Arrange
	discovery := &pb.AtappDiscovery{
		Id:       12345,
		Identity: "unique-identity-001",
	}

	// Act
	jsonBytes, err := MarshalDiscoveryToJSON(discovery)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	// Assert
	assert.Contains(t, jsonStr, `"identity":"unique-identity-001"`, "Should contain identity field")
}

// 该测试函数用于验证相关行为。
func TestMarshalDiscoveryToJSON_NoWhitespace(t *testing.T) {
	// Arrange
	discovery := &pb.AtappDiscovery{
		Id:       12345,
		Name:     "test",
		TypeId:   67890,
		TypeName: "service",
	}

	// Act
	jsonBytes, err := MarshalDiscoveryToJSON(discovery)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	// Assert
	assert.NotContains(t, jsonStr, "\n", "Should not contain newlines")
	assert.NotContains(t, jsonStr, "\r", "Should not contain carriage returns")
	assert.NotContains(t, jsonStr, "  ", "Should not contain multiple spaces")

	// Assert: valid compact JSON
	var temp map[string]interface{}
	err = json.Unmarshal(jsonBytes, &temp)
	require.NoError(t, err, "Should be valid JSON")
}

// 该测试函数用于验证相关行为。
func TestMarshalDiscoveryToJSON_UseEnumNumbers(t *testing.T) {
	// Arrange
	discovery := &pb.AtappDiscovery{
		Id:   12345,
		Name: "test-service",
		Area: &pb.AtappArea{
			Region:   "us-west",
			District: "zone-a",
			ZoneId:   100,
		},
	}

	// Act
	jsonBytes, err := MarshalDiscoveryToJSON(discovery)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	// Assert
	assert.Contains(t, jsonStr, `"area"`)
	assert.Contains(t, jsonStr, `"zone_id"`)

	// Assert: enum field is present in decoded JSON
	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)

	area := parsed["area"].(map[string]interface{})
	assert.Contains(t, area, "zone_id")
}

// 该测试函数用于验证相关行为。
func TestUnmarshalDiscoveryFromJSON_Basic(t *testing.T) {
	// Arrange
	jsonStr := `{"id":12345,"name":"test-service","type_id":67890,"identity":"test-id"}`

	// Act
	var discovery pb.AtappDiscovery
	err := UnmarshalDiscoveryFromJSON([]byte(jsonStr), &discovery)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, uint64(12345), discovery.Id)
	assert.Equal(t, "test-service", discovery.Name)
	assert.Equal(t, uint64(67890), discovery.TypeId)
	assert.Equal(t, "test-id", discovery.Identity)
}

// 该测试函数用于验证相关行为。
func TestUnmarshalDiscoveryFromJSON_DiscardUnknown(t *testing.T) {
	// Arrange
	jsonStr := `{
		"id": 12345,
		"name": "test-service",
		"unknown_field_1": "should be ignored",
		"unknown_field_2": 999,
		"identity": "test-id"
	}`

	// Act
	var discovery pb.AtappDiscovery
	err := UnmarshalDiscoveryFromJSON([]byte(jsonStr), &discovery)
	require.NoError(t, err, "Unknown fields should not cause error")

	// Assert
	assert.Equal(t, uint64(12345), discovery.Id)
	assert.Equal(t, "test-service", discovery.Name)
	assert.Equal(t, "test-id", discovery.Identity)
}

// 该测试函数用于验证相关行为。
func TestRoundTrip(t *testing.T) {
	// Arrange
	original := &pb.AtappDiscovery{
		Id:                      12345,
		Name:                    "test-service",
		Hostname:                "host-001",
		TypeId:                  67890,
		TypeName:                "worker",
		Identity:                "unique-id-123",
		Version:                 "1.0.0",
		AtbusProtocolVersion:    1,
		AtbusProtocolMinVersion: 0,
	}

	// Act: marshal
	jsonBytes, err := MarshalDiscoveryToJSON(original)
	require.NoError(t, err)

	// Act: unmarshal
	var decoded pb.AtappDiscovery
	err = UnmarshalDiscoveryFromJSON(jsonBytes, &decoded)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Name, decoded.Name)
	assert.Equal(t, original.Hostname, decoded.Hostname)
	assert.Equal(t, original.TypeId, decoded.TypeId)
	assert.Equal(t, original.TypeName, decoded.TypeName)
	assert.Equal(t, original.Identity, decoded.Identity)
	assert.Equal(t, original.Version, decoded.Version)
	assert.Equal(t, original.AtbusProtocolVersion, decoded.AtbusProtocolVersion)
	assert.Equal(t, original.AtbusProtocolMinVersion, decoded.AtbusProtocolMinVersion)
}

// 该测试函数用于验证相关行为。
func TestMarshalDiscoveryToJSON_EmptyIdentity(t *testing.T) {
	// Arrange
	discovery := &pb.AtappDiscovery{
		Id:   12345,
		Name: "test",
	}

	// Act
	jsonBytes, err := MarshalDiscoveryToJSON(discovery)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	// Assert
	assert.NotContains(t, jsonStr, `"identity"`, "Empty identity should be omitted with EmitUnpopulated=false")
}

// 该测试函数用于验证相关行为。
func TestMarshalDiscoveryToJSON_ComplexStructure(t *testing.T) {
	// Arrange
	discovery := &pb.AtappDiscovery{
		Id:       12345,
		Name:     "complex-service",
		Identity: "complex-id",
		Area: &pb.AtappArea{
			Region:   "us-west-1",
			District: "zone-a",
			ZoneId:   100,
		},
		Gateways: []*pb.AtappGateway{
			{
				Address: "tcp://10.0.0.1:8080",
			},
			{
				Address: "tcp://10.0.0.2:8080",
			},
		},
		Metadata: &pb.AtappMetadata{
			Labels: map[string]string{
				"env":     "prod",
				"version": "v1",
			},
		},
	}

	// Act
	jsonBytes, err := MarshalDiscoveryToJSON(discovery)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	// Assert
	assert.Contains(t, jsonStr, `"area"`)
	assert.Contains(t, jsonStr, `"gateways"`)
	assert.Contains(t, jsonStr, `"metadata"`)
	assert.Contains(t, jsonStr, `"zone_id"`)

	// Assert: nested structures are present in decoded JSON
	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)

	area := parsed["area"].(map[string]interface{})
	assert.Contains(t, area, "zone_id")
}

func BenchmarkMarshalDiscoveryToJSON(b *testing.B) {
	discovery := &pb.AtappDiscovery{
		Id:       12345,
		Name:     "benchmark-service",
		TypeId:   67890,
		TypeName: "worker",
		Identity: "bench-id",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = MarshalDiscoveryToJSON(discovery)
	}
}

func BenchmarkUnmarshalDiscoveryFromJSON(b *testing.B) {
	jsonStr := `{"id":12345,"name":"benchmark-service","type_id":67890,"type_name":"worker","identity":"bench-id"}`
	data := []byte(jsonStr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var discovery pb.AtappDiscovery
		_ = UnmarshalDiscoveryFromJSON(data, &discovery)
	}
}

func TestUnmarshalDiscoveryFromPayload_JSONAndBase64Proto(t *testing.T) {
	// Arrange: JSON payload
	jsonPayload := []byte(`{"id":12345,"name":"svc-json","identity":"id-json"}`)

	// Act
	var fromJSON pb.AtappDiscovery
	err := UnmarshalDiscoveryFromPayload(jsonPayload, &fromJSON)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, uint64(12345), fromJSON.Id)
	assert.Equal(t, "svc-json", fromJSON.Name)
	assert.Equal(t, "id-json", fromJSON.Identity)

	// Arrange: base64(proto bytes) payload
	original := &pb.AtappDiscovery{Id: 67890, Name: "svc-b64", Identity: "id-b64"}
	rawProto, err := proto.Marshal(original)
	require.NoError(t, err)
	b64Payload := []byte(base64.StdEncoding.EncodeToString(rawProto))

	// Act
	var fromBase64 pb.AtappDiscovery
	err = UnmarshalDiscoveryFromPayload(b64Payload, &fromBase64)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, original.Id, fromBase64.Id)
	assert.Equal(t, original.Name, fromBase64.Name)
	assert.Equal(t, original.Identity, fromBase64.Identity)
}
