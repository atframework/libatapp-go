// Package codec provides protobuf JSON encoding/decoding for discovery and topology payloads.
package codec

import (
	"encoding/base64"
	"fmt"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Packer is the stateless codec for proto messages.
type Packer struct{}

// DefaultPacker is the shared singleton for package-level helpers.
var DefaultPacker = Packer{}

// MarshalProtoToJSON 将 protobuf 消息序列化为紧凑 JSON，并使用 proto 字段名。
func (Packer) MarshalProtoToJSON(msg proto.Message) ([]byte, error) {
	marshaler := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
		UseEnumNumbers:  true,
		Indent:          "",
	}
	return marshaler.Marshal(msg)
}

// UnmarshalProtoFromJSON 将 JSON 反序列化到 protobuf 消息，并忽略未知字段。
func (Packer) UnmarshalProtoFromJSON(data []byte, msg proto.Message) error {
	unmarshaler := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	return unmarshaler.Unmarshal(data, msg)
}

// ── Discovery ─────────────────────────────────────────────────────────────

// MarshalDiscoveryToJSON 将 AtappDiscovery 序列化为 JSON。
func (p Packer) MarshalDiscoveryToJSON(msg *pb.AtappDiscovery) ([]byte, error) {
	return p.MarshalProtoToJSON(msg)
}

// UnmarshalDiscoveryFromJSON 将 JSON 字符串反序列化为 AtappDiscovery。
func (p Packer) UnmarshalDiscoveryFromJSON(data []byte, msg *pb.AtappDiscovery) error {
	return p.UnmarshalProtoFromJSON(data, msg)
}

// UnmarshalDiscoveryFromPayload 从发现数据载荷中反序列化消息。
// 为兼容 C++，支持 protobuf-JSON 或 base64(proto-bytes) 两种格式。
func (p Packer) UnmarshalDiscoveryFromPayload(data []byte, msg *pb.AtappDiscovery) error {
	if msg == nil {
		return fmt.Errorf("target discovery message is nil")
	}
	if err := p.UnmarshalDiscoveryFromJSON(data, msg); err == nil {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return fmt.Errorf("decode discovery payload failed: %w", err)
	}
	if err := proto.Unmarshal(decoded, msg); err != nil {
		return fmt.Errorf("unmarshal discovery payload failed: %w", err)
	}
	return nil
}

// DecodeDiscoveryValue 解码发现数据载荷，输入无效时返回 nil。
func (p Packer) DecodeDiscoveryValue(raw []byte) *pb.AtappDiscovery {
	if len(raw) == 0 {
		return nil
	}
	var discovery pb.AtappDiscovery
	if err := p.UnmarshalDiscoveryFromPayload(raw, &discovery); err != nil {
		return nil
	}
	return &discovery
}

// ── Topology ──────────────────────────────────────────────────────────────

// MarshalTopologyToJSON 将 AtappTopologyInfo 序列化为 JSON。
func (p Packer) MarshalTopologyToJSON(msg *pb.AtappTopologyInfo) ([]byte, error) {
	return p.MarshalProtoToJSON(msg)
}

// UnmarshalTopologyFromJSON 将 JSON 字符串反序列化为 AtappTopologyInfo。
func (p Packer) UnmarshalTopologyFromJSON(data []byte, msg *pb.AtappTopologyInfo) error {
	return p.UnmarshalProtoFromJSON(data, msg)
}

// UnmarshalTopologyFromPayload 从拓扑数据载荷中反序列化消息。
// 为兼容 C++，支持 protobuf-JSON 或 base64(proto-bytes) 两种格式。
func (p Packer) UnmarshalTopologyFromPayload(data []byte, msg *pb.AtappTopologyInfo) error {
	if msg == nil {
		return fmt.Errorf("target topology message is nil")
	}
	if err := p.UnmarshalTopologyFromJSON(data, msg); err == nil {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return fmt.Errorf("decode topology payload failed: %w", err)
	}
	if err := proto.Unmarshal(decoded, msg); err != nil {
		return fmt.Errorf("unmarshal topology payload failed: %w", err)
	}
	return nil
}

// DecodeTopologyValue 解码拓扑数据载荷，输入无效时返回 nil。
func (p Packer) DecodeTopologyValue(raw []byte) *pb.AtappTopologyInfo {
	if len(raw) == 0 {
		return nil
	}
	var topologyInfo pb.AtappTopologyInfo
	if err := p.UnmarshalTopologyFromPayload(raw, &topologyInfo); err != nil {
		return nil
	}
	return &topologyInfo
}

// ── Package-level helpers (via DefaultPacker) ─────────────────────────────

// MarshalDiscoveryToJSON 通过 DefaultPacker 序列化发现消息。
func MarshalDiscoveryToJSON(msg *pb.AtappDiscovery) ([]byte, error) {
	return DefaultPacker.MarshalDiscoveryToJSON(msg)
}

// UnmarshalDiscoveryFromJSON 通过 DefaultPacker 反序列化发现 JSON。
func UnmarshalDiscoveryFromJSON(data []byte, msg *pb.AtappDiscovery) error {
	return DefaultPacker.UnmarshalDiscoveryFromJSON(data, msg)
}

// UnmarshalDiscoveryFromPayload 通过 DefaultPacker 反序列化发现载荷。
func UnmarshalDiscoveryFromPayload(data []byte, msg *pb.AtappDiscovery) error {
	return DefaultPacker.UnmarshalDiscoveryFromPayload(data, msg)
}

// DecodeDiscoveryValue 通过 DefaultPacker 解码发现载荷。
func DecodeDiscoveryValue(raw []byte) *pb.AtappDiscovery {
	return DefaultPacker.DecodeDiscoveryValue(raw)
}

// MarshalTopologyToJSON 通过 DefaultPacker 序列化拓扑消息。
func MarshalTopologyToJSON(msg *pb.AtappTopologyInfo) ([]byte, error) {
	return DefaultPacker.MarshalTopologyToJSON(msg)
}

// UnmarshalTopologyFromJSON 通过 DefaultPacker 反序列化拓扑 JSON。
func UnmarshalTopologyFromJSON(data []byte, msg *pb.AtappTopologyInfo) error {
	return DefaultPacker.UnmarshalTopologyFromJSON(data, msg)
}

// UnmarshalTopologyFromPayload 通过 DefaultPacker 反序列化拓扑载荷。
func UnmarshalTopologyFromPayload(data []byte, msg *pb.AtappTopologyInfo) error {
	return DefaultPacker.UnmarshalTopologyFromPayload(data, msg)
}

// DecodeTopologyValue 通过 DefaultPacker 解码拓扑载荷。
func DecodeTopologyValue(raw []byte) *pb.AtappTopologyInfo {
	return DefaultPacker.DecodeTopologyValue(raw)
}

// UnmarshalProtoFromJSON 通过 DefaultPacker 反序列化 JSON 到 proto 消息。
func UnmarshalProtoFromJSON(data []byte, msg proto.Message) error {
	return DefaultPacker.UnmarshalProtoFromJSON(data, msg)
}
