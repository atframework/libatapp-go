// Package codec provides protobuf JSON encoding/decoding for discovery payloads.
package codec

import (
	"encoding/base64"
	"fmt"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

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

// MarshalDiscoveryToJSON 将 AtappDiscovery 序列化为 JSON。
// 使用 proto 字段名（type_id 而非 typeId）、枚举数字且不输出多余空白。
func (Packer) MarshalDiscoveryToJSON(msg *pb.AtappDiscovery) ([]byte, error) {
	return DefaultPacker.MarshalProtoToJSON(msg)
}

// UnmarshalDiscoveryFromJSON 将 JSON 字符串反序列化为 AtappDiscovery。
// 为了前向兼容，会忽略未知字段。
func (Packer) UnmarshalDiscoveryFromJSON(data []byte, msg *pb.AtappDiscovery) error {
	return DefaultPacker.UnmarshalProtoFromJSON(data, msg)
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

// MarshalProtoToJSON 通过 DefaultPacker 将 protobuf 消息序列化为 JSON。
func MarshalProtoToJSON(msg proto.Message) ([]byte, error) {
	return DefaultPacker.MarshalProtoToJSON(msg)
}

// UnmarshalProtoFromJSON 通过 DefaultPacker 将 JSON 反序列化为 protobuf 消息。
func UnmarshalProtoFromJSON(data []byte, msg proto.Message) error {
	return DefaultPacker.UnmarshalProtoFromJSON(data, msg)
}
