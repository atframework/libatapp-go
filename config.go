package libatapp

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	lu "github.com/atframework/atframe-utils-go/lang_utility"
	log "github.com/atframework/atframe-utils-go/log"
	atframe_protocol "github.com/atframework/libatapp-go/protocol/atframe"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

type LoadConfigOptions struct {
	ReorderListIndexByField string
}

// ============================================================================================
// Expression expansion: supports $VAR, ${VAR}, ${VAR:-default}, ${VAR:+word}, \$ escape,
// and nested expressions like ${OUTER_${INNER}} or ${OUTER:-${INNER:-default2}}.
// ============================================================================================

// maxExpressionDepth is the maximum nesting depth to prevent infinite/circular expansion.
const maxExpressionDepth = 32

// expandBracedExpression expands a single ${...} block starting after the opening '${'.
// Returns the expanded value and the number of bytes consumed (including the closing '}').
func expandBracedExpression(input string, depth int) (string, int) {
	varName := ""
	i := 0
	braceDepth := 1

	for i < len(input) && braceDepth > 0 {
		// Nested ${...} in variable name part
		if i+1 < len(input) && input[i] == '$' && input[i+1] == '{' {
			nested, consumed := expandBracedExpression(input[i+2:], depth+1)
			varName += nested
			i += 2 + consumed
			continue
		}

		if input[i] == '}' {
			braceDepth--
			if braceDepth == 0 {
				i++ // consume '}'
				// Simple variable reference: ${VAR}
				expandedName := expandExpressionImpl(varName, depth+1)
				envVal, exists := os.LookupEnv(expandedName)
				if exists {
					return envVal, i
				}
				return "", i
			}
		}

		// Check for :- or :+ operator
		if input[i] == ':' && i+1 < len(input) && (input[i+1] == '-' || input[i+1] == '+') {
			op := input[i+1]
			i += 2 // skip ':' and '-' or '+'

			// Collect the operand (which may contain nested expressions)
			operand := ""
			for i < len(input) && braceDepth > 0 {
				if i+1 < len(input) && input[i] == '$' && input[i+1] == '{' {
					nested, consumed := expandBracedExpression(input[i+2:], depth+1)
					operand += nested
					i += 2 + consumed
					continue
				}
				if input[i] == '{' {
					braceDepth++
					operand += string(input[i])
					i++
					continue
				}
				if input[i] == '}' {
					braceDepth--
					if braceDepth == 0 {
						i++ // consume '}'
						break
					}
					operand += string(input[i])
					i++
					continue
				}
				if input[i] == '\\' && i+1 < len(input) && input[i+1] == '$' {
					operand += "$"
					i += 2
					continue
				}
				operand += string(input[i])
				i++
			}

			expandedName := expandExpressionImpl(varName, depth+1)
			envVal, exists := os.LookupEnv(expandedName)

			if op == '-' {
				// ${VAR:-default}: use env value if set and non-empty, otherwise use default
				if exists && envVal != "" {
					return envVal, i
				}
				return expandExpressionImpl(operand, depth+1), i
			}
			// ${VAR:+word}: use word if env is set and non-empty, otherwise empty
			if exists && envVal != "" {
				return expandExpressionImpl(operand, depth+1), i
			}
			return "", i
		}

		varName += string(input[i])
		i++
	}

	// No matching '}' found — return literal
	return "${" + varName, i
}

// expandExpressionImpl performs recursive expression expansion on the input string.
func expandExpressionImpl(input string, depth int) string {
	if depth > maxExpressionDepth {
		return input
	}

	var result strings.Builder
	result.Grow(len(input))
	i := 0

	for i < len(input) {
		// Escaped dollar sign: \$ -> literal $
		if input[i] == '\\' && i+1 < len(input) && input[i+1] == '$' {
			result.WriteByte('$')
			i += 2
			continue
		}

		if input[i] == '$' {
			// Braced expression: ${...}
			if i+1 < len(input) && input[i+1] == '{' {
				expanded, consumed := expandBracedExpression(input[i+2:], depth)
				result.WriteString(expanded)
				i += 2 + consumed
				continue
			}

			// Unbraced variable: $variable_name (POSIX standard: [a-zA-Z_][a-zA-Z0-9_]*)
			// For extended characters (dot, hyphen, slash etc.), use the braced ${VAR} form.
			start := i + 1
			if start < len(input) && (input[start] == '_' || (input[start] >= 'a' && input[start] <= 'z') ||
				(input[start] >= 'A' && input[start] <= 'Z')) {
				end := start + 1
				for end < len(input) && (input[end] == '_' ||
					(input[end] >= 'a' && input[end] <= 'z') ||
					(input[end] >= 'A' && input[end] <= 'Z') || (input[end] >= '0' && input[end] <= '9')) {
					end++
				}

				if end > start {
					varName := input[start:end]
					envVal, exists := os.LookupEnv(varName)
					if exists {
						result.WriteString(envVal)
					}
					i = end
					continue
				}
			}

			// Lone '$' at end or followed by non-identifier char
			result.WriteByte('$')
			i++
			continue
		}

		result.WriteByte(input[i])
		i++
	}

	return result.String()
}

// ExpandExpression expands environment variable expressions in the input string.
// Supports $VAR, ${VAR}, ${VAR:-default}, ${VAR:+word}, \$ escape, and nested expressions.
func ExpandExpression(input string) string {
	return expandExpressionImpl(input, 0)
}

// expandExpressionIfEnabled expands expressions in a string value if the field
// has enable_expression set to true in its CONFIGURE field option.
func expandExpressionIfEnabled(value string, fd protoreflect.FieldDescriptor) string {
	if value == "" || fd == nil {
		return value
	}

	confMeta := proto.GetExtension(fd.Options(), atframe_protocol.E_CONFIGURE).(*atframe_protocol.AtappConfigureMeta)
	if confMeta == nil || !confMeta.GetEnableExpression() {
		return value
	}

	return ExpandExpression(value)
}

// expandExpressionForMapField expands expressions in a string value if the
// parent map field has enable_expression set to true in its CONFIGURE option.
// Map keys and values don't carry their own field options, so we check the parent.
func expandExpressionForMapField(value string, parentFd protoreflect.FieldDescriptor) string {
	if value == "" || parentFd == nil {
		return value
	}

	confMeta := proto.GetExtension(parentFd.Options(), atframe_protocol.E_CONFIGURE).(*atframe_protocol.AtappConfigureMeta)
	if confMeta == nil || !confMeta.GetEnableExpression() {
		return value
	}

	return ExpandExpression(value)
}

// skipSpace 跳过字符串中的空白字符
func skipSpace(str string) string {
	return strings.TrimSpace(str)
}

// pickNumber 解析数字并处理负数和十六进制、八进制等
func pickNumber(str string, ignoreNegative bool) (int64, string, error) {
	// 处理负号
	negative := false
	if len(str) > 0 && str[0] == '-' {
		negative = true
		str = str[1:]
		str = skipSpace(str)
	}

	var val int64
	index := 0

	base := int64(10)
	if strings.HasPrefix(str, "0x") || strings.HasPrefix(str, "0X") {
		str = str[2:]
		base = 16
	} else if strings.HasPrefix(str, "0o") || strings.HasPrefix(str, "0O") {
		str = str[2:]
		base = 8
	}

	for ; index < len(str); index++ {
		// 验证字符是否为数字
		if !unicode.IsDigit(rune(str[index])) {
			break
		}

		if base == 16 && ((str[index] >= 'a' && str[index] <= 'f') || (str[index] >= 'A' && str[index] <= 'F')) {
			// 处理十六进制字母
			if str[index] >= 'a' && str[index] <= 'f' {
				val = val*base + int64(str[index]-'a'+10)
			} else {
				val = val*base + int64(str[index]-'A'+10)
			}
			continue
		}
		// 将字符转换为数字，并构建整数值
		val = val*base + int64(str[index]-'0')
	}
	str = str[index:]

	if negative && !ignoreNegative {
		val = -val
	}
	return val, str, nil
}

// pickDuration 解析字符串并填充到 protobuf 的 Duration 结构
func pickDuration(value string) (*durationpb.Duration, error) {
	orginValue := value
	// 去除空格
	value = skipSpace(value)

	// 解析数字
	var tmVal int64
	tmVal, value, err := pickNumber(value, false)
	if err != nil {
		return nil, err
	}

	// 去除空格
	value = skipSpace(value)

	// 解析单位
	unit := strings.ToLower(value)

	duration := durationpb.Duration{}

	switch {
	case unit == "" && tmVal == 0:
		duration.Seconds = 0
	case unit == "s" || unit == "sec" || unit == "second" || unit == "seconds":
		duration.Seconds = tmVal
	case unit == "ms" || unit == "millisecond" || unit == "milliseconds":
		duration.Seconds = tmVal / 1000
		duration.Nanos = int32((tmVal % 1000) * 1000000)
	case unit == "us" || unit == "microsecond" || unit == "microseconds":
		duration.Seconds = tmVal / 1000000
		duration.Nanos = int32((tmVal % 1000000) * 1000)
	case unit == "ns" || unit == "nanosecond" || unit == "nanoseconds":
		duration.Seconds = tmVal / 1000000000
		duration.Nanos = int32(tmVal % 1000000000)
	case unit == "m" || unit == "minute" || unit == "minutes":
		duration.Seconds = tmVal * 60
	case unit == "h" || unit == "hour" || unit == "hours":
		duration.Seconds = tmVal * 3600
		duration.Nanos = int32(tmVal % 1000000000)
	case unit == "d" || unit == "day" || unit == "days":
		duration.Seconds = tmVal * 3600 * 24
	case unit == "w" || unit == "week" || unit == "weeks":
		duration.Seconds = tmVal * 3600 * 24 * 7
	default:
		return nil, fmt.Errorf("pickDuration unsupported orginValue: %s", orginValue)
	}

	return &duration, nil
}

// pickDuration 解析字符串并填充到 protobuf 的 Duration 结构
func pickSize(value string) (uint64, error) {
	// 去除空格
	value = skipSpace(value)

	// 解析数字
	var baseVal int64
	baseVal, value, err := pickNumber(value, true)
	if err != nil {
		return 0, err
	}

	// 去除空格
	value = skipSpace(value)

	// 解析单位
	unit := strings.ToLower(value)

	var val uint64

	switch {
	case unit == "b" || unit == "":
		val = uint64(baseVal)
	case unit == "kb":
		val = uint64(baseVal) * 1024
	case unit == "mb":
		val = uint64(baseVal) * 1024 * 1024
	case unit == "gb":
		val = uint64(baseVal) * 1024 * 1024 * 1024
	case unit == "pb":
		val = uint64(baseVal) * 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("pickSize unsupported unit: %s", unit)
	}

	return val, nil
}

// pickTimestamp 解析时间字符串并填充到 protobuf 的 Timestamp 结构
func pickTimestamp(value string) (*timestamppb.Timestamp, error) {
	// 去除空格
	value = skipSpace(value)

	// 解析日期时间
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t, err = time.Parse(time.DateTime, value)
		if err != nil {
			t, err = time.Parse("2006-01-02 15:04:05Z07:00", value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse timestamp: %v", err)
			}
		}
	}

	// 转换为 protobuf Timestamp
	return timestamppb.New(t), nil
}

type enumAliasMapping struct {
	origin map[string]protoreflect.EnumNumber
	noCase map[string]protoreflect.EnumNumber
}

var (
	enumAliasCache map[string]*enumAliasMapping
	enumAliasLock  sync.RWMutex
)

func getEnumAliasMapping(enumDesc protoreflect.EnumDescriptor) *enumAliasMapping {
	if enumDesc == nil {
		return nil
	}

	enumAliasLock.RLock()
	useRLock := true
	defer func() {
		if useRLock {
			enumAliasLock.RUnlock()
		} else {
			enumAliasLock.Unlock()
		}
	}()

	if enumAliasCache == nil {
		enumAliasLock.RUnlock()
		enumAliasLock.Lock()
		useRLock = false
		enumAliasCache = make(map[string]*enumAliasMapping)
	}

	enumName := string(enumDesc.FullName())
	mapping, ok := enumAliasCache[enumName]
	if ok && mapping != nil {
		return mapping
	}

	mapping = &enumAliasMapping{
		origin: make(map[string]protoreflect.EnumNumber, enumDesc.Values().Len()),
		noCase: make(map[string]protoreflect.EnumNumber, enumDesc.Values().Len()),
	}
	for i := 0; i < enumDesc.Values().Len(); i++ {
		valueDesc := enumDesc.Values().Get(i)

		if enumExt := proto.GetExtension(valueDesc.Options(), atframe_protocol.E_ENUMVALUE).(*atframe_protocol.AtappConfigureEnumvalueOptions); enumExt != nil {
			for _, alias := range enumExt.GetAliasName() {
				if alias == "" {
					continue
				}

				mapping.origin[alias] = valueDesc.Number()

				if !enumExt.GetCaseSensitive() {
					mapping.noCase[strings.ToLower(alias)] = valueDesc.Number()
				}
			}
		}
	}

	if useRLock {
		enumAliasLock.RUnlock()
		enumAliasLock.Lock()
		useRLock = false
	}
	enumAliasCache[enumName] = mapping

	return mapping
}

func pickEnumIntValue(enumDesc protoreflect.EnumDescriptor, value int32) (protoreflect.EnumNumber, error) {
	if enumDesc == nil {
		return 0, fmt.Errorf("enumDesc is nil")
	}

	ret := enumDesc.Values().ByNumber(protoreflect.EnumNumber(value))
	if ret == nil {
		return 0, fmt.Errorf("enum value %d not found in enum %s", value, enumDesc.FullName())
	}

	return ret.Number(), nil
}

func pickEnumStringValue(enumDesc protoreflect.EnumDescriptor, value string) (protoreflect.EnumNumber, error) {
	if enumDesc == nil {
		return 0, fmt.Errorf("enumDesc is nil")
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("enum value is empty for enum %s", enumDesc.FullName())
	}

	if v, err := strconv.ParseInt(value, 0, 32); err == nil {
		return pickEnumIntValue(enumDesc, int32(v))
	}

	valueDesc := enumDesc.Values().ByName(protoreflect.Name(value))
	if valueDesc != nil {
		return valueDesc.Number(), nil
	}

	aliasMapping := getEnumAliasMapping(enumDesc)
	if aliasMapping != nil {
		if enumNumber, ok := aliasMapping.origin[value]; ok {
			return enumNumber, nil
		}

		if len(aliasMapping.noCase) > 0 {
			if enumNumber, ok := aliasMapping.noCase[strings.ToLower(value)]; ok {
				return enumNumber, nil
			}
		}
	}

	return 0, fmt.Errorf("enum value %s not found in enum %s", value, enumDesc.FullName())
}

// 定义字符映射常量
const (
	SPLITCHAR = 1 << iota
	STRINGSYM
	TRANSLATE
	CMDSPLIT
)

// 字符映射数组，最大256个字符
var (
	mapValue   [256]int
	transValue [256]rune
)

func createEmptyMapKey(fd protoreflect.FieldDescriptor) protoreflect.Value {
	keyFd := fd.MapKey() // 获取 key 的 FieldDescriptor

	switch keyFd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("")
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(0)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(0)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(0)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(0)
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(false)
	}
	panic("invalid map key type")
}

func initCharSet() {
	// 如果已初始化则跳过
	if mapValue[' ']&SPLITCHAR != 0 {
		return
	}

	// 设置字符集
	mapValue[' '] = SPLITCHAR
	mapValue['\t'] = SPLITCHAR
	mapValue['\r'] = SPLITCHAR
	mapValue['\n'] = SPLITCHAR

	// 设置字符串开闭符
	mapValue['\''] = STRINGSYM
	mapValue['"'] = STRINGSYM

	// 设置转义字符
	mapValue['\\'] = TRANSLATE

	// 设置命令分隔符
	mapValue[' '] |= CMDSPLIT
	mapValue[','] = CMDSPLIT
	mapValue[';'] = CMDSPLIT

	// 初始化转义字符
	for i := 0; i < 256; i++ {
		transValue[i] = rune(i)
	}

	// 常见转义字符设置
	transValue['0'] = '\x00'
	transValue['a'] = '\a'
	transValue['b'] = '\b'
	transValue['f'] = '\f'
	transValue['r'] = '\r'
	transValue['n'] = '\n'
	transValue['t'] = '\t'
	transValue['v'] = '\v'
	transValue['\\'] = '\\'
	transValue['\''] = '\''
	transValue['"'] = '"'
}

// getSegment 函数：解析字符串并返回下一个段落
func getSegment(beginStr string) (string, string) {
	initCharSet()

	var val strings.Builder
	var flag rune

	// 去除分隔符前缀
	beginStr = strings.TrimLeftFunc(beginStr, func(r rune) bool {
		return (mapValue[r]&SPLITCHAR != 0)
	})

	i := 0
	for i < len(beginStr) {
		ch := rune(beginStr[i])

		if mapValue[ch]&SPLITCHAR != 0 {
			break
		}

		if mapValue[ch]&STRINGSYM != 0 {
			flag = ch
			i++

			// 处理转义字符
			for i < len(beginStr) {
				ch = rune(beginStr[i])
				if ch == flag {
					break
				}
				if mapValue[ch]&TRANSLATE != 0 && i+1 < len(beginStr) {
					i++
					ch = transValue[rune(beginStr[i])]
				}
				val.WriteRune(ch)
				i++
			}
			i++ // 跳过结束的 flag 字符
			break
		} else {
			val.WriteRune(ch)
			i++
		}
	}

	i = max(i, len(val.String()))
	// 去除分隔符后缀
	beginStr = strings.TrimLeftFunc(beginStr[i:], func(r rune) bool {
		return (mapValue[r]&SPLITCHAR != 0)
	})

	return val.String(), beginStr
}

func splitStringToArray(start string) (result []string) {
	result = make([]string, 0)
	for len(start) > 0 {
		splitedVal, next := getSegment(start)
		splitedVal = strings.TrimSpace(splitedVal)
		if splitedVal == "" {
			start = next
			continue
		}
		result = append(result, splitedVal)
		start = next
	}
	return
}

func parseStringToYamlData(stringValue string, fd protoreflect.FieldDescriptor, sizeMode bool, logger *log.Logger) (interface{}, error) {
	if sizeMode {
		return stringValue, nil
	}
	switch fd.Kind() {
	case protoreflect.MessageKind:
		return stringValue, nil
	case protoreflect.BoolKind:
		v, err := strconv.ParseBool(stringValue)
		if err == nil {
			return v, nil
		}
		return nil, fmt.Errorf("expected bool, got %s err %s", stringValue, err)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind:
		var v int64
		var err error
		if strings.HasPrefix(stringValue, "0x") || strings.HasPrefix(stringValue, "0X") {
			v, err = strconv.ParseInt(stringValue, 16, 32)
		} else if strings.HasPrefix(stringValue, "0o") || strings.HasPrefix(stringValue, "0O") {
			v, err = strconv.ParseInt(stringValue[2:], 8, 32)
		} else {
			v, err = strconv.ParseInt(stringValue, 10, 32)
		}
		if err == nil {
			return int32(v), nil
		}
		return nil, fmt.Errorf("expected int32, got %s err %s", stringValue, err)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind:
		var v int64
		var err error
		if strings.HasPrefix(stringValue, "0x") || strings.HasPrefix(stringValue, "0X") {
			v, err = strconv.ParseInt(stringValue, 16, 64)
		} else if strings.HasPrefix(stringValue, "0o") || strings.HasPrefix(stringValue, "0O") {
			v, err = strconv.ParseInt(stringValue[2:], 8, 64)
		} else {
			v, err = strconv.ParseInt(stringValue, 10, 64)
		}
		if err == nil {
			return int64(v), nil
		}
		return nil, fmt.Errorf("expected int64, got %s err %s", stringValue, err)
	case protoreflect.Uint32Kind:
		var v uint64
		var err error
		if strings.HasPrefix(stringValue, "0x") || strings.HasPrefix(stringValue, "0X") {
			v, err = strconv.ParseUint(stringValue, 16, 32)
		} else if strings.HasPrefix(stringValue, "0o") || strings.HasPrefix(stringValue, "0O") {
			v, err = strconv.ParseUint(stringValue[2:], 8, 32)
		} else {
			v, err = strconv.ParseUint(stringValue, 10, 32)
		}
		if err == nil {
			return uint32(v), nil
		}
		return nil, fmt.Errorf("expected uint32, got %s err %s", stringValue, err)
	case protoreflect.Uint64Kind:
		var v uint64
		var err error
		if strings.HasPrefix(stringValue, "0x") || strings.HasPrefix(stringValue, "0X") {
			v, err = strconv.ParseUint(stringValue, 16, 64)
		} else if strings.HasPrefix(stringValue, "0o") || strings.HasPrefix(stringValue, "0O") {
			v, err = strconv.ParseUint(stringValue[2:], 8, 64)
		} else {
			v, err = strconv.ParseUint(stringValue, 10, 64)
		}
		if err == nil {
			return uint64(v), nil
		}
		return nil, fmt.Errorf("expected uint64, got %s err %s", stringValue, err)
	case protoreflect.StringKind:
		return stringValue, nil
	case protoreflect.FloatKind:
		v, err := strconv.ParseFloat(stringValue, 32)
		if err == nil {
			return float32(v), nil
		}
		return nil, fmt.Errorf("expected float32, got %s err %s", stringValue, err)
	case protoreflect.DoubleKind:
		v, err := strconv.ParseFloat(stringValue, 64)
		if err == nil {
			return float64(v), nil
		}
		return nil, fmt.Errorf("expected float64, got %s err %s", stringValue, err)
	case protoreflect.EnumKind:
		return pickEnumStringValue(fd.Enum(), stringValue)
	}
	return nil, fmt.Errorf("parseDefaultToYamlData unsupported field type: %v", fd.Kind())
}

func convertToInt64(data interface{}) (int64, error) {
	switch reflect.ValueOf(data).Kind() {
	case reflect.Int:
		return int64(reflect.ValueOf(data).Int()), nil // 转换为 int64
	case reflect.Int32:
		return int64(reflect.ValueOf(data).Int()), nil // 转换为 int64
	case reflect.Int64:
		return int64(reflect.ValueOf(data).Int()), nil // 转换为 int64
	case reflect.Uint:
		return int64(reflect.ValueOf(data).Uint()), nil // 转换为 int64
	case reflect.Uint32:
		return int64(reflect.ValueOf(data).Uint()), nil // 转换为 int64
	case reflect.Uint64:
		return int64(reflect.ValueOf(data).Uint()), nil // 转换为 int64
	case reflect.Bool:
		if reflect.ValueOf(data).Bool() {
			return 1, nil
		} else {
			return 0, nil
		}
	case reflect.Float32:
		return int64(reflect.ValueOf(data).Float()), nil
	case reflect.Float64:
		return int64(reflect.ValueOf(data).Float()), nil
	case reflect.String:
		value, _, err := pickNumber(reflect.ValueOf(data).String(), false)
		if err != nil {
			return 0, fmt.Errorf("convertToInt64 failed pickNumber failed %v, error: %s", data, err)
		}
		return value, nil
	}
	if v, ok := data.(*timestamppb.Timestamp); ok {
		return v.Seconds*1000000000 + int64(v.Nanos), nil
	}
	if v, ok := data.(*durationpb.Duration); ok {
		return v.Seconds*1000000000 + int64(v.Nanos), nil
	}
	return 0, fmt.Errorf("convertToInt64 failed Type not found: %T", data)
}

func tryLoadListIndex(data interface{}) int {
	if data == nil {
		return -1
	}

	switch reflect.ValueOf(data).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(reflect.ValueOf(data).Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int(reflect.ValueOf(data).Uint())
	case reflect.Bool:
		if reflect.ValueOf(data).Bool() {
			return 1
		} else {
			return 0
		}
	case reflect.Float32, reflect.Float64:
		return int(reflect.ValueOf(data).Float())
	case reflect.String:
		strValue := reflect.ValueOf(data).String()
		if strValue == "" {
			return -1
		}
		value, _, err := pickNumber(reflect.ValueOf(data).String(), false)
		if err != nil {
			return -1
		}

		return int(value)
	}
	return -1
}

func checkMinMax(yamlData interface{}, minData interface{}, maxData interface{}) (interface{}, error) {
	yamlDataNative := yamlData
	minDataNative := minData
	maxDataNative := maxData

	returnNative := yamlDataNative

	var err error
	if yamlData != nil {
		yamlData, err = convertToInt64(yamlData)
		if err != nil {
			return nil, err
		}
	}
	if minData != nil {
		minData, err = convertToInt64(minData)
		if err != nil {
			return nil, err
		}
	}
	if maxData != nil {
		maxData, err = convertToInt64(maxData)
		if err != nil {
			return nil, err
		}
	}

	// 选出最终值
	if yamlData == nil {
		yamlData = minData
		returnNative = minDataNative
	} else if minData != nil {
		// 对比
		yamlDataV, ok := yamlData.(int64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("convertField Check yamlData expected Int64, got %T", yamlData)
		}
		minDataV, ok := minData.(int64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("convertField Check MinValue expected Int64, got %T", minData)
		}

		if minDataV > yamlDataV {
			returnNative = minDataNative
		}
	}

	if yamlData == nil {
		yamlData = maxData
		returnNative = maxDataNative
	} else if maxData != nil {
		// 对比
		yamlDataV, ok := yamlData.(int64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("convertField Check yamlData expected Int64, got %T", yamlData)
		}
		maxDataV, ok := maxData.(int64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("convertField Check maxDataV expected Int64, got %T", maxData)
		}

		if yamlDataV > maxDataV {
			returnNative = maxDataNative
		}
	}

	return returnNative, nil
}

func convertField(inputData interface{}, minData interface{}, maxData interface{}, fd protoreflect.FieldDescriptor, logger *log.Logger) (protoreflect.Value, error) {
	if inputData == nil && minData == nil && maxData == nil {
		return protoreflect.Value{}, nil
	}

	inputData, err := checkMinMax(inputData, minData, maxData)
	if err != nil {
		return protoreflect.Value{}, err
	}

	// 更新最终值
	switch fd.Kind() {
	case protoreflect.BoolKind:
		if v, ok := inputData.(bool); ok {
			return protoreflect.ValueOfBool(v), nil
		}

		if v, ok := inputData.(string); ok {
			bv, err := strconv.ParseBool(v)
			if err != nil {
				return protoreflect.Value{}, fmt.Errorf("expected bool, got string %s", v)
			}

			return protoreflect.ValueOfBool(bv), nil
		}

		v, err := convertToInt64(inputData)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("expected bool, got %T", inputData)
		}

		return protoreflect.ValueOfBool(v != 0), nil
	case protoreflect.Int32Kind:
		v, err := convertToInt64(inputData)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(v)), nil
	case protoreflect.Sint32Kind:
		v, err := convertToInt64(inputData)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(v)), nil
	case protoreflect.Int64Kind:
		v, err := convertToInt64(inputData)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(int64(v)), nil
	case protoreflect.Sint64Kind:
		v, err := convertToInt64(inputData)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(int64(v)), nil
	case protoreflect.Uint32Kind:
		v, err := convertToInt64(inputData)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint32(uint32(v)), nil
	case protoreflect.Uint64Kind:
		v, err := convertToInt64(inputData)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint64(uint64(v)), nil
	case protoreflect.StringKind:
		if v, ok := inputData.(string); ok {
			return protoreflect.ValueOfString(v), nil
		}
		// Convert non-string types (e.g., int from YAML) to string
		return protoreflect.ValueOfString(fmt.Sprintf("%v", inputData)), nil

	case protoreflect.FloatKind:
		if v, ok := inputData.(float32); ok {
			return protoreflect.ValueOfFloat32(v), nil
		}
		if v, ok := inputData.(float64); ok {
			return protoreflect.ValueOfFloat64(v), nil
		}
		if v, ok := inputData.(string); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err == nil {
				return protoreflect.ValueOfFloat64(f), nil
			}
		}
		return protoreflect.Value{}, fmt.Errorf("expected float32, got %T", inputData)

	case protoreflect.MessageKind:
		if v, ok := inputData.(*timestamppb.Timestamp); ok {
			return protoreflect.ValueOfMessage(v.ProtoReflect()), nil
		}
		if v, ok := inputData.(*durationpb.Duration); ok {
			return protoreflect.ValueOfMessage(v.ProtoReflect()), nil
		}
		return protoreflect.Value{}, fmt.Errorf("expected Timestamp or Duration, got %T", inputData)

	case protoreflect.EnumKind:
		if v, ok := inputData.(protoreflect.EnumNumber); ok {
			return protoreflect.ValueOfEnum(v), nil
		}
		if v, ok := inputData.(string); ok {
			enumNumber, err := pickEnumStringValue(fd.Enum(), v)
			if err != nil {
				return protoreflect.Value{}, err
			}
			return protoreflect.ValueOfEnum(enumNumber), nil
		}
		v, err := convertToInt64(inputData)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("can not convert %v to enum %s", inputData, fd.Enum().FullName())
		}

		enumNumber, err := pickEnumIntValue(fd.Enum(), int32(v))
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfEnum(enumNumber), nil
	}

	return protoreflect.Value{}, fmt.Errorf("unsupported field type: %v", fd.Kind())
}

func pickSizeMode(value interface{}) (uint64, error) {
	switch reflect.ValueOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v := reflect.ValueOf(value).Int()
		return uint64(v), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v := reflect.ValueOf(value).Uint()
		return uint64(v), nil
	case reflect.String:
		return pickSize(value.(string))
	default:
		return 0, fmt.Errorf("SizeMode true expected String or int, got %T", value)
	}
}

// 从一个Field内读出数据 非Message 且为最底层 嵌套终点
func parseField(inputData interface{}, fd protoreflect.FieldDescriptor, logger *log.Logger) (protoreflect.Value, error) {
	// Apply expression expansion if enable_expression is set on this field
	if inputData != nil {
		if strVal, ok := inputData.(string); ok && strVal != "" {
			inputData = expandExpressionIfEnabled(strVal, fd)
		}
	}

	// 获取最大最小值
	var minValue interface{}
	var maxValue interface{}

	if confMeta := proto.GetExtension(fd.Options(), atframe_protocol.E_CONFIGURE).(*atframe_protocol.AtappConfigureMeta); confMeta != nil {
		// 取出极值
		if confMeta.MinValue != "" {
			var err error
			minValue, err = parseStringToYamlData(confMeta.MinValue, fd, confMeta.SizeMode, logger)
			if err != nil {
				return protoreflect.Value{}, err
			}
		}
		if confMeta.MaxValue != "" {
			var err error
			maxValue, err = parseStringToYamlData(confMeta.MaxValue, fd, confMeta.SizeMode, logger)
			if err != nil {
				return protoreflect.Value{}, err
			}
		}

		// 转换值
		if confMeta.SizeMode {
			// 需要从String 转为 Int
			if inputData != nil {
				// 基础
				var err error
				inputData, err = pickSizeMode(inputData)
				if err != nil {
					return protoreflect.Value{}, err
				}
			}
			if minValue != nil {
				// 最小
				var err error
				minValue, err = pickSizeMode(minValue)
				if err != nil {
					return protoreflect.Value{}, err
				}
			}
			if maxValue != nil {
				// 最大
				var err error
				maxValue, err = pickSizeMode(maxValue)
				if err != nil {
					return protoreflect.Value{}, err
				}
			}
		}
	}

	if fd.Kind() == protoreflect.MessageKind {
		// 转换值
		if fd.Message().FullName() == proto.MessageName(&durationpb.Duration{}) {
			if inputData != nil {
				v, ok := inputData.(string)
				if !ok {
					return protoreflect.Value{}, fmt.Errorf("duration expected string, got %T", inputData)
				}
				duration, err := pickDuration(v)
				if err != nil {
					return protoreflect.Value{}, err
				}
				inputData = duration
			}
			if minValue != nil {
				v, ok := minValue.(string)
				if !ok {
					return protoreflect.Value{}, fmt.Errorf("duration expected string, got %T", minValue)
				}
				duration, err := pickDuration(v)
				if err != nil {
					return protoreflect.Value{}, err
				}
				minValue = duration
			}
			if maxValue != nil {
				v, ok := maxValue.(string)
				if !ok {
					return protoreflect.Value{}, fmt.Errorf("duration expected string, got %T", maxValue)
				}
				duration, err := pickDuration(v)
				if err != nil {
					return protoreflect.Value{}, err
				}
				maxValue = duration
			}
		} else if fd.Message().FullName() == proto.MessageName(&timestamppb.Timestamp{}) {
			if inputData != nil {
				v, ok := inputData.(string)
				if !ok {
					return protoreflect.Value{}, fmt.Errorf("timestamp expected string, got %T", inputData)
				}
				timestamp, err := pickTimestamp(v)
				if err != nil {
					return protoreflect.Value{}, err
				}
				inputData = timestamp
			}
			if minValue != nil {
				v, ok := minValue.(string)
				if !ok {
					return protoreflect.Value{}, fmt.Errorf("timestamp expected string, got %T", minValue)
				}
				timestamp, err := pickTimestamp(v)
				if err != nil {
					return protoreflect.Value{}, err
				}
				minValue = timestamp
			}
			if maxValue != nil {
				v, ok := maxValue.(string)
				if !ok {
					return protoreflect.Value{}, fmt.Errorf("timestamp expected string, got %T", maxValue)
				}
				timestamp, err := pickTimestamp(v)
				if err != nil {
					return protoreflect.Value{}, err
				}
				maxValue = timestamp
			}
		} else {
			return protoreflect.Value{}, fmt.Errorf("%s expected Duration or Timestamp, got %T", fd.FullName(), inputData)
		}
	}

	return convertField(inputData, minValue, maxValue, fd, logger)
}

type ConfigExistedIndex struct {
	ExistedSet  map[string]struct{}
	MapKeyIndex map[string]int
}

func (i *ConfigExistedIndex) MutableExistedSet() map[string]struct{} {
	if i.ExistedSet == nil {
		i.ExistedSet = make(map[string]struct{})
	}

	return i.ExistedSet
}

func (i *ConfigExistedIndex) MutableMapKeyIndex() map[string]int {
	if i.MapKeyIndex == nil {
		i.MapKeyIndex = make(map[string]int)
	}

	return i.MapKeyIndex
}

func CreateConfigExistedIndex() *ConfigExistedIndex {
	return &ConfigExistedIndex{
		ExistedSet:  make(map[string]struct{}),
		MapKeyIndex: make(map[string]int),
	}
}

func makeExistedMapKeyIndexKey(existedSetPrefix string, fd protoreflect.FieldDescriptor, mk protoreflect.MapKey) string {
	keyFd := fd.MapKey()

	switch keyFd.Kind() {
	case protoreflect.BoolKind:
		if mk.Bool() {
			return fmt.Sprintf("%s%s.1", existedSetPrefix, fd.Name())
		} else {
			return fmt.Sprintf("%s%s.0", existedSetPrefix, fd.Name())
		}
	case protoreflect.StringKind:
		return fmt.Sprintf("%s%s.%s", existedSetPrefix, fd.Name(), mk.String())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind:
		return fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), mk.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind:
		return fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), mk.Int())
	case protoreflect.Uint32Kind:
		return fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), mk.Uint())
	case protoreflect.Uint64Kind:
		return fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), mk.Uint())
	}
	return fmt.Sprintf("%s%s.%s", existedSetPrefix, fd.FullName(), mk.String())
}

func ParsePlainMessage(yamlData map[string]interface{}, msg proto.Message, logger *log.Logger) error {
	len := msg.ProtoReflect().Descriptor().Fields().Len()
	for i := 0; i < len; i++ {
		fd := msg.ProtoReflect().Descriptor().Fields().Get(i)
		fieldName := string(fd.Name())
		fieldMatch := false

		if confMeta := proto.GetExtension(fd.Options(), atframe_protocol.E_CONFIGURE).(*atframe_protocol.AtappConfigureMeta); confMeta != nil {
			if confMeta.FieldMatch != nil && confMeta.FieldMatch.FieldName != "" && confMeta.FieldMatch.FieldValue != "" {
				// 存在跳过规则
				if value, ok := yamlData[confMeta.FieldMatch.FieldName].(string); ok {
					// 存在
					if value == confMeta.FieldMatch.FieldValue {
						fieldMatch = true
					} else {
						continue
					}
				}
			}
		}

		if fd.IsMap() {
			if yamlData == nil {
				continue
			}
			innerMap, ok := yamlData[fieldName].(map[string]interface{})
			if !ok {
				// Try JSON name
				jsonName := fd.JSONName()
				innerMap, ok = yamlData[string(jsonName)].(map[string]interface{})
			}
			if ok {
				// 这边需要循环Value, 对map字段的key和value应用表达式展开
				for k, v := range innerMap {
					// Expand expressions on map key (string) if parent field has enable_expression
					expandedKey := expandExpressionForMapField(k, fd)
					keyValue, err := parseField(expandedKey, fd.MapKey(), logger)
					if err != nil {
						return err
					}
					// Expand expressions on map value (non-message) if parent field has enable_expression
					expandedVal := v
					if strVal, ok := v.(string); ok {
						expandedVal = expandExpressionForMapField(strVal, fd)
					}
					valueValue, err := parseField(expandedVal, fd.MapValue(), logger)
					if err != nil {
						return err
					}

					if keyValue.IsValid() && valueValue.IsValid() {
						msg.ProtoReflect().Mutable(fd).Map().Set(keyValue.MapKey(), valueValue)
					}
				}
			}
			continue
		}

		if fd.IsList() {
			if yamlData == nil {
				continue
			}
			innerData, ok := yamlData[fieldName]
			if !ok || innerData == nil {
				continue
			}
			innerList, ok := innerData.([]interface{})
			if !ok {
				// 可能是string的Array 切割
				innerString, ok := innerData.(string)
				if !ok {
					// 分割 innerString
					continue
				}
				stringSlice := splitStringToArray(innerString)
				for _, v := range stringSlice {
					innerList = append(innerList, v)
				}
			}

			for _, item := range innerList {
				if fd.Kind() == protoreflect.MessageKind {
					if fd.Message().FullName() != proto.MessageName(&durationpb.Duration{}) &&
						fd.Message().FullName() != proto.MessageName(&timestamppb.Timestamp{}) {
						// Message
						innerMap, ok := item.(map[string]interface{})
						if ok {
							if err := ParsePlainMessage(innerMap, msg.ProtoReflect().Mutable(fd).List().AppendMutable().Message().Interface(), logger); err != nil {
								return err
							}
						} else {
							innerString, ok := item.(string)
							if ok {
								// 需要String视为Yaml解析
								yamlData = make(map[string]interface{})
								err := yaml.Unmarshal(lu.StringtoBytes(innerString), yamlData)
								if err != nil {
									return err
								}
								if err = ParsePlainMessage(yamlData, msg.ProtoReflect().Mutable(fd).Message().Interface(), logger); err != nil {
									return err
								}
							}
						}
						continue
					}
				}
				// 非Message
				value, err := parseField(item, fd, logger)
				if err != nil {
					return err
				}
				if value.IsValid() {
					msg.ProtoReflect().Mutable(fd).List().Append(value)
				}
			}
			continue
		}

		if fd.Kind() == protoreflect.MessageKind {
			if fd.Message().FullName() != proto.MessageName(&durationpb.Duration{}) &&
				fd.Message().FullName() != proto.MessageName(&timestamppb.Timestamp{}) {
				// 需要继续解析的字段
				if yamlData == nil {
					if err := ParsePlainMessage(nil, msg.ProtoReflect().Mutable(fd).Message().Interface(), logger); err != nil {
						return err
					}
					continue
				}
				if fieldMatch {
					// 在同层查找
					if err := ParsePlainMessage(yamlData, msg.ProtoReflect().Mutable(fd).Message().Interface(), logger); err != nil {
						return err
					}
				} else {
					innerMap, ok := yamlData[fieldName].(map[string]interface{})
					if ok {
						if err := ParsePlainMessage(innerMap, msg.ProtoReflect().Mutable(fd).Message().Interface(), logger); err != nil {
							return err
						}
					} else {
						innerString, ok := yamlData[fieldName].(string)
						if ok {
							// 需要String视为Yaml解析
							yamlData = make(map[string]interface{})
							err := yaml.Unmarshal(lu.StringtoBytes(innerString), yamlData)
							if err != nil {
								return err
							}
							if err = ParsePlainMessage(yamlData, msg.ProtoReflect().Mutable(fd).Message().Interface(), logger); err != nil {
								return err
							}
						} else {
							logger.LogWarn("ParseMessage message field not found, use default", "field", fieldName)
							if err := ParsePlainMessage(nil, msg.ProtoReflect().Mutable(fd).Message().Interface(), logger); err != nil {
								return err
							}
						}
					}
				}
				continue
			}
		}

		var fieldData interface{}
		if yamlData != nil {
			ok := false
			fieldData, ok = yamlData[fieldName]
			if !ok {
				logger.LogWarn("ParseMessage field not found, use default", "field", fieldName)
			}
		}
		value, err := parseField(fieldData, fd, logger)
		if err != nil {
			return fmt.Errorf("parseField error fieldName %s err %v", fieldName, err)
		}
		if value.IsValid() {
			if !msg.ProtoReflect().Get(fd).IsValid() {
				msg.ProtoReflect().Set(fd, value)
			}
		}
	}

	return nil
}

func dumpYamlIntoMessageFieldValue(yamlData interface{}, dst *protoreflect.Value, fd protoreflect.FieldDescriptor,
	logger *log.Logger, loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if fd == nil || dst == nil || yamlData == nil {
		return false
	}

	var err error

	if fd.Kind() == protoreflect.MessageKind {
		if fd.Message().FullName() == proto.MessageName(&durationpb.Duration{}) ||
			fd.Message().FullName() == proto.MessageName(&timestamppb.Timestamp{}) {
			*dst, err = parseField(yamlData, fd, logger)
			if err != nil {
				return false
			}

			return dst.IsValid()
		} else {
			// Message
			innerMap, ok := yamlData.(map[string]interface{})
			if !ok {
				return false
			}

			return dumpYamlIntoMessage(innerMap, dst.Message().Interface(), logger, loadOptions, dumpExistedSet, existedSetPrefix)
		}
	} else {
		*dst, err = parseField(yamlData, fd, logger)
		if err != nil {
			return false
		}

		return dst.IsValid()
	}
}

func dumpYamlIntoMessageFieldItem(yamlData map[string]interface{}, dst proto.Message, fd protoreflect.FieldDescriptor,
	logger *log.Logger, loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if fd == nil || dst == nil || len(yamlData) == 0 {
		return false
	}

	if fd.IsMap() {
		fieldName := string(fd.Name())
		fieldData, ok := yamlData[fieldName]
		if !ok || fieldData == nil {
			return false
		}

		innerMap, ok := fieldData.(map[string]interface{})
		if !ok {
			return false
		}

		hasValue := false
		field := dst.ProtoReflect().Mutable(fd)
		mapField := field.Map()

		nextMapExistedKeyIndex := 0
		// 这边需要循环Value
		for k, v := range innerMap {
			// Expand expressions in map keys and values using the parent map field's enable_expression option
			expandedK := expandExpressionForMapField(k, fd)
			expandedV := v
			if strV, ok := v.(string); ok {
				expandedV = expandExpressionForMapField(strV, fd)
			}

			mapExistedKeyIndex := nextMapExistedKeyIndex
			newKey := createEmptyMapKey(fd)
			newValue := mapField.NewValue()

			hasKey := dumpYamlIntoMessageFieldValue(expandedK, &newKey, fd.MapKey(), logger, loadOptions, dumpExistedSet,
				fmt.Sprintf("%s%s.%d.key.", existedSetPrefix, fd.Name(), mapExistedKeyIndex))
			valueOk := false
			if hasKey {
				valueOk = dumpYamlIntoMessageFieldValue(expandedV, &newValue, fd.MapValue(), logger, loadOptions, dumpExistedSet,
					fmt.Sprintf("%s%s.%d.value.", existedSetPrefix, fd.Name(), mapExistedKeyIndex))
			}
			if !hasKey || !valueOk {
				continue
			}
			if !newKey.IsValid() || !newValue.IsValid() {
				continue
			}

			nextMapExistedKeyIndex++

			hasValue = true
			if dumpExistedSet != nil {
				dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), mapExistedKeyIndex)] = struct{}{}
				dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d.key", existedSetPrefix, fd.Name(), mapExistedKeyIndex)] = struct{}{}
				dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d.value", existedSetPrefix, fd.Name(), mapExistedKeyIndex)] = struct{}{}
				dumpExistedSet.MutableMapKeyIndex()[makeExistedMapKeyIndexKey(existedSetPrefix, fd, newKey.MapKey())] = mapExistedKeyIndex
			}

			mapField.Set(newKey.MapKey(), newValue)
		}

		return hasValue
	}

	if fd.IsList() {
		fieldName := string(fd.Name())
		fieldData, ok := yamlData[fieldName]
		if !ok || fieldData == nil {
			return false
		}

		hasValue := false
		listField := dst.ProtoReflect().Mutable(fd).List()

		innerList, ok := fieldData.([]interface{})
		if !ok {
			newElement := listField.NewElement()
			listIndex := 0
			// 支持重排序
			if loadOptions != nil && loadOptions.ReorderListIndexByField != "" && fd.Kind() == protoreflect.MessageKind {
				itemAsMap, ok := fieldData.(map[string]interface{})
				if ok {
					indexValue, _ := itemAsMap[loadOptions.ReorderListIndexByField]
					listIndex = tryLoadListIndex(indexValue)
					if listIndex < 0 {
						listIndex = 0
					}
				}
			}
			// 如果不是数组，fallback为单字段模式
			if dumpYamlIntoMessageFieldValue(fieldData, &newElement, fd, logger, loadOptions, dumpExistedSet,
				fmt.Sprintf("%s%s.%d.", existedSetPrefix, fd.Name(), listIndex)) {

				if dumpExistedSet != nil {
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), listIndex)] = struct{}{}
				}

				for listField.Len() <= listIndex {
					listField.Append(listField.NewElement())
				}
				if listField.Len() == listIndex {
					listField.Append(newElement)
				} else {
					listField.Set(listIndex, newElement)
				}
				return true
			}

			return false
		}

		for i, item := range innerList {
			newElement := listField.NewElement()
			listIndex := i
			// 支持重排序
			if loadOptions != nil && loadOptions.ReorderListIndexByField != "" && fd.Kind() == protoreflect.MessageKind {
				itemAsMap, ok := item.(map[string]interface{})
				if ok {
					indexValue, _ := itemAsMap[loadOptions.ReorderListIndexByField]
					listIndex = tryLoadListIndex(indexValue)
					if listIndex < 0 {
						listIndex = i
					}
				}
			}
			if dumpYamlIntoMessageFieldValue(item, &newElement, fd, logger, loadOptions, dumpExistedSet,
				fmt.Sprintf("%s%s.%d.", existedSetPrefix, fd.Name(), listIndex)) {
				hasValue = true
				if dumpExistedSet != nil {
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), listIndex)] = struct{}{}
				}

				for listField.Len() <= listIndex {
					listField.Append(listField.NewElement())
				}
				if listField.Len() == listIndex {
					listField.Append(newElement)
				} else {
					listField.Set(listIndex, newElement)
				}
			} else {
				break
			}
		}

		return hasValue
	}

	// 同层级展开
	fieldMatch := false
	if fd.Message() != nil {
		if confMeta := proto.GetExtension(fd.Options(), atframe_protocol.E_CONFIGURE).(*atframe_protocol.AtappConfigureMeta); confMeta != nil {
			if confMeta.FieldMatch != nil && confMeta.FieldMatch.FieldName != "" && confMeta.FieldMatch.FieldValue != "" {
				// 存在跳过规则
				if value, ok := yamlData[confMeta.FieldMatch.FieldName].(string); ok {
					// 存在
					if value == confMeta.FieldMatch.FieldValue {
						fieldMatch = true
					} else {
						return false
					}
				}
			}
		}
	}

	newElement := dst.ProtoReflect().NewField(fd)
	hasValue := false
	if fieldMatch && fd.Message() != nil {
		if dumpYamlIntoMessage(yamlData, newElement.Message().Interface(), logger, loadOptions, dumpExistedSet, existedSetPrefix) {
			hasValue = true
		}
	} else {
		fieldName := string(fd.Name())
		fieldData, ok := yamlData[fieldName]
		if !ok || fieldData == nil {
			return false
		}
		hasValue = dumpYamlIntoMessageFieldValue(fieldData, &newElement, fd, logger, loadOptions, dumpExistedSet,
			fmt.Sprintf("%s%s.", existedSetPrefix, fd.Name()))
	}

	if hasValue {
		if dumpExistedSet != nil {
			dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s", existedSetPrefix, fd.Name())] = struct{}{}
		}

		dst.ProtoReflect().Set(fd, newElement)
	}

	return hasValue
}

func dumpYamlIntoMessage(yamlData map[string]interface{}, dst proto.Message, logger *log.Logger,
	loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if dst == nil {
		return false
	}
	if len(yamlData) == 0 {
		return false
	}

	ret := false

	// protoreflect.Fie
	fields := dst.ProtoReflect().Descriptor().Fields()
	fieldSize := fields.Len()
	for i := 0; i < fieldSize; i++ {
		fd := fields.Get(i)
		res := dumpYamlIntoMessageFieldItem(yamlData, dst, fd, logger, loadOptions, dumpExistedSet, existedSetPrefix)
		ret = ret || res
	}

	return ret
}

func LoadConfigFromOriginData(originData interface{}, prefixPath string, configPb proto.Message, logger *log.Logger,
	loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) (err error) {
	parent := originData
	pathParts := strings.Split(prefixPath, ".")
	for i, pathPart := range pathParts {
		trimPart := strings.TrimSpace(pathPart)
		if trimPart == "" {
			continue
		}

		if lu.IsNil(parent) {
			err = fmt.Errorf("LoadConfigFromOriginData data nil")
			break
		}

		arrayIndex, convErr := strconv.Atoi(trimPart)
		if convErr == nil {
			// 数组下标
			parentArray, ok := parent.([]interface{})
			if !ok {
				err = fmt.Errorf("LoadConfigFromOriginData expected array at %s, got %T", strings.Join(pathParts[0:i+1], "."), reflect.TypeOf(parent).Elem().Name())
				break
			}
			if len(parentArray) <= arrayIndex {
				err = fmt.Errorf("LoadConfigFromOriginData array index out of range at %s, got %d >= %d", strings.Join(pathParts[0:i+1], "."), arrayIndex, len(parentArray))
				break
			}
			parent = parentArray[arrayIndex]
		} else {
			// 字符串key
			parentMap, ok := parent.(map[string]interface{})
			if !ok {
				err = fmt.Errorf("LoadConfigFromOriginData expected map at %s, got %T", strings.Join(pathParts[0:i+1], "."), reflect.TypeOf(parent).Elem().Name())
				break
			}
			parent, ok = parentMap[trimPart]
			if !ok {
				err = fmt.Errorf("LoadConfigFromOriginData key not found at %s", strings.Join(pathParts[0:i+1], "."))
				break
			}
		}
	}

	if err != nil {
		logger.LogError("load prefixPath failed", "err", err)
		// 使用初始值初始化
		parseErr := LoadDefaultConfigMessageFields(configPb, logger, dumpExistedSet, existedSetPrefix)
		if parseErr != nil {
			logger.LogError("LoadDefaultConfigMessageFields failed", "err", parseErr)
		}
		return
	}

	atappData, ok := parent.(map[string]interface{})
	if !ok {
		err = fmt.Errorf("LoadConfigFromOriginData expected map at %s, got %T", strings.Join(pathParts, "."), reflect.TypeOf(parent).Elem().Name())
		return
	}

	dumpYamlIntoMessage(atappData, configPb, logger, loadOptions, dumpExistedSet, existedSetPrefix)
	return
}

func LoadConfigOriginYaml(configPath string) (yamlData map[string]interface{}, err error) {
	var data []byte
	data, err = os.ReadFile(configPath)
	if err != nil {
		return
	}

	yamlData = make(map[string]interface{})
	err = yaml.Unmarshal(data, yamlData)
	return
}

func LoadConfigFromYaml(configPath string, prefixPath string, configPb proto.Message, logger *log.Logger,
	loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) (yamlData map[string]interface{}, err error) {
	yamlData, err = LoadConfigOriginYaml(configPath)
	if err != nil {
		return
	}

	err = LoadConfigFromOriginData(yamlData, prefixPath, configPb, logger, loadOptions, dumpExistedSet, existedSetPrefix)
	return
}

func GetEnvUpperKey(key string) string {
	return os.Getenv(strings.ToUpper(key))
}

func dumpEnvironemntIntoMessageFieldValueBasic(envPrefix string, dst *protoreflect.Value, fd protoreflect.FieldDescriptor,
	logger *log.Logger, expressionParentFds ...protoreflect.FieldDescriptor,
) bool {
	if fd == nil || dst == nil {
		return false
	}

	envVal := GetEnvUpperKey(envPrefix)
	if envVal == "" {
		return false
	}

	// For map key/value fields, expression expansion needs the parent map field descriptor
	// because the enable_expression option is on the parent map field, not on sub-fields.
	for _, parentFd := range expressionParentFds {
		envVal = expandExpressionForMapField(envVal, parentFd)
	}

	var err error
	*dst, err = parseField(envVal, fd, logger)
	if err != nil {
		return false
	}

	return dst.IsValid()
}

func dumpEnvironemntIntoMessageFieldValueMessage(envPrefix string, dst *protoreflect.Value, fd protoreflect.FieldDescriptor,
	logger *log.Logger, loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if fd == nil || dst == nil {
		return false
	}

	if fd.Message() == nil && !fd.IsMap() {
		return false
	}

	if fd.Message().FullName() == proto.MessageName(&durationpb.Duration{}) ||
		fd.Message().FullName() == proto.MessageName(&timestamppb.Timestamp{}) {
		// 基础类型处理
		return dumpEnvironemntIntoMessageFieldValueBasic(envPrefix, dst, fd, logger)
	}

	return dumpEnvironemntIntoMessage(envPrefix, dst.Message().Interface(), logger, loadOptions, dumpExistedSet, existedSetPrefix)
}

func dumpEnvironemntIntoMessageFieldValue(envPrefix string, dst *protoreflect.Value, fd protoreflect.FieldDescriptor,
	logger *log.Logger, loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if fd == nil || dst == nil {
		return false
	}

	if fd.Kind() == protoreflect.MessageKind {
		return dumpEnvironemntIntoMessageFieldValueMessage(envPrefix, dst, fd, logger, loadOptions, dumpExistedSet, existedSetPrefix)
	} else {
		return dumpEnvironemntIntoMessageFieldValueBasic(envPrefix, dst, fd, logger)
	}
}

// dumpEnvironemntIntoMapSubFieldValue loads a map key or value from environment variables,
// with expression expansion using the parent map field descriptor's enable_expression option.
func dumpEnvironemntIntoMapSubFieldValue(envPrefix string, dst *protoreflect.Value, fd protoreflect.FieldDescriptor,
	parentMapFd protoreflect.FieldDescriptor, logger *log.Logger, loadOptions *LoadConfigOptions,
	dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if fd == nil || dst == nil {
		return false
	}

	// For non-message map key/value fields, use expression expansion with parent fd
	if fd.Kind() != protoreflect.MessageKind {
		return dumpEnvironemntIntoMessageFieldValueBasic(envPrefix, dst, fd, logger, parentMapFd)
	}
	return dumpEnvironemntIntoMessageFieldValue(envPrefix, dst, fd, logger, loadOptions, dumpExistedSet, existedSetPrefix)
}

func dumpEnvironemntIntoMessageFieldItem(envPrefix string, dst proto.Message, fd protoreflect.FieldDescriptor,
	logger *log.Logger, loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if fd == nil || dst == nil {
		return false
	}

	fieldName := string(fd.Name())
	var envKeyPrefix string
	if len(envPrefix) == 0 {
		envKeyPrefix = fieldName
	} else {
		envKeyPrefix = fmt.Sprintf("%s_%s", envPrefix, fieldName)
	}

	if fd.IsMap() {
		hasValue := false
		field := dst.ProtoReflect().Mutable(fd)
		mapField := field.Map()

		for i := 0; ; i++ {
			newKey := createEmptyMapKey(fd)
			newValue := mapField.NewValue()
			hasKey := dumpEnvironemntIntoMapSubFieldValue(
				fmt.Sprintf("%s_%d_KEY", envKeyPrefix, i), &newKey, fd.MapKey(), fd, logger, loadOptions, dumpExistedSet,
				fmt.Sprintf("%s%s.%d.key.", existedSetPrefix, fd.Name(), i))
			if hasKey {
				hasValue = dumpEnvironemntIntoMapSubFieldValue(
					fmt.Sprintf("%s_%d_VALUE", envKeyPrefix, i), &newValue, fd.MapValue(), fd, logger, loadOptions, dumpExistedSet,
					fmt.Sprintf("%s%s.%d.value.", existedSetPrefix, fd.Name(), i))
			}
			if hasKey && hasValue {
				hasValue = true
				if dumpExistedSet != nil {
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), i)] = struct{}{}
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d.key", existedSetPrefix, fd.Name(), i)] = struct{}{}
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d.value", existedSetPrefix, fd.Name(), i)] = struct{}{}
					dumpExistedSet.MutableMapKeyIndex()[makeExistedMapKeyIndexKey(existedSetPrefix, fd, newKey.MapKey())] = i
				}

				mapField.Set(newKey.MapKey(), newValue)
			} else {
				break
			}
		}

		// Fallback to no-index key
		if !hasValue {
			newKey := createEmptyMapKey(fd)
			newValue := mapField.NewValue()
			hasKey := dumpEnvironemntIntoMapSubFieldValue(
				fmt.Sprintf("%s_KEY", envKeyPrefix), &newKey, fd.MapKey(), fd, logger, loadOptions,
				dumpExistedSet, fmt.Sprintf("%s%s.%d.key.", existedSetPrefix, fd.Name(), 0))
			if hasKey {
				hasValue = dumpEnvironemntIntoMapSubFieldValue(
					fmt.Sprintf("%s_VALUE", envKeyPrefix), &newValue, fd.MapValue(), fd, logger, loadOptions,
					dumpExistedSet, fmt.Sprintf("%s%s.%d.value.", existedSetPrefix, fd.Name(), 0))
			}
			if hasKey && hasValue {
				hasValue = true
				if dumpExistedSet != nil {
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), 0)] = struct{}{}
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d.key", existedSetPrefix, fd.Name(), 0)] = struct{}{}
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d.value", existedSetPrefix, fd.Name(), 0)] = struct{}{}
					dumpExistedSet.MutableMapKeyIndex()[makeExistedMapKeyIndexKey(existedSetPrefix, fd, newKey.MapKey())] = 0
				}

				mapField.Set(newKey.MapKey(), newValue)
			}
		}
		return hasValue

	}

	if fd.IsList() {
		hasValue := false
		listField := dst.ProtoReflect().Mutable(fd).List()
		for i := 0; ; i++ {
			newElement := listField.NewElement()
			listIndex := i
			// 支持重排序
			if loadOptions != nil && loadOptions.ReorderListIndexByField != "" && fd.Kind() == protoreflect.MessageKind {
				listIndex = tryLoadListIndex(GetEnvUpperKey(fmt.Sprintf("%s_%d_%s", envKeyPrefix, i, loadOptions.ReorderListIndexByField)))
				if listIndex < 0 {
					listIndex = i
				}
			}
			if dumpEnvironemntIntoMessageFieldValue(fmt.Sprintf("%s_%d", envKeyPrefix, i), &newElement, fd, logger, loadOptions,
				dumpExistedSet, fmt.Sprintf("%s%s.%d.", existedSetPrefix, fd.Name(), listIndex)) {
				hasValue = true
				if dumpExistedSet != nil {
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), listIndex)] = struct{}{}
				}

				for listField.Len() <= listIndex {
					listField.Append(listField.NewElement())
				}
				if listField.Len() == listIndex {
					listField.Append(newElement)
				} else {
					listField.Set(listIndex, newElement)
				}
			} else {
				break
			}
		}

		// Fallback to no-index key
		if !hasValue {
			listIndex := 0
			// 支持重排序
			if loadOptions != nil && loadOptions.ReorderListIndexByField != "" && fd.Kind() == protoreflect.MessageKind {
				listIndex = tryLoadListIndex(GetEnvUpperKey(fmt.Sprintf("%s_%s", envKeyPrefix, loadOptions.ReorderListIndexByField)))
				if listIndex < 0 {
					listIndex = 0
				}
			}
			newElement := listField.NewElement()
			if dumpEnvironemntIntoMessageFieldValue(envKeyPrefix, &newElement, fd, logger, loadOptions, dumpExistedSet,
				fmt.Sprintf("%s%s.%d.", existedSetPrefix, fd.Name(), listIndex)) {
				hasValue = true
				if dumpExistedSet != nil {
					dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s.%d", existedSetPrefix, fd.Name(), listIndex)] = struct{}{}
				}

				for listField.Len() <= listIndex {
					listField.Append(listField.NewElement())
				}
				if listField.Len() == listIndex {
					listField.Append(newElement)
				} else {
					listField.Set(listIndex, newElement)
				}
			}
		}
		return hasValue
	}

	// 同层级展开
	fieldMatch := false
	if fd.Message() != nil {
		if confMeta := proto.GetExtension(fd.Options(), atframe_protocol.E_CONFIGURE).(*atframe_protocol.AtappConfigureMeta); confMeta != nil {
			if confMeta.FieldMatch != nil && confMeta.FieldMatch.FieldName != "" && confMeta.FieldMatch.FieldValue != "" {
				checkFieldMatchValue := strings.TrimSpace(GetEnvUpperKey(fmt.Sprintf("%s_%s", envPrefix, confMeta.FieldMatch.FieldName)))
				// 存在跳过规则
				if checkFieldMatchValue == confMeta.FieldMatch.FieldValue {
					fieldMatch = true
				} else {
					return false
				}
			}
		}
	}

	newElement := dst.ProtoReflect().NewField(fd)
	hasValue := false
	if fieldMatch && fd.Message() != nil {
		hasValue = dumpEnvironemntIntoMessageFieldValue(envPrefix, &newElement, fd, logger, loadOptions, dumpExistedSet, existedSetPrefix)
	} else {
		hasValue = dumpEnvironemntIntoMessageFieldValue(envKeyPrefix, &newElement, fd, logger, loadOptions, dumpExistedSet,
			fmt.Sprintf("%s%s.", existedSetPrefix, fd.Name()))
	}

	if hasValue {
		if dumpExistedSet != nil {
			dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%s%s", existedSetPrefix, fd.Name())] = struct{}{}
		}

		dst.ProtoReflect().Set(fd, newElement)
	}

	return hasValue
}

func dumpEnvironemntIntoMessage(envPrefix string, dst proto.Message, logger *log.Logger,
	loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if dst == nil {
		return false
	}

	ret := false

	// protoreflect.Fie
	fields := dst.ProtoReflect().Descriptor().Fields()
	fieldSize := fields.Len()
	for i := 0; i < fieldSize; i++ {
		fd := fields.Get(i)
		res := dumpEnvironemntIntoMessageFieldItem(envPrefix, dst, fd, logger, loadOptions, dumpExistedSet, existedSetPrefix)
		ret = ret || res
	}

	return ret
}

func LoadConfigFromEnvironemnt(envPrefix string, configPb proto.Message, logger *log.Logger,
	loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) (bool, error) {
	if configPb == nil {
		return false, fmt.Errorf("dumpEnvironemntIntoMessage logger or configPb is nil")
	}

	return dumpEnvironemntIntoMessage(envPrefix, configPb, logger, loadOptions, dumpExistedSet, existedSetPrefix), nil
}

func dumpDefaultConfigMessageField(configPb proto.Message, fd protoreflect.FieldDescriptor, logger *log.Logger,
	dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) {
	if configPb == nil || fd == nil {
		return
	}

	if fd.ContainingOneof() != nil {
		if configPb.ProtoReflect().WhichOneof(fd.ContainingOneof()) != nil {
			return
		}
	}

	allowStringDefaultValue := fd.Message() == nil && !fd.IsMap()
	if !allowStringDefaultValue && !fd.IsMap() {
		allowStringDefaultValue = fd.Message().FullName() == proto.MessageName(&durationpb.Duration{}) ||
			fd.Message().FullName() == proto.MessageName(&timestamppb.Timestamp{})
	}

	confMeta := proto.GetExtension(fd.Options(), atframe_protocol.E_CONFIGURE).(*atframe_protocol.AtappConfigureMeta)
	if allowStringDefaultValue {
		if confMeta == nil {
			return
		}

		if confMeta.DefaultValue == "" {
			return
		}

		if fd.IsList() {
			return
		}

		if dumpExistedSet != nil {
			checkKey := fmt.Sprintf("%s%s", existedSetPrefix, fd.Name())
			_, existed := dumpExistedSet.MutableExistedSet()[checkKey]
			if existed {
				return
			}
		}
	}

	// Map展开默认值
	if fd.IsMap() {
		mapValueFd := fd.MapValue()
		if mapValueFd.Message() == nil {
			return
		}
		nextMapIndex := 0
		// parseField
		configPb.ProtoReflect().Mutable(fd).Map().Range(func(mk protoreflect.MapKey, v protoreflect.Value) bool {
			var foundIndex int
			var exists bool = false
			mapIndexKey := makeExistedMapKeyIndexKey(existedSetPrefix, fd, mk)
			if dumpExistedSet != nil {
				foundIndex, exists = dumpExistedSet.MutableMapKeyIndex()[mapIndexKey]
			}
			if !exists {
				foundIndex = nextMapIndex
				nextMapIndex++

				dumpExistedSet.MutableMapKeyIndex()[mapIndexKey] = foundIndex
			}

			LoadDefaultConfigMessageFields(v.Message().Interface(), logger, dumpExistedSet,
				fmt.Sprintf("%s%s.%d.value.", existedSetPrefix, fd.Name(), foundIndex))
			return true
		})

		return
	}

	// List展开默认值
	if fd.IsList() {
		if fd.Message() == nil {
			return
		}

		list := configPb.ProtoReflect().Get(fd).List()
		for i := 0; i < list.Len(); i++ {
			LoadDefaultConfigMessageFields(list.Get(i).Message().Interface(), logger, dumpExistedSet,
				fmt.Sprintf("%s%s.%d.", existedSetPrefix, fd.Name(), i))
		}
		return
	}

	// 普通类型
	if allowStringDefaultValue && confMeta != nil {
		v, err := parseField(confMeta.DefaultValue, fd, logger)
		if err == nil && v.IsValid() {
			configPb.ProtoReflect().Set(fd, v)
			return
		}
	}

	if fd.Message() == nil {
		return
	}

	// 普通Message默认值填充
	subMsg := configPb.ProtoReflect().Mutable(fd).Message()
	if subMsg == nil || !subMsg.IsValid() {
		return
	}

	LoadDefaultConfigMessageFields(subMsg.Interface(), logger, dumpExistedSet,
		fmt.Sprintf("%s%s.", existedSetPrefix, fd.Name()))
}

func LoadDefaultConfigMessageFields(configPb proto.Message, logger *log.Logger,
	dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) error {
	if configPb == nil {
		return fmt.Errorf("LoadDefaultConfigFields configPb is nil")
	}

	if dumpExistedSet == nil {
		dumpExistedSet = CreateConfigExistedIndex()
	}

	fields := configPb.ProtoReflect().Descriptor().Fields()
	fieldSize := fields.Len()
	for i := 0; i < fieldSize; i++ {
		fd := fields.Get(i)
		dumpDefaultConfigMessageField(configPb, fd, logger, dumpExistedSet, existedSetPrefix)
	}

	return nil
}

// dumpEnvironemntIntoLogCategory 从环境变量加载 category 配置
func dumpEnvironemntIntoLogCategory(envPrefix string, category *atframe_protocol.AtappLogCategory,
	logger *log.Logger, loadOptions *LoadConfigOptions, dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if category == nil {
		return false
	}

	ret := false

	// 加载拓展 sink 列表: <envPrefix>_<sink_index>_<FIELD_NAME>
	// 回退到旧格式: <envPrefix>_CATEGORY_<category_index>_SINK_<sink_index>_<FIELD_NAME>
	for sinkIndex := 0; ; sinkIndex++ {
		sinkEnvPrefix := fmt.Sprintf("%s_%d", envPrefix, sinkIndex)
		newSink := &atframe_protocol.AtappLogSink{}

		if dumpEnvironemntIntoMessage(sinkEnvPrefix, newSink, logger, loadOptions, dumpExistedSet,
			fmt.Sprintf("%ssink.%d.", existedSetPrefix, sinkIndex)) {
		} else {
			break
		}

		if newSink.Type == "" {
			continue
		}

		category.Sink = append(category.Sink, newSink)
		ret = true

		if dumpExistedSet != nil {
			dumpExistedSet.MutableExistedSet()[fmt.Sprintf("%ssink.%d", existedSetPrefix, sinkIndex)] = struct{}{}
		}
	}

	return ret
}

// LoadLogCategoryConfigFromEnvironemnt 从环境变量加载日志配置
// 支持特殊的 sink 配置格式: <前缀>_<CATEGORY_NAME>_<sink 下标>_<大写字段名>
func LoadLogCategoryConfigFromEnvironemnt(envPrefix string, logCategoryPb *atframe_protocol.AtappLogCategory, logger *log.Logger,
	dumpExistedSet *ConfigExistedIndex, existedSetPrefix string,
) bool {
	if logCategoryPb == nil {
		return false
	}

	return dumpEnvironemntIntoLogCategory(envPrefix, logCategoryPb, logger, nil, dumpExistedSet, existedSetPrefix)
}
