package libatapp

import (
	"strconv"
	"strings"
	"unicode"
)

// SplitIdsByString splits a dot-separated string of numbers into a slice of uint64.
// For example, "8.8.8.8" -> [8, 8, 8, 8], "1.2.3.4" -> [1, 2, 3, 4].
func SplitIdsByString(in string) []uint64 {
	if in == "" {
		return nil
	}

	var out []uint64
	for _, part := range strings.Split(in, ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		val, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			val = 0
		}
		out = append(out, val)
	}
	return out
}

// ConvertAppIdByString converts an id string to a uint64 value using the given mask.
//
// If the id string is a pure number (no dots), it is parsed directly as an integer
// (supports "0x"/"0X" hex prefix and plain decimal).
//
// If the id string contains dots (e.g. "1.2.3.4") and mask is non-empty,
// each dot-separated segment is combined using bit-shift according to the mask.
// For example, with mask [8,8,8,8], "1.2.3.4" -> 0x01020304.
func ConvertAppIdByString(idIn string, mask string) uint64 {
	if idIn == "" {
		return 0
	}

	idInIsNumber := true
	if len(mask) > 0 {
		for _, ch := range idIn {
			if ch == '.' {
				idInIsNumber = false
				break
			}
		}
	}

	if idInIsNumber {
		return parseIdNumber(idIn)
	}

	masks := SplitIdsByString(mask)

	ids := SplitIdsByString(idIn)
	var ret uint64
	for i := 0; i < len(ids) && i < len(masks); i++ {
		ret <<= masks[i]
		ret |= ids[i] & ((uint64(1) << masks[i]) - 1)
	}
	return ret
}

// ConvertAppIdToString converts a uint64 id back to a dot-separated string using the given mask.
// If hex is true, each segment is formatted as "0x<hex>".
func ConvertAppIdToString(idIn uint64, mask []uint64, hex bool) string {
	if len(mask) == 0 {
		if hex {
			return "0x" + strconv.FormatUint(idIn, 16)
		}
		return strconv.FormatUint(idIn, 10)
	}

	ids := make([]uint64, len(mask))
	for i := len(mask) - 1; i >= 0; i-- {
		ids[i] = idIn & ((uint64(1) << mask[i]) - 1)
		idIn >>= mask[i]
	}

	var sb strings.Builder
	for i, v := range ids {
		if i > 0 {
			sb.WriteByte('.')
		}
		if hex {
			sb.WriteString("0x")
			sb.WriteString(strconv.FormatUint(v, 16))
		} else {
			sb.WriteString(strconv.FormatUint(v, 10))
		}
	}
	return sb.String()
}

// parseIdNumber parses a numeric string to uint64, supporting 0x/0X hex prefix.
func parseIdNumber(s string) uint64 {
	s = strings.TrimLeftFunc(s, unicode.IsSpace)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		val, err := strconv.ParseUint(s[2:], 16, 64)
		if err != nil {
			return 0
		}
		return val
	}
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return val
}
