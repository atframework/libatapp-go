package libatapp

import (
	"testing"
)

func TestSplitIdsByString(t *testing.T) {
	tests := []struct {
		input    string
		expected []uint64
	}{
		{"", nil},
		{"8.8.8.8", []uint64{8, 8, 8, 8}},
		{"1.2.3.4", []uint64{1, 2, 3, 4}},
		{"16", []uint64{16}},
		{"1.2", []uint64{1, 2}},
	}

	for _, tt := range tests {
		result := SplitIdsByString(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("SplitIdsByString(%q) = %v, want %v", tt.input, result, tt.expected)
			continue
		}
		for i := range result {
			if result[i] != tt.expected[i] {
				t.Errorf("SplitIdsByString(%q)[%d] = %d, want %d", tt.input, i, result[i], tt.expected[i])
			}
		}
	}
}

func TestConvertAppIdByString(t *testing.T) {
	tests := []struct {
		idIn     string
		mask     string
		expected uint64
	}{
		// Pure number, no mask
		{"12345", "", 12345},
		{"0", "", 0},
		{"", "", 0},
		// Hex number
		{"0x1A2B", "", 0x1A2B},
		{"0X1a2b", "", 0x1A2B},
		// Dot-separated with mask "8.8.8.8"
		{"1.2.3.4", "8.8.8.8", 0x01020304},
		// Pure number with mask present (no dots → treated as number)
		{"12345", "8.8.8.8", 12345},
		// Dot-separated with different mask
		{"1.2", "16.16", (1 << 16) | 2},
		// Mask shorter than segments
		{"1.2.3.4", "8.8", (1 << 8) | 2},
		// Empty id
		{"", "8.8.8.8", 0},
	}

	for _, tt := range tests {
		result := ConvertAppIdByString(tt.idIn, tt.mask)
		if result != tt.expected {
			t.Errorf("ConvertAppIdByString(%q, %q) = 0x%X (%d), want 0x%X (%d)",
				tt.idIn, tt.mask, result, result, tt.expected, tt.expected)
		}
	}
}

func TestConvertAppIdToString(t *testing.T) {
	tests := []struct {
		idIn     uint64
		mask     []uint64
		hex      bool
		expected string
	}{
		{0x01020304, []uint64{8, 8, 8, 8}, false, "1.2.3.4"},
		{0x01020304, []uint64{8, 8, 8, 8}, true, "0x1.0x2.0x3.0x4"},
		{12345, nil, false, "12345"},
		{12345, nil, true, "0x3039"},
	}

	for _, tt := range tests {
		result := ConvertAppIdToString(tt.idIn, tt.mask, tt.hex)
		if result != tt.expected {
			t.Errorf("ConvertAppIdToString(0x%X, %v, %v) = %q, want %q",
				tt.idIn, tt.mask, tt.hex, result, tt.expected)
		}
	}
}

func TestRoundTripConversion(t *testing.T) {
	mask := "8.8.8.8"
	maskSlice := []uint64{8, 8, 8, 8}
	idStr := "1.2.3.4"
	id := ConvertAppIdByString(idStr, mask)
	if id != 0x01020304 {
		t.Fatalf("ConvertAppIdByString(%q, %q) = 0x%X, want 0x01020304", idStr, mask, id)
	}
	back := ConvertAppIdToString(id, maskSlice, false)
	if back != idStr {
		t.Errorf("ConvertAppIdToString(0x%X, %v, false) = %q, want %q", id, maskSlice, back, idStr)
	}
}
