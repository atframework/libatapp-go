package consistenthash

import (
	"fmt"
	"sort"
	"testing"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestNewRing(t *testing.T) {
	// Arrange
	ring := NewRing(10)

	// Assert
	if ring == nil {
		t.Fatal("Expected ring to be initialized")
	}
	ring.Add("member1")
	if got := ring.Get("health-check-key"); got != "member1" {
		t.Errorf("Expected ring to serve added member, got %q", got)
	}
}

func TestAddAndGet(t *testing.T) {
	// Arrange
	ring := NewRing(1)
	ring.Add("member1")

	key := "any-key"

	// Act
	member := ring.Get(key)

	// Assert
	if member != "member1" {
		t.Errorf("Expected to get 'member1', but got '%s'", member)
	}
}

func TestSetAndGet(t *testing.T) {
	// Arrange
	ring := NewRing(10)
	members := []string{"member1", "member2", "member3"}
	ring.Set(members)

	key := "a-very-specific-key"

	// Act
	member1 := ring.Get(key)
	member2 := ring.Get(key)

	// Assert
	if member1 != member2 {
		t.Errorf("Expected Get to be deterministic, but got '%s' then '%s'", member1, member2)
	}

	found := false
	for _, m := range members {
		if m == member1 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Get returned a member '%s' that is not in the ring", member1)
	}
}

func TestGetN(t *testing.T) {
	// Arrange
	ring := NewRing(10)
	members := []string{"m1", "m2", "m3", "m4", "m5"}
	ring.Set(members)

	// Act: get fewer members than total
	n_less, err := ring.GetN("some-key", 3, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	if err != nil {
		t.Fatalf("GetN for 3 failed: %v", err)
	}

	// Assert
	if len(n_less) != 3 {
		t.Errorf("Expected 3 members, got %d", len(n_less))
	}
	if hasDuplicates(n_less) {
		t.Errorf("Expected unique members, but found duplicates in %v", n_less)
	}

	// Act: get as many members as total
	n_equal, err := ring.GetN("another-key", 5, pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE)
	if err != nil {
		t.Fatalf("GetN for 5 failed: %v", err)
	}

	// Assert
	if len(n_equal) != 5 {
		t.Errorf("Expected 5 members, got %d", len(n_equal))
	}
	sort.Strings(n_equal)
	sort.Strings(members)
	for i := range members {
		if members[i] != n_equal[i] {
			t.Errorf("Expected all members to be returned, but lists differ. Expected %v, got %v", members, n_equal)
			break
		}
	}

	// Act: get more members than total
	n_more, err := ring.GetN("yet-another-key", 10, pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE)
	if err != nil {
		t.Fatalf("GetN for 10 failed: %v", err)
	}

	// Assert
	if len(n_more) != 5 {
		t.Errorf("Expected 5 members when asking for 10, got %d", len(n_more))
	}
}

// 该测试函数用于验证相关行为。
func TestSearchModes(t *testing.T) {
	// Arrange
	ring := NewRing(10)
	members := []string{"server1", "server2", "server3", "server4", "server5"}
	ring.Set(members)

	key := "test-key"

	tests := []struct {
		name  string
		mode  pb.EtcdSearchMode
		count int
	}{
		{"pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL", pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL, 3},
		{"pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT", pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT, 3},
		{"pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE", pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE, 3},
		{"pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE", pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE, 3},
		{"pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE", pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE, 3},
		{"pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT", pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT, 3},
		{"pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE", pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE, 3},
		{"pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE", pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			result, err := ring.GetN(key, tt.count, tt.mode)
			if err != nil {
				t.Fatalf("GetN with mode %s failed: %v", tt.name, err)
			}

			// Assert
			if len(result) != tt.count {
				t.Errorf("Expected %d members with mode %s, got %d", tt.count, tt.name, len(result))
			}
			if hasDuplicates(result) {
				t.Errorf("Expected unique members with mode %s, but got duplicates in %v", tt.name, result)
			}
			// Verify all returned members are in the original set
			for _, member := range result {
				found := false
				for _, m := range members {
					if member == m {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Mode %s returned member '%s' not in original set", tt.name, member)
				}
			}
		})
	}
}

// 该测试函数用于验证相关行为。
func TestSearchModesUniqueness(t *testing.T) {
	// Arrange
	ring := NewRing(100) // Many virtual nodes for same physical node
	ring.Set([]string{"node1", "node2"})

	key := "test-key"

	uniqueModes := []pb.EtcdSearchMode{
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE,
	}

	for _, mode := range uniqueModes {
		t.Run(mode.String(), func(t *testing.T) {
			// Act
			result, err := ring.GetN(key, 10, mode)
			if err != nil {
				t.Fatalf("GetN failed: %v", err)
			}

			// Assert
			// Should return at most 2 unique nodes
			if len(result) > 2 {
				t.Errorf("Expected at most 2 unique nodes, got %d", len(result))
			}
			if hasDuplicates(result) {
				t.Errorf("Expected unique nodes, got duplicates: %v", result)
			}
		})
	}
}

// 该测试函数用于验证相关行为。
func TestSearchModesOrder(t *testing.T) {
	// Arrange
	ring := NewRing(10)
	ring.Set([]string{"a", "b", "c"})

	key := "test-key"

	// Act
	allResult, _ := ring.GetN(key, 3, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)

	// Act
	nextResult, _ := ring.GetN(key, 3, pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE)

	// Assert
	if hasDuplicates(allResult) {
		t.Error("pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL has duplicates")
	}
	if hasDuplicates(nextResult) {
		t.Error("pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE has duplicates")
	}
}

func TestEmptyRing(t *testing.T) {
	// Arrange
	ring := NewRing(10)

	// Act
	member := ring.Get("any-key")

	// Assert
	if member != "" {
		t.Errorf("Expected empty string on Get from empty ring, got '%s'", member)
	}

	// Act
	members, err := ring.GetN("any-key", 3, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	if err != nil {
		t.Errorf("GetN on empty ring returned an error: %v", err)
	}

	// Assert
	if len(members) != 0 {
		t.Errorf("Expected 0 members from GetN on empty ring, but got %d", len(members))
	}
}

// hasDuplicates 判断是否存在Duplicates。
func hasDuplicates(slice []string) bool {
	seen := make(map[string]struct{}, len(slice))
	for _, item := range slice {
		if _, ok := seen[item]; ok {
			return true
		}
		seen[item] = struct{}{}
	}
	return false
}

func TestGetDeterministic(t *testing.T) {
	// Arrange
	members1 := []string{"a", "b", "c"}
	members2 := []string{"c", "a", "b"}

	ring1 := NewRing(50)
	ring1.Set(members1)

	ring2 := NewRing(50)
	ring2.Set(members2)

	// Act
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		m1 := ring1.Get(key)
		m2 := ring2.Get(key)

		// Assert
		if m1 != m2 {
			t.Errorf("For key '%s', expected deterministic result, but got '%s' and '%s'", key, m1, m2)
		}
	}
}
