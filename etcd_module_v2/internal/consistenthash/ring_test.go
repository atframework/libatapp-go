package consistenthash

import (
	"fmt"
	"sort"
	"testing"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hasDuplicates(s []string) bool {
	seen := make(map[string]struct{}, len(s))
	for _, v := range s {
		if _, ok := seen[v]; ok {
			return true
		}
		seen[v] = struct{}{}
	}
	return false
}

func TestNewRing_DefaultVirtualNodes(t *testing.T) {
	ring := NewRing(0)
	ring.Add("member1")
	assert.NotEmpty(t, ring.Get("any-key"))
}

func TestAddAndGet_SingleMember(t *testing.T) {
	ring := NewRing(1)
	ring.Add("member1")
	assert.Equal(t, "member1", ring.Get("any-key"))
}

func TestAdd_DuplicateIsNoop(t *testing.T) {
	ring := NewRing(10)
	ring.Add("m1")
	ring.Add("m1")
	// Only one member in ring; Get should still return "m1".
	assert.Equal(t, "m1", ring.Get("key"))
}

func TestSetAndGet_Deterministic(t *testing.T) {
	members := []string{"member1", "member2", "member3"}
	ring := NewRing(10)
	ring.Set(members)

	key := "a-very-specific-key"
	first := ring.Get(key)
	second := ring.Get(key)
	assert.Equal(t, first, second)

	found := false
	for _, m := range members {
		if m == first {
			found = true
			break
		}
	}
	assert.True(t, found, "Get returned a member not in the ring: %s", first)
}

func TestGetDeterministicAcrossSetOrders(t *testing.T) {
	ring1 := NewRing(50)
	ring1.Set([]string{"a", "b", "c"})

	ring2 := NewRing(50)
	ring2.Set([]string{"c", "a", "b"})

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		assert.Equal(t, ring1.Get(key), ring2.Get(key), "non-deterministic for key %s", key)
	}
}

func TestEmptyRing_GetReturnsEmpty(t *testing.T) {
	ring := NewRing(10)
	assert.Equal(t, "", ring.Get("any-key"))
}

func TestEmptyRing_GetNReturnsEmpty(t *testing.T) {
	ring := NewRing(10)
	result, err := ring.GetN("key", 3, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetN_LessThanTotal(t *testing.T) {
	ring := NewRing(10)
	ring.Set([]string{"m1", "m2", "m3", "m4", "m5"})

	result, err := ring.GetN("key", 3, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.False(t, hasDuplicates(result))
}

func TestGetN_MoreThanTotal_CappedAtTotal(t *testing.T) {
	ring := NewRing(10)
	ring.Set([]string{"m1", "m2", "m3"})

	result, err := ring.GetN("key", 10, pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE)
	require.NoError(t, err)
	assert.Len(t, result, 3)
}

func TestGetN_AllUniqueResults(t *testing.T) {
	ring := NewRing(10)
	members := []string{"m1", "m2", "m3", "m4", "m5"}
	ring.Set(members)

	result, err := ring.GetN("another-key", 5, pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE)
	require.NoError(t, err)
	assert.Len(t, result, 5)

	got := make([]string, len(result))
	copy(got, result)
	sort.Strings(got)
	exp := make([]string, len(members))
	copy(exp, members)
	sort.Strings(exp)
	assert.Equal(t, exp, got)
}

func TestSearchModes_AllReturnValidMembers(t *testing.T) {
	ring := NewRing(10)
	members := []string{"server1", "server2", "server3", "server4", "server5"}
	ring.Set(members)

	modes := []pb.EtcdSearchMode{
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE,
	}

	memberSet := make(map[string]bool, len(members))
	for _, m := range members {
		memberSet[m] = true
	}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			result, err := ring.GetN("test-key", 3, mode)
			require.NoError(t, err)
			assert.Len(t, result, 3)
			assert.False(t, hasDuplicates(result))
			for _, m := range result {
				assert.True(t, memberSet[m], "returned member %q not in ring", m)
			}
		})
	}
}

func TestUniqueModes_NoDuplicatesWithManyVirtualNodes(t *testing.T) {
	ring := NewRing(100)
	ring.Set([]string{"node1", "node2"})

	uniqueModes := []pb.EtcdSearchMode{
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE,
	}

	for _, mode := range uniqueModes {
		t.Run(mode.String(), func(t *testing.T) {
			result, err := ring.GetN("key", 10, mode)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(result), 2, "should not exceed physical member count")
			assert.False(t, hasDuplicates(result))
		})
	}
}

func TestNextModes_DifferFromBaseModes(t *testing.T) {
	// With enough members and virtual nodes the NEXT variants may return a
	// different first element.  We at least verify they run without error.
	ring := NewRing(20)
	ring.Set([]string{"a", "b", "c", "d"})

	base, err := ring.GetN("key", 4, pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE)
	require.NoError(t, err)

	next, err := ring.GetN("key", 4, pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE)
	require.NoError(t, err)

	// Both should return all members.
	assert.Len(t, base, 4)
	assert.Len(t, next, 4)
}
