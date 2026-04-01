package etcdversion

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew_SetsAllFields(t *testing.T) {
	v := New(10, 20, 5)
	assert.Equal(t, int64(10), v.CreateRevision)
	assert.Equal(t, int64(20), v.ModRevision)
	assert.Equal(t, int64(5), v.Version)
}

func TestNew_ZeroValues(t *testing.T) {
	v := New(0, 0, 0)
	assert.Equal(t, int64(0), v.CreateRevision)
	assert.Equal(t, int64(0), v.ModRevision)
	assert.Equal(t, int64(0), v.Version)
}

func TestDefaults(t *testing.T) {
	assert.Equal(t, int64(0), DefaultCreateRevision)
	assert.Equal(t, int64(0), DefaultModRevision)
	assert.Equal(t, int64(0), DefaultVersion)
}

func TestDataVersion_Equality(t *testing.T) {
	a := New(1, 2, 3)
	b := New(1, 2, 3)
	c := New(1, 2, 4)

	assert.Equal(t, a, b)
	assert.NotEqual(t, a, c)
}
