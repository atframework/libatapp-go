package pathbuilder

import (
	"testing"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/stretchr/testify/assert"
)

var testInfo = &pb.AtappDiscovery{
	Id:       42,
	Name:     "mysvc",
	TypeId:   7,
	TypeName: "worker",
}

const base = "/services"

func TestBuildByIDPath(t *testing.T) {
	assert.Equal(t, "/services/by_id/mysvc-42", BuildByIDPath(base, testInfo))
}

func TestBuildByIDPath_NilInfo(t *testing.T) {
	assert.Equal(t, "", BuildByIDPath(base, nil))
}

func TestBuildByTypeIDPath(t *testing.T) {
	assert.Equal(t, "/services/by_type_id/7/mysvc-42", BuildByTypeIDPath(base, testInfo))
}

func TestBuildByTypeIDPath_NilInfo(t *testing.T) {
	assert.Equal(t, "", BuildByTypeIDPath(base, nil))
}

func TestBuildByTypeNamePath(t *testing.T) {
	assert.Equal(t, "/services/by_type_name/worker/mysvc-42", BuildByTypeNamePath(base, testInfo))
}

func TestBuildByTypeNamePath_NilInfo(t *testing.T) {
	assert.Equal(t, "", BuildByTypeNamePath(base, nil))
}

func TestBuildByNamePath(t *testing.T) {
	assert.Equal(t, "/services/by_name/mysvc-42", BuildByNamePath(base, testInfo))
}

func TestBuildByNamePath_NilInfo(t *testing.T) {
	assert.Equal(t, "", BuildByNamePath(base, nil))
}

func TestBuildByTagPath(t *testing.T) {
	assert.Equal(t, "/services/by_tag/prod/mysvc-42", BuildByTagPath(base, testInfo, "prod"))
}

func TestBuildByTagPath_NilInfo(t *testing.T) {
	assert.Equal(t, "", BuildByTagPath(base, nil, "prod"))
}

func TestBuildByIDWatcherPath(t *testing.T) {
	assert.Equal(t, "/services/by_id", BuildByIDWatcherPath(base))
}

func TestBuildByTypeIDWatcherPath(t *testing.T) {
	assert.Equal(t, "/services/by_type_id/7", BuildByTypeIDWatcherPath(base, 7))
}

func TestBuildByTypeNameWatcherPath(t *testing.T) {
	assert.Equal(t, "/services/by_type_name/worker", BuildByTypeNameWatcherPath(base, "worker"))
}

func TestBuildByNameWatcherPath(t *testing.T) {
	assert.Equal(t, "/services/by_name", BuildByNameWatcherPath(base))
}

func TestBuildByTagWatcherPath(t *testing.T) {
	assert.Equal(t, "/services/by_tag/prod", BuildByTagWatcherPath(base, "prod"))
}

// Verify paths are unique for different services to prevent key collision.
func TestBuildPaths_UniqueForDifferentIDs(t *testing.T) {
	info1 := &pb.AtappDiscovery{Id: 1, Name: "svc", TypeId: 1, TypeName: "t"}
	info2 := &pb.AtappDiscovery{Id: 2, Name: "svc", TypeId: 1, TypeName: "t"}

	assert.NotEqual(t, BuildByIDPath(base, info1), BuildByIDPath(base, info2))
	assert.NotEqual(t, BuildByNamePath(base, info1), BuildByNamePath(base, info2))
	assert.NotEqual(t, BuildByTypeIDPath(base, info1), BuildByTypeIDPath(base, info2))
}
