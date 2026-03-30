package topology

import "testing"

func TestParseInfo_DefaultVersionIsZero(t *testing.T) {
	raw := []byte(`{"name":"svc-a"}`)
	info := ParseInfo("/topology/a", 100, 0, 0, 0, raw)
	if info == nil {
		t.Fatal("ParseInfo should return non-nil info")
	}

	if info.CreateRevision != 100 {
		t.Fatalf("expected CreateRevision=100, got %d", info.CreateRevision)
	}
	if info.ModRevision != 100 {
		t.Fatalf("expected ModRevision=100, got %d", info.ModRevision)
	}
	if info.Version != 0 {
		t.Fatalf("expected Version=0 by etcd_data_version default, got %d", info.Version)
	}
}

func TestParseInfo_KeepPositiveVersion(t *testing.T) {
	raw := []byte(`{"name":"svc-a"}`)
	info := ParseInfo("/topology/b", 100, 11, 17, 23, raw)
	if info == nil {
		t.Fatal("ParseInfo should return non-nil info")
	}

	if info.CreateRevision != 11 || info.ModRevision != 17 || info.Version != 23 {
		t.Fatalf("unexpected version tuple (%d,%d,%d)", info.CreateRevision, info.ModRevision, info.Version)
	}
}
