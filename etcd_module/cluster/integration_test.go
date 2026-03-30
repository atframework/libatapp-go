//go:build integration
// +build integration

package cluster

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	log "log/slog"

	"github.com/atframework/libatapp-go/etcd_module/client"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

type integrationEnv struct {
	endpoints []string
	basePath  string
	username  string
	password  string
	timeout   time.Duration
}

func loadIntegrationEnv() (*integrationEnv, error) {
	endpointsStr := os.Getenv("SD_ETCD_ENDPOINTS")
	if endpointsStr == "" {
		return nil, fmt.Errorf("SD_ETCD_ENDPOINTS not set (example: localhost:2379)")
	}

	basePath := os.Getenv("SD_ETCD_BASE_PATH")
	if basePath == "" {
		basePath = "/atapp/test/integration"
	}

	timeout := 10 * time.Second
	if timeoutStr := os.Getenv("SD_ETCD_TIMEOUT"); timeoutStr != "" {
		if parsed, err := time.ParseDuration(timeoutStr); err == nil {
			timeout = parsed
		}
	}

	env := &integrationEnv{
		endpoints: strings.Split(endpointsStr, ","),
		basePath:  basePath,
		username:  os.Getenv("SD_ETCD_USERNAME"),
		password:  os.Getenv("SD_ETCD_PASSWORD"),
		timeout:   timeout,
	}

	return env, nil
}

func createIntegrationClient(t *testing.T, env *integrationEnv) client.EtcdClient {
	config := clientv3.Config{
		Endpoints:   env.endpoints,
		DialTimeout: env.timeout,
	}

	if env.username != "" && env.password != "" {
		config.Username = env.username
		config.Password = env.password
	}

	etcdClient, err := clientv3.New(config)
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}

	return &client.EtcdCluster{Client: etcdClient}
}

func TestIntegration_SmokeSkipIfNoEnv(t *testing.T) {
	// Arrange
	env, err := loadIntegrationEnv()
	if err != nil {
		t.Skipf("Skipping integration tests: %v\n\nTo run integration tests, set: SD_ETCD_ENDPOINTS=localhost:2379", err)
	}

	// Assert
	if len(env.endpoints) == 0 {
		t.Skip("No etcd endpoints configured")
	}

	t.Logf("Integration environment loaded: endpoints=%v, basePath=%s", env.endpoints, env.basePath)
}

func TestIntegration_ConnectAndPing(t *testing.T) {
	// Arrange
	env, err := loadIntegrationEnv()
	if err != nil {
		t.Skipf("Skipping: %v", err)
	}

	// Act
	etcdClient := createIntegrationClient(t, env)
	defer etcdClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), env.timeout)
	defer cancel()

	// Act
	_, err = etcdClient.Get(ctx, "/", clientv3.WithLimit(1))

	// Assert
	if err != nil {
		t.Fatalf("Failed to ping etcd: %v", err)
	}

	t.Log("Successfully connected to etcd and verified connectivity")
}

func TestIntegration_WriteAndReadJSON(t *testing.T) {
	// Arrange
	env, err := loadIntegrationEnv()
	if err != nil {
		t.Skipf("Skipping: %v", err)
	}

	// Act
	etcdClient := createIntegrationClient(t, env)
	defer etcdClient.Close()

	logger := log.Default()
	cluster, err := NewEtcdCluster(etcdClient, logger)
	// Assert
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Stop(context.Background())

	testKey := fmt.Sprintf("%s/test-%d", env.basePath, time.Now().UnixNano())
	serviceInfo := &pb.AtappDiscovery{
		Id:       uint64(time.Now().UnixNano()),
		Name:     "integration-test",
		Identity: fmt.Sprintf("test-%d", time.Now().Unix()),
		Hostname: "test-host",
		TypeId:   999,
		TypeName: "test-type",
		Version:  "1.0.0-test",
	}

	ctx, cancel := context.WithTimeout(context.Background(), env.timeout)
	defer cancel()

	// Act
	cluster.ApplyEtcdConfig(&pb.AtappEtcd{Path: env.basePath})
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Act
	err = cluster.RegisterService(ctx, serviceInfo, testKey, 16)

	// Assert
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Act
	resp, err := etcdClient.Get(ctx, testKey)

	// Assert
	if err != nil {
		t.Fatalf("Failed to get key from etcd: %v", err)
	}

	if resp.Count == 0 {
		t.Fatalf("Service key not found: %s", testKey)
	}

	value := resp.Kvs[0].Value
	if !strings.HasPrefix(string(value), "{") {
		t.Errorf("Expected JSON value starting with '{', got: %s", string(value[:min(50, len(value))]))
	}

	if !strings.Contains(string(value), `"identity"`) {
		t.Error("Expected JSON to contain 'identity' field")
	}

	if !strings.Contains(string(value), serviceInfo.Identity) {
		t.Errorf("Expected JSON to contain identity value '%s'", serviceInfo.Identity)
	}

	t.Logf("Successfully wrote and verified JSON format in etcd")

	_, err = etcdClient.Delete(ctx, testKey)
	if err != nil {
		t.Logf("Warning: cleanup failed: %v", err)
	}
}

func TestIntegration_IdentityCollision(t *testing.T) {
	// Arrange
	env, err := loadIntegrationEnv()
	if err != nil {
		t.Skipf("Skipping: %v", err)
	}

	// Act
	etcdClient := createIntegrationClient(t, env)
	defer etcdClient.Close()

	logger := log.Default()

	testKey := fmt.Sprintf("%s/collision-test-%d", env.basePath, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), env.timeout)
	defer cancel()

	cluster1, err := NewEtcdCluster(etcdClient, logger)
	// Assert
	if err != nil {
		t.Fatalf("Failed to create cluster1: %v", err)
	}
	defer cluster1.Stop(ctx)

	serviceInfo1 := &pb.AtappDiscovery{
		Id:       uint64(time.Now().UnixNano()),
		Name:     "collision-test",
		Identity: "identity-A",
		Hostname: "host-a",
	}

	// Act
	cluster1.ApplyEtcdConfig(&pb.AtappEtcd{Path: env.basePath})
	if err := cluster1.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster1: %v", err)
	}

	// Act
	err = cluster1.RegisterService(ctx, serviceInfo1, testKey, 16)

	// Assert
	if err != nil {
		t.Fatalf("Failed to register first service: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Act
	cluster2, err := NewEtcdCluster(etcdClient, logger)
	// Assert
	if err != nil {
		t.Fatalf("Failed to create cluster2: %v", err)
	}
	defer cluster2.Stop(ctx)

	serviceInfo2 := &pb.AtappDiscovery{
		Id:       serviceInfo1.Id,
		Name:     "collision-test",
		Identity: "identity-B",
		Hostname: "host-b",
	}

	// Act
	cluster2.ApplyEtcdConfig(&pb.AtappEtcd{Path: env.basePath})
	if err := cluster2.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster2: %v", err)
	}

	// Act
	err = cluster2.RegisterService(ctx, serviceInfo2, testKey, 16)

	// Assert
	if err == nil {
		t.Fatal("Expected registration to fail due to identity collision")
	}

	t.Logf("Identity collision correctly prevented: %v", err)

	// Act
	_, err = etcdClient.Delete(ctx, testKey)
	if err != nil {
		t.Logf("Warning: cleanup failed: %v", err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
