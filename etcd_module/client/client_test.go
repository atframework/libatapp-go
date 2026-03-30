package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	log "log/slog"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"github.com/golang/mock/gomock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newTestAtappEtcdConfig(endpoints []string, dialTimeout time.Duration, ssl *pb.AtappEtcdSsl) *pb.AtappEtcd {
	cfg := &pb.AtappEtcd{Hosts: append([]string(nil), endpoints...)}
	if dialTimeout > 0 {
		cfg.Request = &pb.AtappEtcdRequest{ConnectTimeout: durationpb.New(dialTimeout)}
	}
	if ssl != nil {
		cfg.Ssl = ssl
	}
	return cfg
}

func TestNewTLSConfig(t *testing.T) {
	// Act
	tlsConfig := NewTLSConfig("cert.pem", "key.pem", "ca.pem", "etcd.example.com")

	// Assert
	if !tlsConfig.Enabled {
		t.Error("Expected TLS config to be enabled")
	}

	if tlsConfig.CertFile != "cert.pem" {
		t.Errorf("Expected CertFile 'cert.pem', got '%s'", tlsConfig.CertFile)
	}

	if tlsConfig.KeyFile != "key.pem" {
		t.Errorf("Expected KeyFile 'key.pem', got '%s'", tlsConfig.KeyFile)
	}

	if tlsConfig.CaFile != "ca.pem" {
		t.Errorf("Expected CAFile 'ca.pem', got '%s'", tlsConfig.CaFile)
	}

	if tlsConfig.ServerName != "etcd.example.com" {
		t.Errorf("Expected ServerName 'etcd.example.com', got '%s'", tlsConfig.ServerName)
	}

	if tlsConfig.MinVersion != uint32(tls.VersionTLS12) {
		t.Error("Expected MinVersion TLS 1.2")
	}

	if tlsConfig.MaxVersion != uint32(tls.VersionTLS13) {
		t.Error("Expected MaxVersion TLS 1.3")
	}

	if tlsConfig.ClientAuth != int32(tls.NoClientCert) {
		t.Error("Expected ClientAuth NoClientCert")
	}
}

func TestNewMTLSConfig(t *testing.T) {
	// Act
	tlsConfig := NewMTLSConfig("cert.pem", "key.pem", "ca.pem", "etcd.example.com")

	// Assert
	if !tlsConfig.Enabled {
		t.Error("Expected mTLS config to be enabled")
	}

	if tlsConfig.ClientAuth != int32(tls.RequireAndVerifyClientCert) {
		t.Error("Expected ClientAuth RequireAndVerifyClientCert for mTLS")
	}

	if tlsConfig.CertFile != "cert.pem" {
		t.Errorf("Expected CertFile 'cert.pem', got '%s'", tlsConfig.CertFile)
	}

	if tlsConfig.KeyFile != "key.pem" {
		t.Errorf("Expected KeyFile 'key.pem', got '%s'", tlsConfig.KeyFile)
	}
}

func TestClientWithoutTLS(t *testing.T) {
	// Arrange
	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 5*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())

	// Assert
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer cluster.Close()

	if cluster.Client == nil {
		t.Error("Expected client to be created")
	}
}

func TestClientWithTLS(t *testing.T) {
	// Arrange
	sslConfig := &pb.AtappEtcdSsl{VerifyPeer: false}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 5*time.Second, sslConfig)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())

	// Assert
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer cluster.Close()

	if cluster.Client == nil {
		t.Error("Expected client to be created")
	}
}

func TestClientWithInvalidCertFiles(t *testing.T) {
	// Arrange
	sslConfig := &pb.AtappEtcdSsl{
		VerifyPeer:    true,
		SslClientCert: "/nonexistent/cert.pem",
		SslClientKey:  "/nonexistent/key.pem",
		SslCaCert:     "/nonexistent/ca.pem",
		SslMinVersion: "TLS_V12",
	}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 5*time.Second, sslConfig)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())

	// Assert
	if err == nil {
		cluster.Close()
		t.Error("Expected error for invalid cert files")
	}
}

func TestTLSConfigDefaults(t *testing.T) {
	// Arrange
	tlsConfig := &pb.TLSConfig{
		Enabled: true,
	}

	// Assert
	if tlsConfig.MinVersion != 0 {
		t.Errorf("Expected default MinVersion 0, got %d", tlsConfig.MinVersion)
	}

	if tlsConfig.MaxVersion != 0 {
		t.Errorf("Expected default MaxVersion 0, got %d", tlsConfig.MaxVersion)
	}

	if tlsConfig.ClientAuth != 0 {
		t.Errorf("Expected default ClientAuth 0, got %d", tlsConfig.ClientAuth)
	}
}

func TestClientGet(t *testing.T) {
	// Arrange
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 2*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())
	if err != nil {
		t.Skipf("Cannot connect to etcd: %v", err)
		return
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Act
	_, err = cluster.Client.Get(ctx, "/nonexistent")

	// Assert
	if err != nil {
		t.Skipf("Cannot execute Get: %v", err)
	}
}

func TestClientClose(t *testing.T) {
	// Arrange
	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 5*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())
	if err != nil {
		t.Skipf("Cannot connect to etcd: %v", err)
		return
	}

	// Act
	err = cluster.Close()

	// Assert
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestTLSConfigFromEnv(t *testing.T) {
	// Arrange
	os.Setenv("TEST_CERT_FILE", "/path/to/cert.pem")
	os.Setenv("TEST_KEY_FILE", "/path/to/key.pem")
	os.Setenv("TEST_CA_FILE", "/path/to/ca.pem")
	os.Setenv("TEST_SERVER_NAME", "etcd.example.com")
	defer func() {
		os.Unsetenv("TEST_CERT_FILE")
		os.Unsetenv("TEST_KEY_FILE")
		os.Unsetenv("TEST_CA_FILE")
		os.Unsetenv("TEST_SERVER_NAME")
	}()

	certFile := os.Getenv("TEST_CERT_FILE")
	keyFile := os.Getenv("TEST_KEY_FILE")
	caFile := os.Getenv("TEST_CA_FILE")
	serverName := os.Getenv("TEST_SERVER_NAME")

	// Act
	tlsConfig := NewTLSConfig(certFile, keyFile, caFile, serverName)

	// Assert
	if tlsConfig.CertFile != certFile {
		t.Errorf("Expected cert file from env")
	}
}

func TestConfigWithEmptyEndpoints(t *testing.T) {
	// Arrange
	cfg := newTestAtappEtcdConfig([]string{}, 5*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())

	// Assert
	if err == nil {
		cluster.Close()
		t.Error("Expected error for empty endpoints")
	}
}

func TestClientLeaseGrant(t *testing.T) {
	// Arrange
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 2*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())
	if err != nil {
		t.Skipf("Cannot connect to etcd: %v", err)
		return
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act
	resp, err := cluster.Grant(ctx, 60)
	if err != nil {
		t.Skipf("Cannot execute Grant: %v", err)
		return
	}

	// Assert
	if resp == nil {
		t.Error("Expected non-nil LeaseGrantResponse")
		return
	}

	if resp.ID == 0 {
		t.Error("Expected valid lease ID, got 0")
	}

	if resp.TTL <= 0 {
		t.Errorf("Expected positive TTL, got %d", resp.TTL)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	// Act
	cluster.Revoke(ctx2, resp.ID)
}

func TestClientLeaseRevoke(t *testing.T) {
	// Arrange
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 2*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())
	if err != nil {
		t.Skipf("Cannot connect to etcd: %v", err)
		return
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act
	grantResp, err := cluster.Grant(ctx, 60)
	if err != nil {
		t.Skipf("Cannot execute Grant: %v", err)
		return
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	// Act
	revokeResp, err := cluster.Revoke(ctx2, grantResp.ID)
	if err != nil {
		t.Skipf("Cannot execute Revoke: %v", err)
		return
	}

	// Assert
	if revokeResp == nil {
		t.Error("Expected non-nil LeaseRevokeResponse")
	}
}

func TestClientLeaseKeepAlive(t *testing.T) {
	// Arrange
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 2*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())
	if err != nil {
		t.Skipf("Cannot connect to etcd: %v", err)
		return
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act
	grantResp, err := cluster.Grant(ctx, 60)
	if err != nil {
		t.Skipf("Cannot execute Grant: %v", err)
		return
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	// Act
	keepAliveChan, err := cluster.KeepAlive(ctx2, grantResp.ID)
	if err != nil {
		t.Skipf("Cannot execute KeepAlive: %v", err)
		return
	}

	// Assert
	if keepAliveChan == nil {
		t.Error("Expected non-nil KeepAlive channel")
		return
	}

	select {
	case resp, ok := <-keepAliveChan:
		if !ok {
			t.Error("KeepAlive channel closed unexpectedly")
		}
		if resp != nil && resp.TTL <= 0 {
			t.Errorf("Expected positive TTL in KeepAlive response, got %d", resp.TTL)
		}
	case <-time.After(2 * time.Second):
	}

	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	// Act
	cluster.Revoke(ctx3, grantResp.ID)
}

func TestClientLeaseGrantWithZeroTTL(t *testing.T) {
	// Arrange
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 2*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())
	if err != nil {
		t.Skipf("Cannot connect to etcd: %v", err)
		return
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act
	resp, err := cluster.Grant(ctx, 0)

	// Assert
	if err == nil && resp != nil {
		if resp.TTL <= 0 {
			t.Error("Expected positive TTL after Grant with 0 TTL")
		}
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()
		// Act
		cluster.Revoke(ctx2, resp.ID)
	}
}

func TestClientLeaseGrantWithLargeTTL(t *testing.T) {
	// Arrange
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 2*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())
	if err != nil {
		t.Skipf("Cannot connect to etcd: %v", err)
		return
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act
	resp, err := cluster.Grant(ctx, 3600)
	if err != nil {
		t.Skipf("Cannot execute Grant with large TTL: %v", err)
		return
	}

	// Assert
	if resp == nil {
		t.Error("Expected non-nil LeaseGrantResponse")
		return
	}

	if resp.TTL <= 0 {
		t.Errorf("Expected positive TTL, got %d", resp.TTL)
	}

	if resp.TTL > 3600 {
		t.Errorf("Expected TTL <= 3600, got %d", resp.TTL)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	// Act
	cluster.Revoke(ctx2, resp.ID)
}

func TestClientLeaseRevokeInvalidID(t *testing.T) {
	// Arrange
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := newTestAtappEtcdConfig([]string{"localhost:2379"}, 2*time.Second, nil)

	// Act
	cluster, err := NewEtcdCluster(cfg, log.Default())
	if err != nil {
		t.Skipf("Cannot connect to etcd: %v", err)
		return
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act
	revokeResp, err := cluster.Revoke(ctx, 99999999)

	// Assert
	if err == nil && revokeResp == nil {
		t.Error("Expected non-nil LeaseRevokeResponse even for invalid ID")
	}
}

func TestStartLeaseKeepalive_InvalidTTL(t *testing.T) {
	cluster := &EtcdClusterClient{logger: log.Default()}

	_, err := cluster.StartLeaseKeepalive(context.Background(), LeaseKeepaliveOptions{TTL: 0}, 31*time.Second)
	if err == nil {
		t.Fatalf("expected invalid ttl error")
	}
}

func TestLeaseKeepaliveHandle_StopIsIdempotent(t *testing.T) {
	h := &LeaseKeepaliveHandle{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := h.Stop(ctx); err != nil {
		t.Fatalf("first Stop failed: %v", err)
	}
	if err := h.Stop(ctx); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}
}

// collectEvents drains the events channel (closed by loop goroutine) into a slice.
func collectEvents(handle *LeaseKeepaliveHandle) []LeaseKeepaliveEvent {
	var result []LeaseKeepaliveEvent
	for ev := range handle.Events() {
		result = append(result, ev)
	}
	return result
}

// TestStartLeaseKeepaliveWithLeaser_KeepAliveError verifies that a KeepAlive-stream error
// emits LeaseKeepaliveError then LeaseKeepaliveStopped when EnableRegrant is false.
func TestStartLeaseKeepaliveWithLeaser_KeepAliveError(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLeaser := mocks.NewMockEtcdClient(ctrl)
	leaseID := clientv3.LeaseID(12345)

	mockLeaser.EXPECT().Grant(gomock.Any(), int64(5)).
		Return(&clientv3.LeaseGrantResponse{ID: leaseID, TTL: 5}, nil).Times(1)
	mockLeaser.EXPECT().KeepAlive(gomock.Any(), leaseID).
		Return(nil, fmt.Errorf("connection refused")).Times(1)

	// Act
	handle, err := StartLeaseKeepaliveWithLeaser(context.Background(), mockLeaser, LeaseKeepaliveOptions{
		TTL:           5,
		EventBuffer:   16,
		EnableRegrant: false,
	}, 31*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := collectEvents(handle)

	// Assert
	types := make(map[LeaseKeepaliveEventType]int)
	for _, ev := range events {
		types[ev.Type]++
	}
	if types[LeaseKeepaliveGranted] == 0 {
		t.Errorf("expected LeaseKeepaliveGranted, got events: %v", types)
	}
	if types[LeaseKeepaliveError] == 0 {
		t.Errorf("expected LeaseKeepaliveError, got events: %v", types)
	}
	if types[LeaseKeepaliveStopped] == 0 {
		t.Errorf("expected LeaseKeepaliveStopped, got events: %v", types)
	}
}

// TestStartLeaseKeepaliveWithLeaser_ChannelClosed verifies the channel-close path emits stop when regrant is disabled.
func TestStartLeaseKeepaliveWithLeaser_ChannelClosed(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLeaser := mocks.NewMockEtcdClient(ctrl)
	leaseID := clientv3.LeaseID(22222)

	kaCh := make(chan *clientv3.LeaseKeepAliveResponse)
	close(kaCh) // closed immediately → simulates server drop

	mockLeaser.EXPECT().Grant(gomock.Any(), int64(3)).
		Return(&clientv3.LeaseGrantResponse{ID: leaseID, TTL: 3}, nil).Times(1)
	mockLeaser.EXPECT().KeepAlive(gomock.Any(), leaseID).
		Return(kaCh, nil).Times(1)

	// Act
	handle, err := StartLeaseKeepaliveWithLeaser(context.Background(), mockLeaser, LeaseKeepaliveOptions{
		TTL:           3,
		EventBuffer:   16,
		EnableRegrant: false,
	}, 31*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := collectEvents(handle)

	// Assert
	types := make(map[LeaseKeepaliveEventType]int)
	for _, ev := range events {
		types[ev.Type]++
	}
	if types[LeaseKeepaliveChannelClosed] == 0 {
		t.Errorf("expected LeaseKeepaliveChannelClosed, got events: %v", types)
	}
	if types[LeaseKeepaliveStopped] == 0 {
		t.Errorf("expected LeaseKeepaliveStopped, got events: %v", types)
	}
}

func TestStartLeaseKeepaliveWithLeaser_ChannelClosed_Regrant(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLeaser := mocks.NewMockEtcdClient(ctrl)
	leaseID1 := clientv3.LeaseID(33331)
	leaseID2 := clientv3.LeaseID(33332)

	kaCh1 := make(chan *clientv3.LeaseKeepAliveResponse)
	close(kaCh1)
	kaCh2 := make(chan *clientv3.LeaseKeepAliveResponse)

	gomock.InOrder(
		mockLeaser.EXPECT().Grant(gomock.Any(), int64(3)).Return(&clientv3.LeaseGrantResponse{ID: leaseID1, TTL: 3}, nil),
		mockLeaser.EXPECT().KeepAlive(gomock.Any(), leaseID1).Return(kaCh1, nil),
		mockLeaser.EXPECT().Grant(gomock.Any(), int64(3)).Return(&clientv3.LeaseGrantResponse{ID: leaseID2, TTL: 3}, nil),
		mockLeaser.EXPECT().KeepAlive(gomock.Any(), leaseID2).Return(kaCh2, nil),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu        sync.Mutex
		allEvents []LeaseKeepaliveEvent
	)
	regrantSeen := make(chan struct{}, 1)

	handle, err := StartLeaseKeepaliveWithLeaser(ctx, mockLeaser, LeaseKeepaliveOptions{
		TTL:           3,
		EventBuffer:   16,
		EnableRegrant: true,
		MaxRetry:      1,
		OnEvent: func(ev LeaseKeepaliveEvent) {
			mu.Lock()
			allEvents = append(allEvents, ev)
			mu.Unlock()
			if ev.Type == LeaseKeepaliveGranted && ev.LeaseID == leaseID2 {
				select {
				case regrantSeen <- struct{}{}:
				default:
				}
			}
		},
	}, 31*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-regrantSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for regrant after channel close")
	}

	cancel()
	_ = handle.Stop(context.Background())
	collectEvents(handle)

	mu.Lock()
	snapshot := append([]LeaseKeepaliveEvent(nil), allEvents...)
	mu.Unlock()

	var sawChannelClosed, sawRegrant bool
	for _, ev := range snapshot {
		if ev.Type == LeaseKeepaliveChannelClosed {
			sawChannelClosed = true
		}
		if ev.Type == LeaseKeepaliveGranted && ev.LeaseID == leaseID2 {
			sawRegrant = true
		}
	}
	if !sawChannelClosed {
		t.Fatalf("expected channel closed event, got %v", snapshot)
	}
	if !sawRegrant {
		t.Fatalf("expected regrant after channel close, got %v", snapshot)
	}
}

// TestStartLeaseKeepaliveWithLeaser_LeaseNotFound_Regrant verifies that when TTL<=0 is returned
// and EnableRegrant=true, the loop regranting issues a new Grant and continues.
func TestStartLeaseKeepaliveWithLeaser_LeaseNotFound_Regrant(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLeaser := mocks.NewMockEtcdClient(ctrl)
	leaseID1 := clientv3.LeaseID(11111)
	leaseID2 := clientv3.LeaseID(22222)

	// first Grant (initial)
	mockLeaser.EXPECT().Grant(gomock.Any(), int64(3)).
		Return(&clientv3.LeaseGrantResponse{ID: leaseID1, TTL: 3}, nil).Times(1)

	kaCh1 := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	// TTL=0 → LeaseNotFound
	kaCh1 <- &clientv3.LeaseKeepAliveResponse{ID: leaseID1, TTL: 0}
	close(kaCh1)
	mockLeaser.EXPECT().KeepAlive(gomock.Any(), leaseID1).
		Return(kaCh1, nil).Times(1)

	// regrant Grant (after lease not found)
	mockLeaser.EXPECT().Grant(gomock.Any(), int64(3)).
		Return(&clientv3.LeaseGrantResponse{ID: leaseID2, TTL: 3}, nil).Times(1)

	// second KeepAlive with new lease — return a blocking channel so loop stays alive
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kaCh2 := make(chan *clientv3.LeaseKeepAliveResponse)
	mockLeaser.EXPECT().KeepAlive(gomock.Any(), leaseID2).
		Return(kaCh2, nil).Times(1)

	// Collect all events via OnEvent callback (fires before channel send, no double-drain)
	var (
		mu        sync.Mutex
		allEvents []LeaseKeepaliveEvent
	)
	regrantSeen := make(chan struct{}, 1)

	// Act
	handle, err := StartLeaseKeepaliveWithLeaser(ctx, mockLeaser, LeaseKeepaliveOptions{
		TTL:                3,
		EventBuffer:        32,
		EnableRegrant:      true,
		MaxRetry:           1,
		GrantRequestTimout: time.Second,
		OnEvent: func(ev LeaseKeepaliveEvent) {
			mu.Lock()
			allEvents = append(allEvents, ev)
			mu.Unlock()
			if ev.Type == LeaseKeepaliveGranted && ev.LeaseID == leaseID2 {
				select {
				case regrantSeen <- struct{}{}:
				default:
				}
			}
		},
	}, 31*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for regrant to be observed, then stop
	select {
	case <-regrantSeen:
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for regrant Granted event")
	}
	cancel()
	_ = handle.Stop(context.Background())
	// drain remaining channel events
	collectEvents(handle)

	// Assert
	mu.Lock()
	snapshot := append([]LeaseKeepaliveEvent(nil), allEvents...)
	mu.Unlock()

	var sawInitialGrant, sawLeaseNotFound, sawRegrant bool
	for _, ev := range snapshot {
		switch {
		case ev.Type == LeaseKeepaliveGranted && ev.LeaseID == leaseID1:
			sawInitialGrant = true
		case ev.Type == LeaseKeepaliveLeaseNotFound:
			sawLeaseNotFound = true
		case ev.Type == LeaseKeepaliveGranted && ev.LeaseID == leaseID2:
			sawRegrant = true
		}
	}
	if !sawInitialGrant {
		t.Errorf("expected initial Granted event for leaseID1; events: %v", snapshot)
	}
	if !sawLeaseNotFound {
		t.Errorf("expected LeaseNotFound event; events: %v", snapshot)
	}
	if !sawRegrant {
		t.Errorf("expected regrant Granted event for leaseID2; events: %v", snapshot)
	}
	if got := handle.LeaseID(); got != leaseID2 {
		t.Errorf("expected handle.LeaseID()=%d after regrant, got %d", leaseID2, got)
	}
}

// TestStartLeaseKeepaliveWithLeaser_EnableRegrantFalse_Stops verifies that disabling
// regrant causes keepalive loop to stop immediately on stream setup failure.
func TestStartLeaseKeepaliveWithLeaser_EnableRegrantFalse_Stops(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLeaser := mocks.NewMockEtcdClient(ctrl)
	leaseID := clientv3.LeaseID(55555)

	mockLeaser.EXPECT().Grant(gomock.Any(), int64(5)).
		Return(&clientv3.LeaseGrantResponse{ID: leaseID, TTL: 5}, nil).Times(1)
	mockLeaser.EXPECT().KeepAlive(gomock.Any(), leaseID).
		Return(nil, fmt.Errorf("connection refused")).Times(1)

	// Act
	handle, err := StartLeaseKeepaliveWithLeaser(context.Background(), mockLeaser, LeaseKeepaliveOptions{
		TTL:           5,
		EventBuffer:   16,
		EnableRegrant: false,
	}, 31*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := collectEvents(handle)

	// Assert: no Grant call besides the initial one (i.e. no regrant)
	types := make(map[LeaseKeepaliveEventType]int)
	for _, ev := range events {
		types[ev.Type]++
	}
	if types[LeaseKeepaliveStopped] == 0 {
		t.Errorf("expected LeaseKeepaliveStopped, got events: %v", types)
	}
	if types[LeaseKeepaliveGranted] != 1 {
		t.Errorf("expected exactly 1 Granted (initial), got %d", types[LeaseKeepaliveGranted])
	}
}

func TestStartLeaseKeepaliveWithLeaser_RegrantRetryExhausted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLeaser := mocks.NewMockEtcdClient(ctrl)
	leaseID := clientv3.LeaseID(88888)

	gomock.InOrder(
		mockLeaser.EXPECT().Grant(gomock.Any(), int64(5)).
			Return(&clientv3.LeaseGrantResponse{ID: leaseID, TTL: 5}, nil),
		mockLeaser.EXPECT().KeepAlive(gomock.Any(), leaseID).
			Return(nil, fmt.Errorf("connection refused")),
		mockLeaser.EXPECT().Grant(gomock.Any(), int64(5)).
			Return(nil, fmt.Errorf("connection refused")),
		mockLeaser.EXPECT().Grant(gomock.Any(), int64(5)).
			Return(nil, fmt.Errorf("connection refused")),
	)

	handle, err := StartLeaseKeepaliveWithLeaser(context.Background(), mockLeaser, LeaseKeepaliveOptions{
		TTL:               5,
		EventBuffer:       32,
		EnableRegrant:     true,
		MaxRetry:          2,
		RetryBaseInterval: time.Millisecond,
	}, 31*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := collectEvents(handle)
	var exhausted *LeaseKeepaliveEvent
	for i := range events {
		if events[i].Type == LeaseKeepaliveRetryExhausted {
			exhausted = &events[i]
			break
		}
	}

	if exhausted == nil {
		t.Fatalf("expected LeaseKeepaliveRetryExhausted event, got: %v", events)
	}
	if exhausted.RetryAttempts != 2 {
		t.Fatalf("expected RetryAttempts=2, got %d", exhausted.RetryAttempts)
	}
	if exhausted.MaxRetry != 2 {
		t.Fatalf("expected MaxRetry=2, got %d", exhausted.MaxRetry)
	}
}
