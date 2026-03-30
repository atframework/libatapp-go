package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	log "log/slog"
	"math/rand"
	"sync"
	"time"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdClusterClient 定义EtcdClusterClient类型。
type EtcdClusterClient struct {
	*clientv3.Client
	Config *pb.AtappEtcd
	logger *log.Logger
}

// ClientCluster 定义ClientCluster类型。
type ClientCluster = EtcdClusterClient

const (
	BackoffFixed       pb.BackoffType = pb.BackoffType_BACKOFF_FIXED
	BackoffLinear      pb.BackoffType = pb.BackoffType_BACKOFF_LINEAR
	BackoffExponential pb.BackoffType = pb.BackoffType_BACKOFF_EXPONENTIAL
)

func splitAuth(value string) (string, string, bool) {
	for i := 0; i < len(value); i++ {
		if value[i] == ':' {
			return value[:i], value[i+1:], true
		}
	}
	return "", "", false
}

func parseTLSMinVersion(value string) uint16 {
	switch value {
	case "TLS_V13", "TLS1.3", "TLS13":
		return tls.VersionTLS13
	case "TLS_V12", "TLS1.2", "TLS12":
		return tls.VersionTLS12
	case "TLS_V11", "TLS1.1", "TLS11":
		return tls.VersionTLS11
	case "TLS_V10", "TLS1.0", "TLS10":
		return tls.VersionTLS10
	case "SSL3":
		return tls.VersionSSL30
	case "DISABLED":
		return 0
	default:
		return 0
	}
}

func NewTLSConfig(certFile, keyFile, caFile, serverName string) *pb.TLSConfig {
	return &pb.TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CaFile:     caFile,
		ServerName: serverName,
		MinVersion: uint32(tls.VersionTLS12),
		MaxVersion: uint32(tls.VersionTLS13),
		ClientAuth: int32(tls.NoClientCert),
	}
}

func NewMTLSConfig(certFile, keyFile, caFile, serverName string) *pb.TLSConfig {
	return &pb.TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CaFile:     caFile,
		ServerName: serverName,
		MinVersion: uint32(tls.VersionTLS12),
		MaxVersion: uint32(tls.VersionTLS13),
		ClientAuth: int32(tls.RequireAndVerifyClientCert),
	}
}

type LeaseKeepaliveEventType int

const (
	LeaseKeepaliveGranted LeaseKeepaliveEventType = iota
	LeaseKeepaliveResponse
	LeaseKeepaliveChannelClosed
	LeaseKeepaliveLeaseNotFound
	LeaseKeepaliveRetryExhausted
	LeaseKeepaliveError
	LeaseKeepaliveStopped
)

// LeaseKeepaliveEvent is emitted by client lease keepalive executor.
// Cluster layer should consume this event stream and decide recovery strategy.
type LeaseKeepaliveEvent struct {
	Type          LeaseKeepaliveEventType
	LeaseID       clientv3.LeaseID
	TTL           int64
	Err           error
	RetryAttempts int
	MaxRetry      int
	At            time.Time
}

type LeaseKeepaliveOptions struct {
	TTL                int64
	EventBuffer        int
	EnableRegrant      bool
	MaxRetry           int
	RetryBackoff       pb.BackoffType
	RetryBaseInterval  time.Duration
	GrantRequestTimout time.Duration
	OnEvent            func(LeaseKeepaliveEvent)
}

// LeaseKeepaliveHandle controls and observes one keepalive execution session.
//
// Concurrency model:
//
//   - Events()    — single-consumer ordered stream; use for logging / stats.
//   - Done()      — broadcast signal (closed channel); any number of goroutines
//     can select on it to learn that the lease is definitively gone.
//   - LeaseChanged() — broadcast signal; re-created on each successful regrant so
//     that actor goroutines can re-register with the new lease ID.
type LeaseKeepaliveHandle struct {
	mu      sync.RWMutex
	leaseID clientv3.LeaseID
	events  chan LeaseKeepaliveEvent
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	once    sync.Once

	// done is closed once when the keepalive session stops permanently.
	// Closing broadcasts to all goroutines that select on Done() simultaneously.
	done     chan struct{}
	doneOnce sync.Once

	// leaseChanged is replaced (closed then recreated) on each successful regrant.
	// Goroutines that need the new lease ID call LeaseChanged(), select on the
	// returned channel, then read the new ID via LeaseID().
	leaseChangedMu sync.RWMutex
	leaseChanged   chan struct{}
}

func (h *LeaseKeepaliveHandle) LeaseID() clientv3.LeaseID {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.leaseID
}

func (h *LeaseKeepaliveHandle) setLeaseID(leaseID clientv3.LeaseID) {
	h.mu.Lock()
	h.leaseID = leaseID
	h.mu.Unlock()

	// Broadcast lease change to all waiting goroutines by closing the old channel
	// and installing a fresh one for the next round.
	h.leaseChangedMu.Lock()
	prev := h.leaseChanged
	h.leaseChanged = make(chan struct{})
	h.leaseChangedMu.Unlock()
	if prev != nil {
		close(prev)
	}
}

// Done returns a channel that is closed once the keepalive session has stopped
// permanently (lease definitively lost, no further regrant will occur).
// Multiple goroutines may select on this channel simultaneously.
func (h *LeaseKeepaliveHandle) Done() <-chan struct{} {
	if h == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return h.done
}

// LeaseChanged returns a channel that is closed whenever a regrant produces a
// new lease ID.  The caller should read LeaseID() after the channel closes to
// obtain the new value.  A new channel is issued for each subsequent regrant.
func (h *LeaseKeepaliveHandle) LeaseChanged() <-chan struct{} {
	if h == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	h.leaseChangedMu.RLock()
	defer h.leaseChangedMu.RUnlock()
	return h.leaseChanged
}

func (h *LeaseKeepaliveHandle) signalDone() {
	h.doneOnce.Do(func() {
		if h.done != nil {
			close(h.done)
		}
	})
}

func (h *LeaseKeepaliveHandle) Events() <-chan LeaseKeepaliveEvent {
	if h == nil {
		return nil
	}
	return h.events
}

// Stop stops the keepalive session and waits for goroutine exit.
func (h *LeaseKeepaliveHandle) Stop(ctx context.Context) error {
	if h == nil {
		return nil
	}

	h.once.Do(func() {
		if h.cancel != nil {
			h.cancel()
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.wg.Wait()
	}()

	if ctx == nil {
		<-done
		return nil
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func NewEtcdClusterClient(cfg *pb.AtappEtcd, logger *log.Logger) (*EtcdClusterClient, error) {
	clusterLogger := normalizeLogger(logger)
	if cfg == nil {
		return nil, fmt.Errorf("etcd config is nil")
	}
	if len(cfg.GetHosts()) == 0 {
		return nil, fmt.Errorf("etcd hosts are empty")
	}

	clientConfig, err := buildClientConfig(cfg, clusterLogger)
	if err != nil {
		return nil, err
	}
	cli, err := clientv3.New(clientConfig)
	if err != nil {
		clusterLogger.Error("Failed to connect to etcd", "error", err, "endpoints", cfg.GetHosts())
		return nil, err
	}
	clusterLogger.Info("Successfully connected to etcd", "endpoints", cfg.GetHosts())

	return &EtcdClusterClient{
		Client: cli,
		Config: cfg,
		logger: clusterLogger,
	}, nil
}

// NewEtcdCluster is a compatibility constructor for existing call-sites.
func NewEtcdCluster(cfg *pb.AtappEtcd, logger *log.Logger) (*EtcdClusterClient, error) {
	return NewEtcdClusterClient(cfg, logger)
}

func createTLSConfig(tlsCfg *pb.TLSConfig) (*tls.Config, error) {
	config := initTLSConfig(tlsCfg)
	if err := addClientCertificate(config, tlsCfg); err != nil {
		return nil, err
	}
	if err := addRootCAs(config, tlsCfg); err != nil {
		return nil, err
	}
	return config, nil
}

func normalizeLogger(logger *log.Logger) *log.Logger {
	if logger == nil {
		return log.Default()
	}
	return logger
}

func buildClientConfig(cfg *pb.AtappEtcd, logger *log.Logger) (clientv3.Config, error) {
	username, password := authCredentials(cfg)
	clientConfig := clientv3.Config{
		Endpoints:   append([]string(nil), cfg.GetHosts()...),
		DialTimeout: dialTimeoutFromAtappEtcd(cfg),
		Username:    username,
		Password:    password,
	}
	applyOptionalClientConfig(&clientConfig, cfg)
	if tlsCfg := tlsConfigFromAtappEtcd(cfg); tlsCfg != nil {
		tlsConfig, err := createTLSConfig(tlsCfg)
		if err != nil {
			logger.Error("Failed to create TLS configuration", "error", err)
			return clientv3.Config{}, err
		}
		clientConfig.TLS = tlsConfig
	}
	if cfg.GetHttp() != nil && cfg.GetHttp().GetDebug() {
		logger.Debug("etcd client debug mode enabled")
	}
	return clientConfig, nil
}

func authCredentials(cfg *pb.AtappEtcd) (string, string) {
	if cfg == nil || cfg.GetAuthorization() == "" {
		return "", ""
	}
	username, password, ok := splitAuth(cfg.GetAuthorization())
	if !ok {
		return "", ""
	}
	return username, password
}

func dialTimeoutFromAtappEtcd(cfg *pb.AtappEtcd) time.Duration {
	if cfg == nil {
		return 5 * time.Second
	}
	if requestCfg := cfg.GetRequest(); requestCfg != nil {
		if requestCfg.GetConnectTimeout() != nil {
			return requestCfg.GetConnectTimeout().AsDuration()
		}
		if requestCfg.GetTimeout() != nil {
			return requestCfg.GetTimeout().AsDuration()
		}
	}
	if initCfg := cfg.GetInit(); initCfg != nil && initCfg.GetTimeout() != nil {
		return initCfg.GetTimeout().AsDuration()
	}
	return 5 * time.Second
}

func applyOptionalClientConfig(clientConfig *clientv3.Config, cfg *pb.AtappEtcd) {
	if cfg == nil {
		return
	}
	if clusterCfg := cfg.GetCluster(); clusterCfg != nil && clusterCfg.GetUpdateInterval() != nil {
		clientConfig.AutoSyncInterval = clusterCfg.GetUpdateInterval().AsDuration()
	}
	if keepaliveCfg := cfg.GetKeepalive(); keepaliveCfg != nil {
		if keepaliveCfg.GetTtl() != nil {
			clientConfig.DialKeepAliveTime = keepaliveCfg.GetTtl().AsDuration()
		}
		if keepaliveCfg.GetTimeout() != nil {
			clientConfig.DialKeepAliveTimeout = keepaliveCfg.GetTimeout().AsDuration()
		}
	}
}

func tlsConfigFromAtappEtcd(cfg *pb.AtappEtcd) *pb.TLSConfig {
	if cfg == nil || cfg.GetSsl() == nil {
		return nil
	}
	sslCfg := cfg.GetSsl()
	return &pb.TLSConfig{
		Enabled:            true,
		CertFile:           sslCfg.GetSslClientCert(),
		KeyFile:            sslCfg.GetSslClientKey(),
		CaFile:             sslCfg.GetSslCaCert(),
		ServerName:         "",
		InsecureSkipVerify: !sslCfg.GetVerifyPeer(),
		MinVersion:         uint32(parseTLSMinVersion(sslCfg.GetSslMinVersion())),
		EnableAlpn:         sslCfg.GetEnableAlpn(),
		CipherSuites:       sslCfg.GetSslCipherList(),
		CipherSuitesTls13:  sslCfg.GetSslCipherListTls13(),
		ClientCertType:     sslCfg.GetSslClientCertType(),
		KeyType:            sslCfg.GetSslClientKeyType(),
		KeyPassword:        sslCfg.GetSslClientKeyPasswd(),
		TlsAuthUsername:    sslCfg.GetSslClientTlsauthUsername(),
		TlsAuthPassword:    sslCfg.GetSslClientTlsauthPassword(),
	}
}

func initTLSConfig(tlsCfg *pb.TLSConfig) *tls.Config {
	config := &tls.Config{
		ServerName:         tlsCfg.ServerName,
		InsecureSkipVerify: tlsCfg.InsecureSkipVerify,
		MinVersion:         uint16(tlsCfg.MinVersion),
		MaxVersion:         uint16(tlsCfg.MaxVersion),
		ClientAuth:         tls.ClientAuthType(tlsCfg.ClientAuth),
	}
	if config.MinVersion == 0 {
		config.MinVersion = tls.VersionTLS12
	}
	if config.ClientAuth == 0 {
		config.ClientAuth = tls.NoClientCert
	}
	return config
}

func addClientCertificate(config *tls.Config, tlsCfg *pb.TLSConfig) error {
	if tlsCfg.CertFile == "" || tlsCfg.KeyFile == "" {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return err
	}
	config.Certificates = []tls.Certificate{cert}
	return nil
}

func addRootCAs(config *tls.Config, tlsCfg *pb.TLSConfig) error {
	if tlsCfg.CaFile == "" {
		return nil
	}
	caCert, err := os.ReadFile(tlsCfg.CaFile)
	if err != nil {
		return err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	config.RootCAs = caCertPool
	return nil
}

func (c *EtcdClusterClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return c.Client.Get(ctx, key, opts...)
}

func (c *EtcdClusterClient) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return c.Client.Delete(ctx, key, opts...)
}

func (c *EtcdClusterClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return c.Client.Grant(ctx, ttl)
}

// StartLeaseKeepalive starts one lease keepalive execution session.
// It only emits execution results; recovery strategy must be handled by cluster layer.
func (c *EtcdClusterClient) StartLeaseKeepalive(ctx context.Context, opts LeaseKeepaliveOptions, keepaliveTimeout time.Duration) (*LeaseKeepaliveHandle, error) {
	if c == nil || c.Client == nil {
		return nil, fmt.Errorf("etcd client is nil")
	}
	return StartLeaseKeepaliveWithLeaser(ctx, c, opts, keepaliveTimeout)
}

// StartLeaseKeepaliveWithLeaser is the testable entry point; any EtcdClient implementation can be injected.
// keepaliveTimeout is obtained from proto config (pb.AtappEtcd.keepalive.timeout); pass 0 to use default.
func StartLeaseKeepaliveWithLeaser(ctx context.Context, leaser EtcdClient, opts LeaseKeepaliveOptions, keepaliveTimeout time.Duration) (*LeaseKeepaliveHandle, error) {
	if leaser == nil {
		return nil, fmt.Errorf("etcd client is nil")
	}
	if opts.TTL <= 0 {
		return nil, fmt.Errorf("lease ttl must be > 0")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.EventBuffer <= 0 {
		opts.EventBuffer = 16
	}
	if opts.MaxRetry <= 0 {
		opts.MaxRetry = 3
	}
	if opts.RetryBaseInterval <= 0 {
		opts.RetryBaseInterval = time.Second
	}
	if opts.GrantRequestTimout <= 0 {
		opts.GrantRequestTimout = 3 * time.Second
	}

	grantResp, err := grantLeaseWithTimeout(ctx, leaser, opts.TTL, opts.GrantRequestTimout)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	handle := &LeaseKeepaliveHandle{
		leaseID:      grantResp.ID,
		events:       make(chan LeaseKeepaliveEvent, opts.EventBuffer),
		cancel:       cancel,
		done:         make(chan struct{}),
		leaseChanged: make(chan struct{}),
	}

	handle.wg.Add(1)
	go runLeaseKeepaliveLoop(runCtx, leaser, handle, opts, keepaliveTimeout)

	handle.emit(context.Background(), LeaseKeepaliveEvent{
		Type:    LeaseKeepaliveGranted,
		LeaseID: grantResp.ID,
		TTL:     grantResp.TTL,
		At:      time.Now(),
	}, opts)

	return handle, nil
}

func grantLeaseWithTimeout(ctx context.Context, leaser EtcdClient, ttl int64, timeout time.Duration) (*clientv3.LeaseGrantResponse, error) {
	grantCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		grantCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	resp, err := leaser.Grant(grantCtx, ttl)
	if cancel != nil {
		cancel()
	}
	return resp, err
}

func runLeaseKeepaliveLoop(ctx context.Context, leaser EtcdClient, handle *LeaseKeepaliveHandle, opts LeaseKeepaliveOptions, keepaliveTimeout time.Duration) {
	defer handle.wg.Done()
	defer handle.signalDone() // broadcast to all Done() waiters before closing events
	defer close(handle.events)

	if keepaliveTimeout <= 0 {
		keepaliveTimeout = 31 * time.Second // default from proto config
	}

	keepAliveChan, ok := beginLeaseKeepaliveStream(ctx, leaser, handle, opts)
	if !ok {
		handle.emit(context.Background(), LeaseKeepaliveEvent{Type: LeaseKeepaliveStopped, LeaseID: handle.LeaseID(), At: time.Now()}, opts)
		return
	}

	// Create a timer to detect keepalive response timeout.
	// Reset this timer each time a response is received.
	keepAliveTimer := time.NewTimer(keepaliveTimeout)
	defer keepAliveTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			handle.emit(context.Background(), LeaseKeepaliveEvent{Type: LeaseKeepaliveStopped, LeaseID: handle.LeaseID(), At: time.Now()}, opts)
			return
		case <-keepAliveTimer.C:
			// Keepalive response timeout detected
			event := LeaseKeepaliveEvent{
				Type:    LeaseKeepaliveError,
				LeaseID: handle.LeaseID(),
				Err:     fmt.Errorf("keepalive response timeout"),
				At:      time.Now(),
			}
			nextChan, continueLoop := recoverLeaseKeepalive(ctx, leaser, handle, opts, event)
			if !continueLoop {
				handle.emit(context.Background(), LeaseKeepaliveEvent{Type: LeaseKeepaliveStopped, LeaseID: handle.LeaseID(), At: time.Now()}, opts)
				return
			}
			if nextChan != nil {
				keepAliveChan = nextChan
				keepAliveTimer.Reset(keepaliveTimeout) // reset timer for new stream
			}
		case resp, ok := <-keepAliveChan:
			// Reset timer on each successful response
			if !keepAliveTimer.Stop() {
				<-keepAliveTimer.C
			}
			keepAliveTimer.Reset(keepaliveTimeout)

			nextChan, continueLoop := consumeKeepaliveResponse(ctx, leaser, handle, opts, resp, ok)
			if !continueLoop {
				handle.emit(context.Background(), LeaseKeepaliveEvent{Type: LeaseKeepaliveStopped, LeaseID: handle.LeaseID(), At: time.Now()}, opts)
				return
			}
			if nextChan != nil {
				keepAliveChan = nextChan
			}
		}
	}
}

func beginLeaseKeepaliveStream(ctx context.Context, leaser EtcdClient, handle *LeaseKeepaliveHandle, opts LeaseKeepaliveOptions) (<-chan *clientv3.LeaseKeepAliveResponse, bool) {
	leaseID := handle.LeaseID()
	keepAliveChan, err := leaser.KeepAlive(ctx, leaseID)
	if err == nil {
		return keepAliveChan, true
	}

	event := LeaseKeepaliveEvent{Type: LeaseKeepaliveError, LeaseID: leaseID, Err: err, At: time.Now()}
	return recoverLeaseKeepalive(ctx, leaser, handle, opts, event)
}

func consumeKeepaliveResponse(ctx context.Context, leaser EtcdClient, handle *LeaseKeepaliveHandle, opts LeaseKeepaliveOptions, resp *clientv3.LeaseKeepAliveResponse, ok bool) (<-chan *clientv3.LeaseKeepAliveResponse, bool) {
	if !ok {
		event := LeaseKeepaliveEvent{Type: LeaseKeepaliveChannelClosed, LeaseID: handle.LeaseID(), At: time.Now()}
		return recoverLeaseKeepalive(ctx, leaser, handle, opts, event)
	}

	if resp == nil {
		event := LeaseKeepaliveEvent{Type: LeaseKeepaliveError, LeaseID: handle.LeaseID(), Err: fmt.Errorf("nil keepalive response"), At: time.Now()}
		return recoverLeaseKeepalive(ctx, leaser, handle, opts, event)
	}

	eventType := LeaseKeepaliveResponse
	if resp.TTL <= 0 {
		eventType = LeaseKeepaliveLeaseNotFound
	}

	event := LeaseKeepaliveEvent{
		Type:    eventType,
		LeaseID: resp.ID,
		TTL:     resp.TTL,
		At:      time.Now(),
	}
	handle.emit(ctx, event, opts)

	if resp.ID != 0 {
		handle.setLeaseID(resp.ID)
	}

	if eventType == LeaseKeepaliveLeaseNotFound {
		return recoverLeaseKeepalive(ctx, leaser, handle, opts, event)
	}

	return nil, true
}

func recoverLeaseKeepalive(ctx context.Context, leaser EtcdClient, handle *LeaseKeepaliveHandle, opts LeaseKeepaliveOptions, event LeaseKeepaliveEvent) (<-chan *clientv3.LeaseKeepAliveResponse, bool) {
	handle.emit(ctx, event, opts)

	if !opts.EnableRegrant {
		return nil, false
	}

	keepAliveChan, leaseID, ttl, attempts, err := regrantLeaseKeepalive(ctx, leaser, opts)
	if err != nil {
		handle.emit(ctx, LeaseKeepaliveEvent{
			Type:          LeaseKeepaliveRetryExhausted,
			LeaseID:       handle.LeaseID(),
			Err:           err,
			RetryAttempts: attempts,
			MaxRetry:      opts.MaxRetry,
			At:            time.Now(),
		}, opts)
		return nil, false
	}

	handle.setLeaseID(leaseID)
	handle.emit(ctx, LeaseKeepaliveEvent{Type: LeaseKeepaliveGranted, LeaseID: leaseID, TTL: ttl, At: time.Now()}, opts)
	return keepAliveChan, true
}

func regrantLeaseKeepalive(ctx context.Context, leaser EtcdClient, opts LeaseKeepaliveOptions) (<-chan *clientv3.LeaseKeepAliveResponse, clientv3.LeaseID, int64, int, error) {
	var lastErr error
	attempts := 0
	retreat := make([]time.Duration, 0, opts.MaxRetry)

	for attempt := 0; attempt < opts.MaxRetry; attempt++ {
		attempts++
		if attempt > 0 {
			delay := computeLeaseRecoveryDelay(opts, attempt, &retreat)
			if err := waitRetryDelay(ctx, delay); err != nil {
				return nil, 0, 0, attempts, err
			}
		}

		resp, err := grantLeaseWithTimeout(ctx, leaser, opts.TTL, opts.GrantRequestTimout)
		if err != nil {
			lastErr = err
			continue
		}

		keepAliveChan, err := leaser.KeepAlive(ctx, resp.ID)
		if err == nil {
			return keepAliveChan, resp.ID, resp.TTL, attempts, nil
		}

		lastErr = err
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("regrant lease failed")
	}
	return nil, 0, 0, attempts, lastErr
}

func computeLeaseRecoveryDelay(opts LeaseKeepaliveOptions, attempt int, retreat *[]time.Duration) time.Duration {
	baseDelay := opts.RetryBaseInterval
	if baseDelay <= 0 {
		baseDelay = time.Second
	}

	delay := computeRetryDelay(opts.RetryBackoff, attempt, baseDelay)
	if opts.RetryBackoff == pb.BackoffType_BACKOFF_EXPONENTIAL {
		*retreat = append(*retreat, delay)
		return (*retreat)[rand.Intn(len(*retreat))]
	}

	if delay <= 0 {
		return 0
	}
	return delay + time.Duration(rand.Int63n(int64(delay/4+1)))
}

func (h *LeaseKeepaliveHandle) emit(ctx context.Context, event LeaseKeepaliveEvent, opts LeaseKeepaliveOptions) {
	if h == nil || h.events == nil {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if opts.OnEvent != nil {
		opts.OnEvent(event)
	}

	select {
	case h.events <- event:
	case <-ctx.Done():
	default:
		// Drop event when consumer lags behind to keep heartbeat recovery progressing.
	}
}

func (c *EtcdClusterClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return c.Client.Watch(ctx, key, opts...)
}

func computeRetryDelay(backoff pb.BackoffType, attempt int, baseDelay time.Duration) time.Duration {
	switch backoff {
	case pb.BackoffType_BACKOFF_FIXED:
		return baseDelay
	case pb.BackoffType_BACKOFF_LINEAR:
		delay := time.Duration(attempt) * baseDelay
		if delay > 30*time.Second {
			return 30 * time.Second
		}
		return delay
	case pb.BackoffType_BACKOFF_EXPONENTIAL:
		delay := time.Duration(1<<uint(attempt)) * baseDelay
		if delay > 30*time.Second {
			return 30 * time.Second
		}
		return delay
	default:
		return baseDelay
	}
}

func waitRetryDelay(ctx context.Context, delay time.Duration) error {
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *EtcdClusterClient) logRetrySuccess(attempt int) {
	if attempt > 0 {
		c.logger.Info("Operation succeeded after retry", "attempt", attempt)
	}
}

func (c *EtcdClusterClient) Close() error {
	if c.Client != nil {
		normalizeLogger(c.logger).Info("Closing etcd client connection")
		return c.Client.Close()
	}
	return nil
}
