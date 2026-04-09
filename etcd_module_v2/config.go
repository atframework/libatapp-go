package modulev2

// config.go provides NewEtcdModuleFromConfig helpers that wire up EtcdModule
// directly from a *pb.AtappEtcd proto.

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	log "log/slog"
	"os"
	"time"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/pathbuilder"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NewEtcdModuleFromConfig creates an EtcdModule whose etcd client and path
// configuration are derived from cfg.
//
// Path convention (mirrors etcd_module/module):
//
//	basePath = cfg.GetPath()
//	ByNamePrefix  = basePath + "/by_name"
//	ByIDPrefix    = basePath + "/by_id"
//	TopologyPrefix = basePath + "/topology"
//	WatchPrefixes = [ByIDPrefix, ByNamePrefix, TopologyPrefix]
//
// LeaseTTL is taken from cfg.GetKeepalive().GetTtl() (in seconds), falling
// back to 16 s (etcd_module cluster default) when not set.
//
// RetryInterval is taken from cfg.GetCluster().GetRetryInterval() first, then
// cfg.GetKeepalive().GetRetryInterval() as fallback.
//
// Call Start on the returned *EtcdModule to begin operation.
func NewEtcdModuleFromConfig(cfg *pb.AtappEtcd, logger *log.Logger, opts ModuleOptions) (*EtcdModule, error) {
	return NewEtcdModuleFromConfigWithClient(cfg, nil, logger, opts)
}

// NewEtcdModuleFromConfigWithClient creates an EtcdModule from cfg and allows
// callers to inject an existing clientv3 instance.
//
// If etcdClient is nil, this helper builds a new client from cfg.
func NewEtcdModuleFromConfigWithClient(cfg *pb.AtappEtcd, etcdClient *clientv3.Client, logger *log.Logger, opts ModuleOptions) (*EtcdModule, error) {
	if cfg == nil {
		return nil, errors.New("etcd_module_v2: etcd config is nil")
	}
	if len(cfg.GetHosts()) == 0 {
		return nil, errors.New("etcd_module_v2: etcd hosts list is empty")
	}

	conn := etcdClient
	if conn == nil {
		var err error
		conn, err = NewClientV3FromConfig(cfg, logger)
		if err != nil {
			return nil, err
		}
	}

	basePath := cfg.GetPath()
	leaseTTL := int64(16) // default: 16 s matches etcd_module cluster default

	if kp := cfg.GetKeepalive(); kp != nil {
		if ttl := kp.GetTtl(); ttl != nil {
			if secs := int64(ttl.AsDuration().Seconds()); secs > 0 {
				leaseTTL = secs
			}
		}
	}

	// cluster.retry_interval governs module-level reconnection retry (AtappEtcdCluster.RetryInterval).
	if opts.RetryInterval <= 0 {
		if cl := cfg.GetCluster(); cl != nil {
			if ri := cl.GetRetryInterval(); ri != nil {
				opts.RetryInterval = ri.AsDuration()
			}
		}
	}
	// keepalive.retry_interval as fallback, mirroring etcd_module cluster.ApplyEtcdConfig.
	if opts.RetryInterval <= 0 {
		if kp := cfg.GetKeepalive(); kp != nil {
			if ri := kp.GetRetryInterval(); ri != nil {
				opts.RetryInterval = ri.AsDuration()
			}
		}
	}

	pathCfg := PathConfig{
		ByNamePrefix:   basePath + "/" + pathbuilder.ByNameDir,
		ByIDPrefix:     basePath + "/" + pathbuilder.ByIDDir,
		TopologyPrefix: basePath + "/" + pathbuilder.TopologyDir,
		WatchPrefixes: []string{
			basePath + "/" + pathbuilder.ByIDDir,
			basePath + "/" + pathbuilder.ByNameDir,
			basePath + "/" + pathbuilder.TopologyDir,
		},
		LeaseTTL: leaseTTL,
	}
	return NewEtcdModule(conn, pathCfg, opts), nil
}

// NewClientV3FromConfig builds a go.etcd.io/etcd/client/v3 client from cfg.
func NewClientV3FromConfig(cfg *pb.AtappEtcd, logger *log.Logger) (*clientv3.Client, error) {
	if cfg == nil {
		return nil, errors.New("etcd_module_v2: etcd config is nil")
	}
	if len(cfg.GetHosts()) == 0 {
		return nil, errors.New("etcd_module_v2: etcd hosts list is empty")
	}

	connCfg, err := buildClientConfig(cfg, normalizeLogger(logger))
	if err != nil {
		return nil, err
	}
	return clientv3.New(connCfg)
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
			logger.Error("failed to create TLS configuration", "error", err)
			return clientv3.Config{}, err
		}
		clientConfig.TLS = tlsConfig
	}
	return clientConfig, nil
}

func splitAuth(value string) (string, string, bool) {
	for i := 0; i < len(value); i++ {
		if value[i] == ':' {
			return value[:i], value[i+1:], true
		}
	}
	return "", "", false
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
	case "DISABLED":
		return 0
	default:
		return 0
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
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return fmt.Errorf("append ca cert failed")
	}
	config.RootCAs = caCertPool
	return nil
}
