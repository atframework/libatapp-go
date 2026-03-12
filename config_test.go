package libatapp

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	atframe_protocol "github.com/atframework/libatapp-go/protocol/atframe"
	"github.com/stretchr/testify/assert"
)

// 示例模块实现
type ExampleModule struct {
	AppModuleBase

	name   string
	inited bool
}

func NewExampleModule(name string) *ExampleModule {
	return &ExampleModule{
		name: name,
	}
}

func (m *ExampleModule) Name() string {
	return m.name
}

func (m *ExampleModule) OnBind() {
	fmt.Printf("Module %s: OnBind called\n", m.name)
}

func (m *ExampleModule) OnUnbind() {
	fmt.Printf("Module %s: OnUnbind called\n", m.name)
}

func (m *ExampleModule) Setup(parent context.Context) error {
	fmt.Printf("Module %s: Setup called\n", m.name)
	return nil
}

func (m *ExampleModule) SetupLog(parent context.Context) error {
	fmt.Printf("Module %s: SetupLog called\n", m.name)
	return nil
}

func (m *ExampleModule) Init(parent context.Context) error {
	fmt.Printf("Module %s: Init called\n", m.name)
	m.inited = true
	return nil
}

func (m *ExampleModule) Ready() {
	fmt.Printf("Module %s: Ready called\n", m.name)
}

func (m *ExampleModule) Reload() error {
	fmt.Printf("Module %s: Reload called\n", m.name)
	return nil
}

func (m *ExampleModule) Stop() (bool, error) {
	fmt.Printf("Module %s: Stop called\n", m.name)
	return true, nil
}

func (m *ExampleModule) Cleanup() {
	fmt.Printf("Module %s: Cleanup called\n", m.name)
	m.inited = false
}

func (m *ExampleModule) Timeout() {
	fmt.Printf("Module %s: Timeout called\n", m.name)
}

func (m *ExampleModule) Tick(parent context.Context) bool {
	fmt.Printf("Module %s: Tick called\n", m.name)
	return false
}

// loadEnvFile 从文件加载环境变量，返回已设置的环境变量key列表以便清理
func loadEnvFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open env file: %w", err)
	}
	defer file.Close()

	var keys []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// 跳过空行和注释
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "#") {
			continue
		}

		// 解析 KEY=VALUE 格式
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		// 保留 value 的尾部空格，只截断左边的空格
		value := strings.TrimLeft(parts[1], " \t")

		if err := os.Setenv(key, value); err != nil {
			return keys, fmt.Errorf("failed to set env %s: %w", key, err)
		}
		keys = append(keys, key)
	}

	if err := scanner.Err(); err != nil {
		return keys, fmt.Errorf("error reading env file: %w", err)
	}

	return keys, nil
}

// clearEnvVars 清理指定的环境变量
func clearEnvVars(keys []string) {
	for _, key := range keys {
		os.Unsetenv(key)
	}
}

// verifyRootFields 验证根级别字段
func verifyRootFields(t *testing.T, cfg *atframe_protocol.AtappConfigure) {
	t.Helper()
	assert := assert.New(t)
	assert.Equal("0x00001234", cfg.GetId(), "atapp.id should match")
	assert.Equal("sample_echo_svr-1", cfg.GetName(), "atapp.name should match")
	assert.Equal(uint64(1), cfg.GetTypeId(), "atapp.type_id should match")
	assert.Equal("sample_echo_svr", cfg.GetTypeName(), "atapp.type_name should match")
}

// verifyBusConfig 验证 bus 配置
func verifyBusConfig(t *testing.T, cfg *atframe_protocol.AtappConfigure) {
	t.Helper()
	assert := assert.New(t)
	bus := cfg.GetBus()
	if !assert.NotNil(bus, "bus config should not be nil") {
		return
	}

	assert.Equal([]string{"atcp://:::21437"}, bus.GetListen(), "bus.listen should contain single endpoint")
	assert.Equal("", bus.GetProxy(), "bus.proxy should be empty string")
	assert.Equal(int32(256), bus.GetBacklog(), "bus.backlog should match")
	assert.Equal(uint64(5), bus.GetAccessTokenMaxNumber(), "bus.access_token_max_number should match")
	if assert.NotNil(bus.GetFirstIdleTimeout(), "first_idle_timeout should be parsed") {
		assert.Equal(30*time.Second, bus.GetFirstIdleTimeout().AsDuration(), "bus.first_idle_timeout should match")
	}
	if assert.NotNil(bus.GetPingInterval(), "ping_interval should be parsed") {
		assert.Equal(60*time.Second, bus.GetPingInterval().AsDuration(), "bus.ping_interval should match")
	}
	if assert.NotNil(bus.GetRetryInterval(), "retry_interval should be parsed") {
		assert.Equal(3*time.Second, bus.GetRetryInterval().AsDuration(), "bus.retry_interval should match")
	}
	assert.Equal(uint64(3), bus.GetFaultTolerant(), "bus.fault_tolerant should match")
	assert.Equal(uint64(256*1024), bus.GetMessageSize(), "bus.message_size should match")
	assert.Equal(uint64(8*1024*1024), bus.GetReceiveBufferSize(), "bus.receive_buffer_size should match")
	assert.Equal(uint64(2*1024*1024), bus.GetSendBufferSize(), "bus.send_buffer_size should match")
	assert.Equal(uint64(0), bus.GetSendBufferNumber(), "bus.send_buffer_number should match")

	topology := bus.GetTopology()
	if assert.NotNil(topology, "bus.topology should not be nil") {
		rule := topology.GetRule()
		if assert.NotNil(rule, "bus.topology.rule should not be nil") {
			assert.True(rule.GetAllowDirectConnection(), "bus.topology.rule.allow_direct_connection should match")
			assert.True(rule.GetRequireSameUpstream(), "bus.topology.rule.require_same_upstream should match")
			assert.False(rule.GetRequireSameProcess(), "bus.topology.rule.require_same_process should match")
			assert.True(rule.GetRequireSameHost(), "bus.topology.rule.require_same_host should match")
			ruleLabels := rule.GetMatchLabel()
			if assert.Len(ruleLabels, 2, "bus.topology.rule.match_label size should match") {
				assert.Equal([]string{"cn", "us"}, ruleLabels["region"].GetValue(), "bus.topology.rule.match_label.zone should match")
				assert.Equal([]string{"test"}, ruleLabels["env"].GetValue(), "bus.topology.rule.match_label.env should match")
			}
		}

		data := topology.GetData()
		if assert.NotNil(data, "bus.topology.data should not be nil") {
			dataLabels := data.GetLabel()
			if assert.Len(dataLabels, 2, "bus.topology.data.label size should match") {
				assert.Equal("cn", dataLabels["region"], "bus.topology.data.label.region should match")
				assert.Equal("test", dataLabels["env"], "bus.topology.data.label.env should match")
			}
		}
	}

	crypto := bus.GetCrypto()
	if assert.NotNil(crypto, "bus.crypto should not be nil") {
		assert.Equal(atframe_protocol.ATAPP_CRYPTO_KEY_EXCHANGE_TYPE_ATAPP_CRYPTO_KEY_EXCHANGE_X25519, crypto.GetKeyExchangeType(),
			"bus.crypto.key_exchange_type should match")
		if assert.NotNil(crypto.GetKeyRefreshInterval(), "bus.crypto.key_refresh_interval should be parsed") {
			assert.Equal(5*time.Second, crypto.GetKeyRefreshInterval().AsDuration(), "bus.crypto.key_refresh_interval should match")
		}
		assert.Equal([]atframe_protocol.ATAPP_CRYPTO_ALGORITHM_TYPE{
			atframe_protocol.ATAPP_CRYPTO_ALGORITHM_TYPE_ATAPP_CRYPTO_ALGORITHM_AES_128_GCM,
			atframe_protocol.ATAPP_CRYPTO_ALGORITHM_TYPE_ATAPP_CRYPTO_ALGORITHM_CHACHA20_POLY1305_IETF,
			atframe_protocol.ATAPP_CRYPTO_ALGORITHM_TYPE_ATAPP_CRYPTO_ALGORITHM_XCHACHA20_POLY1305_IETF,
		}, crypto.GetAlgorithm(), "bus.crypto.algorithm should match")
	}

	compression := bus.GetCompression()
	if assert.NotNil(compression, "bus.compression should not be nil") {
		assert.Equal([]atframe_protocol.ATAPP_COMPRESSION_ALGORITHM_TYPE{
			atframe_protocol.ATAPP_COMPRESSION_ALGORITHM_TYPE_ATAPP_COMPRESSION_ALGORITHM_ZSTD,
			atframe_protocol.ATAPP_COMPRESSION_ALGORITHM_TYPE_ATAPP_COMPRESSION_ALGORITHM_LZ4,
			atframe_protocol.ATAPP_COMPRESSION_ALGORITHM_TYPE_ATAPP_COMPRESSION_ALGORITHM_ZLIB,
		}, compression.GetAlgorithm(), "bus.compression.algorithm should match")
		assert.Equal(atframe_protocol.ATAPP_COMPRESSION_LEVEL_ATAPP_COMPRESSION_LEVEL_FAST, compression.GetLevel(),
			"bus.compression.level should match")
	}

	// 默认值验证
	assert.Equal(int32(1000), bus.GetLoopTimes(), "bus.loop_times should be 1000 by default")
}

// verifyTimerConfig 验证 timer 配置
func verifyTimerConfig(t *testing.T, cfg *atframe_protocol.AtappConfigure) {
	t.Helper()
	assert := assert.New(t)
	timer := cfg.GetTimer()
	if !assert.NotNil(timer, "timer config should not be nil") {
		return
	}

	if assert.NotNil(timer.GetTickInterval(), "tick_interval should be parsed") {
		assert.Equal(32*time.Millisecond, timer.GetTickInterval().AsDuration(), "timer.tick_interval should match")
	}
	if assert.NotNil(timer.GetStopTimeout(), "stop_timeout should be parsed") {
		assert.Equal(10*time.Second, timer.GetStopTimeout().AsDuration(), "timer.stop_timeout should match")
	}
	if assert.NotNil(timer.GetStopInterval(), "stop_interval should be parsed") {
		assert.Equal(256*time.Millisecond, timer.GetStopInterval().AsDuration(), "timer.stop_interval should match")
	}
}

// verifyEtcdConfig 验证 etcd 配置
func verifyEtcdConfig(t *testing.T, etcd *atframe_protocol.AtappEtcd) {
	t.Helper()
	assert := assert.New(t)
	if !assert.NotNil(etcd, "etcd config should not be nil") {
		return
	}

	assert.False(etcd.GetEnable(), "etcd.enable should be false")
	assert.Equal([]string{"http://127.0.0.1:2375", "http://127.0.0.1:2376", "http://127.0.0.1:2377"}, etcd.GetHosts(), "etcd.hosts should match order")
	assert.Equal("/atapp/services/astf4g/", etcd.GetPath(), "etcd.path should match")
	assert.Equal("", etcd.GetAuthorization(), "etcd.authorization should be empty string")
	if initCfg := etcd.GetInit(); assert.NotNil(initCfg, "etcd.init should not be nil") {
		if assert.NotNil(initCfg.GetTickInterval(), "etcd.init.tick_interval should be parsed") {
			assert.Equal(16*time.Millisecond, initCfg.GetTickInterval().AsDuration(), "etcd.init.tick_interval should match min bound")
		}
		// etcd.init.timeout is commented out in yaml, so it uses default value from proto extension
		// We only verify it's not nil if it exists
		// Note: The default value comes from proto extension, which may or may not be set
	}
}

// verifyLogConfig 验证 log 配置
func verifyLogConfig(t *testing.T, logCfg *atframe_protocol.AtappLog) {
	t.Helper()
	assert := assert.New(t)
	if !assert.NotNil(logCfg, "log config should not be nil") {
		return
	}

	assert.Equal("debug", logCfg.GetLevel(), "log.level should match")
	categories := logCfg.GetCategory()
	assert.Len(categories, 2, "log.category length should match")

	for _, cat := range categories {
		switch cat.GetName() {
		case "db":
			assert.Equal(int32(1), cat.GetIndex(), "db category index should match")
			assert.Equal("[Log %L][%F %T.%f]: ", cat.GetPrefix(), "db prefix should match")
			if stack := cat.GetStacktrace(); assert.NotNil(stack) {
				assert.Equal("disable", stack.GetMin(), "db stacktrace.min should match")
				assert.Equal("disable", stack.GetMax(), "db stacktrace.max should match")
			}
		case "default":
			assert.Equal(int32(0), cat.GetIndex(), "default category index should match")
			assert.Equal("[Log %L][%F %T.%f][%s:%n(%C)]: ", cat.GetPrefix(), "default prefix should match")
			if stack := cat.GetStacktrace(); assert.NotNil(stack) {
				assert.Equal("error", stack.GetMin(), "default stacktrace.min should match")
				assert.Equal("fatal", stack.GetMax(), "default stacktrace.max should match")
			}
			sinks := cat.GetSink()
			assert.Len(sinks, 4, "default category sinks should match count")
			type sinkKey struct {
				typeName string
				fileName string
			}
			found := map[sinkKey]bool{}
			for _, sink := range sinks {
				switch sink.GetType() {
				case "file":
					fileBackend := sink.GetLogBackendFile()
					if assert.NotNil(fileBackend, "file backend should exist") {
						rotate := fileBackend.GetRotate()
						if assert.NotNil(rotate) {
							assert.Equal(uint32(10), rotate.GetNumber(), "file rotate.number should match")
							assert.Equal(uint64(10485760), rotate.GetSize(), "file rotate.size should match")
						}
						key := sinkKey{typeName: "file", fileName: fileBackend.GetFile()}
						found[key] = true
						switch fileBackend.GetFile() {
						case "../log/sample_echo_svr.error.%N.log":
							assert.Equal("../log/sample_echo_svr.error.log", fileBackend.GetWritingAlias(), "error file alias should match")
						case "../log/sample_echo_svr.all.%N.log":
							assert.Equal("../log/sample_echo_svr.all.log", fileBackend.GetWritingAlias(), "all file alias should match")
						default:
							t.Errorf("unexpected file sink %s", fileBackend.GetFile())
						}
					}
				case "stderr", "stdout":
					found[sinkKey{typeName: sink.GetType()}] = true
				default:
					t.Errorf("unexpected sink type %s", sink.GetType())
				}
			}
			assert.True(found[sinkKey{typeName: "file", fileName: "../log/sample_echo_svr.error.%N.log"}], "first file sink missing")
			assert.True(found[sinkKey{typeName: "file", fileName: "../log/sample_echo_svr.all.%N.log"}], "second file sink missing")
			assert.True(found[sinkKey{typeName: "stderr"}], "stderr sink missing")
			assert.True(found[sinkKey{typeName: "stdout"}], "stdout sink missing")
		default:
			t.Errorf("unexpected log category %s", cat.GetName())
		}
	}
}

// verifyMetadata 验证 metadata 配置 (仅 YAML 支持)
func verifyMetadata(t *testing.T, cfg *atframe_protocol.AtappConfigure) {
	t.Helper()
	assert := assert.New(t)
	metadata := cfg.GetMetadata()
	if assert.NotNil(metadata, "metadata must be parsed") {
		assert.Equal("v1", metadata.GetApiVersion(), "metadata.api_version should match")
		labels := metadata.GetLabels()
		assert.Len(labels, 2, "metadata.labels size should match")
		assert.Equal("test", labels["deployment.environment"], "metadata.labels.deployment.environment should match")
		assert.Equal("cn", labels["deployment.region"], "metadata.labels.deployment.region should match")
	}
}

// runConfigVerification 运行所有配置验证
func runConfigVerification(t *testing.T, cfg *atframe_protocol.AtappConfigure, logCfg *atframe_protocol.AtappLog, includeMetadata bool) {
	t.Helper()

	t.Run("root fields", func(t *testing.T) {
		verifyRootFields(t, cfg)
	})

	if includeMetadata {
		t.Run("metadata", func(t *testing.T) {
			verifyMetadata(t, cfg)
		})
	}

	t.Run("bus config", func(t *testing.T) {
		verifyBusConfig(t, cfg)
	})

	t.Run("timer config", func(t *testing.T) {
		verifyTimerConfig(t, cfg)
	})

	t.Run("etcd config", func(t *testing.T) {
		verifyEtcdConfig(t, cfg.Etcd)
	})

	t.Run("log config", func(t *testing.T) {
		verifyLogConfig(t, logCfg)
	})
}

// loadExistedKeyFile 从文件加载环境变量，返回已设置的环境变量key列表以便清理
func loadExistedKeyFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open env file: %w", err)
	}
	defer file.Close()

	var keys []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// 跳过空行和注释
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "#") {
			continue
		}

		keys = append(keys, trimmedLine)
	}

	if err := scanner.Err(); err != nil {
		return keys, fmt.Errorf("error reading env file: %w", err)
	}

	return keys, nil
}

func verifyAppConfigExistedIndex(t *testing.T, existedAppKeys *ConfigExistedIndex) {
	appKeys, _ := loadExistedKeyFile("atapp_configure_loader_test_app_keys.txt")
	t.Helper()
	assert := assert.New(t)
	assert.NotEmpty(appKeys, "app existed keys should not be empty")

	for _, key := range appKeys {
		_, exists := existedAppKeys.MutableExistedSet()[key]
		assert.True(exists, "app config key %s should exist in index", key)
	}
}

func verifyEtcdLogConfigExistedIndex(t *testing.T, existedEtcdKeys *ConfigExistedIndex, existedLogKeys *ConfigExistedIndex) {
	etcdKeys, _ := loadExistedKeyFile("atapp_configure_loader_test_etcd_keys.txt")
	logKeys, _ := loadExistedKeyFile("atapp_configure_loader_test_log_keys.txt")

	t.Helper()
	assert := assert.New(t)
	assert.NotEmpty(etcdKeys, "etcd existed keys should not be empty")
	assert.NotEmpty(logKeys, "log existed keys should not be empty")

	for _, key := range etcdKeys {
		_, exists := existedEtcdKeys.MutableExistedSet()[key]
		assert.True(exists, "etcd config key %s should exist in index", key)
	}

	for _, key := range logKeys {
		_, exists := existedLogKeys.MutableExistedSet()[key]
		assert.True(exists, "log config key %s should exist in index", key)
	}
}

// TestConfigManagementFromYaml 测试从 YAML 文件加载配置
func TestConfigManagementFromYaml(t *testing.T) {
	// Set env vars needed by expression gateways in YAML
	os.Setenv("EXPR_LOADER_COND_SET", "active-value")
	defer os.Unsetenv("EXPR_LOADER_COND_SET")

	app := CreateAppInstance().(*AppInstance)
	config := app.GetConfig()

	// 测试配置加载
	existedAppKeys := CreateConfigExistedIndex()
	if err := app.LoadConfig("atapp_configure_loader_test.yaml", "atapp", "ATAPP", existedAppKeys); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.ConfigFile != "atapp_configure_loader_test.yaml" {
		t.Errorf("Expected ConfigFile 'atapp_configure_loader_test.yaml', got '%s'", config.ConfigFile)
	}

	cfg := app.config.ConfigPb
	if cfg == nil {
		t.Fatalf("config proto should be initialized after LoadConfig")
	}
	logCfg := app.config.ConfigLog
	if logCfg == nil {
		t.Fatalf("log config proto should be initialized after LoadConfig")
	}

	// 运行所有验证，包括 metadata（YAML 支持 map 类型）
	runConfigVerification(t, cfg, logCfg, true)
	verifyAppConfigExistedIndex(t, existedAppKeys)

	// 验证 :+ 表达式 gateway（与 env 测试共享验证代码）
	t.Run("loader gateways", func(t *testing.T) {
		verifyLoaderConditionalWordGateway(t, cfg.GetBus().GetGateways())
	})

	// 验证key存在索引
	existedEtcdKeys := CreateConfigExistedIndex()
	existedLogKeys := CreateConfigExistedIndex()
	etcdCfg := &atframe_protocol.AtappEtcd{}
	logsCfg := &atframe_protocol.AtappLog{}
	app.LoadConfigByPath(etcdCfg, "atapp.etcd", "ATAPP_ETCD", existedEtcdKeys, "")
	app.LoadLogConfigByPath(logsCfg, "atapp.log", "ATAPP_LOG", existedLogKeys, "")
	verifyEtcdLogConfigExistedIndex(t, existedEtcdKeys, existedLogKeys)
	verifyEtcdConfig(t, etcdCfg)
	verifyLogConfig(t, logsCfg)

	// YAML 特有的验证
	t.Run("yaml specific - bus.overwrite_listen_path", func(t *testing.T) {
		assert := assert.New(t)
		bus := cfg.GetBus()
		if assert.NotNil(bus) {
			assert.False(bus.GetOverwriteListenPath(), "bus.overwrite_listen_path should be false per yaml")
		}
	})
}

// verifyLogConfigBasic 验证基本的 log 配置（不包含 sink 的详细验证）
func verifyLogConfigBasic(t *testing.T, logCfg *atframe_protocol.AtappLog) {
	t.Helper()
	assert := assert.New(t)
	if !assert.NotNil(logCfg, "log config should not be nil") {
		return
	}

	assert.Equal("debug", logCfg.GetLevel(), "log.level should match")
	categories := logCfg.GetCategory()
	assert.Len(categories, 2, "log.category length should match")

	for _, cat := range categories {
		switch cat.GetName() {
		case "db":
			assert.Equal(int32(1), cat.GetIndex(), "db category index should match")
			assert.Equal("[Log %L][%F %T.%f]: ", cat.GetPrefix(), "db prefix should match")
			if stack := cat.GetStacktrace(); assert.NotNil(stack) {
				assert.Equal("disable", stack.GetMin(), "db stacktrace.min should match")
				assert.Equal("disable", stack.GetMax(), "db stacktrace.max should match")
			}
		case "default":
			assert.Equal(int32(0), cat.GetIndex(), "default category index should match")
			assert.Equal("[Log %L][%F %T.%f][%s:%n(%C)]: ", cat.GetPrefix(), "default prefix should match")
			if stack := cat.GetStacktrace(); assert.NotNil(stack) {
				assert.Equal("error", stack.GetMin(), "default stacktrace.min should match")
				assert.Equal("fatal", stack.GetMax(), "default stacktrace.max should match")
			}
		default:
			t.Errorf("unexpected log category %s", cat.GetName())
		}
	}
}

// runConfigVerificationForEnv 运行环境变量配置验证（简化版，跳过复杂嵌套结构）
func runConfigVerificationForEnv(t *testing.T, cfg *atframe_protocol.AtappConfigure, logCfg *atframe_protocol.AtappLog) {
	t.Helper()

	t.Run("root fields", func(t *testing.T) {
		verifyRootFields(t, cfg)
	})

	t.Run("bus config", func(t *testing.T) {
		verifyBusConfig(t, cfg)
	})

	t.Run("timer config", func(t *testing.T) {
		verifyTimerConfig(t, cfg)
	})

	t.Run("etcd config", func(t *testing.T) {
		verifyEtcdConfig(t, cfg.Etcd)
	})

	t.Run("log config basic", func(t *testing.T) {
		verifyLogConfigBasic(t, logCfg)
	})
}

// TestConfigManagementFromEnvironment 测试从环境变量加载配置
func TestConfigManagementFromEnvironment(t *testing.T) {
	// 加载环境变量文件
	existedAppKeys := CreateConfigExistedIndex()
	envKeys, err := loadEnvFile("atapp_configure_loader_test.env.txt")
	if err != nil {
		t.Fatalf("Failed to load env file: %v", err)
	}
	// 测试结束后清理环境变量
	defer clearEnvVars(envKeys)

	app := CreateAppInstance().(*AppInstance)

	// 测试配置加载
	if err := app.LoadConfig("", "atapp", "ATAPP", existedAppKeys); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	cfg := app.config.ConfigPb
	if cfg == nil {
		t.Fatalf("config proto should be initialized after LoadConfig")
	}
	logCfg := app.config.ConfigLog
	if logCfg == nil {
		t.Fatalf("log config proto should be initialized after LoadConfig")
	}

	// 运行环境变量配置验证（与 YAML 测试共享基础验证代码）
	runConfigVerificationForEnv(t, cfg, logCfg)
	verifyAppConfigExistedIndex(t, existedAppKeys)

	// 验证 :+ 表达式 gateway（与 YAML 测试共享验证代码）
	t.Run("loader gateways", func(t *testing.T) {
		verifyLoaderConditionalWordGateway(t, cfg.GetBus().GetGateways())
	})

	// 验证key存在索引
	existedEtcdKeys := CreateConfigExistedIndex()
	existedLogKeys := CreateConfigExistedIndex()
	etcdCfg := &atframe_protocol.AtappEtcd{}
	logsCfg := &atframe_protocol.AtappLog{}
	app.LoadConfigByPath(etcdCfg, "atapp.etcd", "ATAPP_ETCD", existedEtcdKeys, "")
	app.LoadLogConfigByPath(logsCfg, "atapp.log", "ATAPP_LOG", existedLogKeys, "")
	verifyEtcdLogConfigExistedIndex(t, existedEtcdKeys, existedLogKeys)
	verifyEtcdConfig(t, etcdCfg)
	verifyLogConfig(t, logsCfg)
}

// =========================================================================================
// Expression expansion unit tests
// =========================================================================================

// TestExpandExpressionBasicVariable tests $VAR and ${VAR} variable reference parsing.
func TestExpandExpressionBasicVariable(t *testing.T) {
	assert := assert.New(t)

	// Arrange: set environment variables
	os.Setenv("EXPR_BASIC_VAR", "hello")
	os.Setenv("EXPR_BASIC_VAR2", "world")
	defer os.Unsetenv("EXPR_BASIC_VAR")
	defer os.Unsetenv("EXPR_BASIC_VAR2")

	// Act & Assert: $VAR form
	assert.Equal("hello", ExpandExpression("$EXPR_BASIC_VAR"), "$VAR should expand to variable value")

	// Act & Assert: ${VAR} form
	assert.Equal("hello", ExpandExpression("${EXPR_BASIC_VAR}"), "${VAR} should expand to variable value")

	// Act & Assert: embedded in text
	assert.Equal("say hello to world", ExpandExpression("say $EXPR_BASIC_VAR to ${EXPR_BASIC_VAR2}"),
		"embedded variables should expand correctly")

	// Act & Assert: undefined variable should expand to empty string
	assert.Equal("", ExpandExpression("$EXPR_UNDEFINED_VAR_12345"),
		"undefined $VAR should expand to empty string")
	assert.Equal("", ExpandExpression("${EXPR_UNDEFINED_VAR_12345}"),
		"undefined ${VAR} should expand to empty string")

	// Act & Assert: text with no variables
	assert.Equal("no variables here", ExpandExpression("no variables here"),
		"text without variables should remain unchanged")

	// Act & Assert: empty string
	assert.Equal("", ExpandExpression(""), "empty string should return empty string")

	// Act & Assert: unbraced $VAR only supports POSIX chars, special chars stop the name
	os.Setenv("app", "partial")
	defer os.Unsetenv("app")
	assert.Equal("partial.kubernetes.io/name", ExpandExpression("$app.kubernetes.io/name"),
		"unbraced $VAR stops at dot — only 'app' is the var name")

	// Act & Assert: braced ${VAR} supports k8s label characters (dot, hyphen, slash)
	os.Setenv("app.kubernetes.io/name", "my-app")
	os.Setenv("my-label.tier", "frontend")
	os.Setenv("config/version", "v2.1")
	os.Setenv("a.b-c/d_e", "complex")
	defer os.Unsetenv("app.kubernetes.io/name")
	defer os.Unsetenv("my-label.tier")
	defer os.Unsetenv("config/version")
	defer os.Unsetenv("a.b-c/d_e")

	assert.Equal("my-app", ExpandExpression("${app.kubernetes.io/name}"),
		"${VAR} with k8s label key chars should expand")
	assert.Equal("frontend", ExpandExpression("${my-label.tier}"),
		"${VAR} with hyphen and dot should expand")
	assert.Equal("v2.1", ExpandExpression("${config/version}"),
		"${VAR} with slash should expand")
	assert.Equal("complex", ExpandExpression("${a.b-c/d_e}"),
		"${VAR} with mixed special chars should expand")

	// Special chars in default/conditional operators
	assert.Equal("my-app", ExpandExpression("${app.kubernetes.io/name:-fallback}"),
		"${k8s.var:-default} with set var should return var value")
	assert.Equal("default-val", ExpandExpression("${unset.k8s.label:-default-val}"),
		"${unset.k8s.var:-default} should return default")
	assert.Equal("word", ExpandExpression("${app.kubernetes.io/name:+word}"),
		"${k8s.var:+word} with set var should return word")
	assert.Equal("", ExpandExpression("${unset.k8s.label:+word}"),
		"${unset.k8s.var:+word} should return empty")
}

// TestExpandExpressionDefaultValue tests ${VAR:-default} parsing.
func TestExpandExpressionDefaultValue(t *testing.T) {
	assert := assert.New(t)

	// Arrange
	os.Setenv("EXPR_DEFAULT_EXISTING", "real_value")
	defer os.Unsetenv("EXPR_DEFAULT_EXISTING")

	// Act & Assert: variable exists - use its value
	assert.Equal("real_value", ExpandExpression("${EXPR_DEFAULT_EXISTING:-fallback}"),
		"${VAR:-default} should use VAR value when VAR is set")

	// Act & Assert: variable not set - use default
	assert.Equal("fallback", ExpandExpression("${EXPR_DEFAULT_MISSING:-fallback}"),
		"${VAR:-default} should use default when VAR is not set")

	// Act & Assert: variable set but empty - use default
	os.Setenv("EXPR_DEFAULT_EMPTY", "")
	defer os.Unsetenv("EXPR_DEFAULT_EMPTY")
	assert.Equal("fallback", ExpandExpression("${EXPR_DEFAULT_EMPTY:-fallback}"),
		"${VAR:-default} should use default when VAR is empty")

	// Act & Assert: default value is empty string
	assert.Equal("", ExpandExpression("${EXPR_DEFAULT_MISSING:-}"),
		"${VAR:-} should return empty when VAR is not set and default is empty")
}

// TestExpandExpressionConditionalWord tests ${VAR:+word} parsing.
func TestExpandExpressionConditionalWord(t *testing.T) {
	assert := assert.New(t)

	// Arrange
	os.Setenv("EXPR_COND_EXISTING", "some_value")
	defer os.Unsetenv("EXPR_COND_EXISTING")

	// Act & Assert: variable exists - use word
	assert.Equal("replacement_word", ExpandExpression("${EXPR_COND_EXISTING:+replacement_word}"),
		"${VAR:+word} should use word when VAR is set and non-empty")

	// Act & Assert: variable not set - return empty
	assert.Equal("", ExpandExpression("${EXPR_COND_MISSING:+replacement_word}"),
		"${VAR:+word} should return empty when VAR is not set")

	// Act & Assert: variable set but empty - return empty
	os.Setenv("EXPR_COND_EMPTY", "")
	defer os.Unsetenv("EXPR_COND_EMPTY")
	assert.Equal("", ExpandExpression("${EXPR_COND_EMPTY:+replacement_word}"),
		"${VAR:+word} should return empty when VAR is empty")
}

// TestExpandExpressionEscape tests \$ escape character.
func TestExpandExpressionEscape(t *testing.T) {
	assert := assert.New(t)

	// Arrange
	os.Setenv("EXPR_ESCAPE_VAR", "value")
	defer os.Unsetenv("EXPR_ESCAPE_VAR")

	// Act & Assert: escaped dollar sign should produce literal $
	assert.Equal("$EXPR_ESCAPE_VAR", ExpandExpression("\\$EXPR_ESCAPE_VAR"),
		"\\$VAR should produce literal $VAR")

	// Act & Assert: escaped in braced form
	assert.Equal("${hello}", ExpandExpression("\\${hello}"),
		"\\${...} should produce literal ${...}")

	// Act & Assert: mixed escaped and real variables
	assert.Equal("$literal and value", ExpandExpression("\\$literal and $EXPR_ESCAPE_VAR"),
		"mixed escaped and real variables should work correctly")

	// Act & Assert: double backslash (not an escape for $)
	assert.Equal("prefix$EXPR_ESCAPE_VAR", ExpandExpression("prefix\\$EXPR_ESCAPE_VAR"),
		"\\$ in middle should produce literal $")

	// Act & Assert: escaped dollar in ${:-} default value
	assert.Equal("price is $5", ExpandExpression("${EXPR_UNDEFINED_FOR_ESCAPE:-price is \\$5}"),
		"\\$ in default value should produce literal $")
}

// TestExpandExpressionNested tests nested expressions like ${OUTER_${INNER}}.
func TestExpandExpressionNested(t *testing.T) {
	assert := assert.New(t)

	// Arrange
	os.Setenv("EXPR_NESTED_INNER", "PART")
	os.Setenv("EXPR_NESTED_OUTER_PART", "resolved_value")
	defer os.Unsetenv("EXPR_NESTED_INNER")
	defer os.Unsetenv("EXPR_NESTED_OUTER_PART")

	// Act & Assert: nested variable name
	assert.Equal("resolved_value", ExpandExpression("${EXPR_NESTED_OUTER_${EXPR_NESTED_INNER}}"),
		"${OUTER_${INNER}} should first resolve INNER, then use result in OUTER lookup")

	// Act & Assert: nested inner resolves to empty -> looks up EXPR_NESTED_OUTER_
	assert.Equal("", ExpandExpression("${EXPR_NESTED_OUTER_${EXPR_NESTED_UNDEFINED}}"),
		"nested with undefined inner should resolve to empty var name suffix")
}

// TestExpandExpressionMultiLevelNested tests multi-level nested defaults like ${OUTER:-${INNER:-default}}.
func TestExpandExpressionMultiLevelNested(t *testing.T) {
	assert := assert.New(t)

	// Scenario 1: outermost variable is set
	os.Setenv("EXPR_MULTI_OUTER", "outer_value")
	defer os.Unsetenv("EXPR_MULTI_OUTER")
	assert.Equal("outer_value", ExpandExpression("${EXPR_MULTI_OUTER:-${EXPR_MULTI_INNER:-default2}}"),
		"outermost variable set should use its value")

	// Scenario 2: outer not set, inner is set
	os.Setenv("EXPR_MULTI_INNER", "inner_value")
	defer os.Unsetenv("EXPR_MULTI_INNER")
	assert.Equal("inner_value", ExpandExpression("${EXPR_MULTI_MISSING_OUTER:-${EXPR_MULTI_INNER:-default2}}"),
		"outer not set, inner set should use inner value")

	// Scenario 3: both outer and inner not set, use deepest default
	assert.Equal("default2", ExpandExpression("${EXPR_MULTI_MISSING_OUTER:-${EXPR_MULTI_MISSING_INNER:-default2}}"),
		"both not set should use deepest default value")

	// Scenario 4: three-level nesting
	assert.Equal("deep_default", ExpandExpression(
		"${EXPR_MULTI_MISS_A:-${EXPR_MULTI_MISS_B:-${EXPR_MULTI_MISS_C:-deep_default}}}"),
		"three-level nested defaults should resolve to deepest default")
}

// TestExpandExpressionEdgeCases tests edge cases and boundary conditions.
func TestExpandExpressionEdgeCases(t *testing.T) {
	assert := assert.New(t)

	// Act & Assert: lone $ at end of string
	assert.Equal("trailing$", ExpandExpression("trailing$"),
		"lone $ at end should be preserved")

	// Act & Assert: $ followed by non-identifier character
	assert.Equal("$ not a var", ExpandExpression("$ not a var"),
		"$ followed by space should be preserved")

	// Act & Assert: unclosed brace
	assert.Equal("${unclosed", ExpandExpression("${unclosed"),
		"unclosed ${...} should be preserved as literal")

	// Act & Assert: empty variable name
	os.Setenv("", "empty_name_val")
	defer os.Unsetenv("")
	assert.Equal("$", ExpandExpression("$"),
		"lone $ should be preserved")

	// Act & Assert: consecutive dollar signs
	assert.Equal("$$", ExpandExpression("$$"),
		"$$ should be preserved (two lone dollars)")

	// Act & Assert: nested with default that contains braces
	os.Setenv("EXPR_EDGE_VAR", "found")
	defer os.Unsetenv("EXPR_EDGE_VAR")
	assert.Equal("found", ExpandExpression("${EXPR_EDGE_VAR:-default}"),
		"variable found should use its value")
}

// TestExpandExpressionDepthLimit tests that deeply nested expressions don't cause stack overflow.
func TestExpandExpressionDepthLimit(t *testing.T) {
	assert := assert.New(t)

	// Build a deeply nested expression that exceeds maxExpressionDepth
	// ${A:-${A:-${A:-...default...}}}
	expr := "final_default"
	for i := 0; i < maxExpressionDepth+5; i++ {
		expr = fmt.Sprintf("${EXPR_DEPTH_LIMIT_MISSING_%d:-%s}", i, expr)
	}

	// Should not panic and should return some result
	result := ExpandExpression(expr)
	assert.NotEmpty(result, "deeply nested expression should not panic and return a result")
}

// =========================================================================================
// Shared gateway verification functions
// =========================================================================================

// verifyExpressionGateways verifies expression expansion in all 4 gateway configs.
// Used by both TestExpandExpressionInConfigYaml and TestExpandExpressionInConfigEnv.
func verifyExpressionGateways(t *testing.T, gateways []*atframe_protocol.AtappGateway) {
	t.Helper()

	if !assert.New(t).GreaterOrEqual(len(gateways), 4, "should have at least 4 gateways for expression tests") {
		return
	}

	t.Run("gateway 0 basic expression", func(t *testing.T) {
		assert := assert.New(t)
		gw := gateways[0]
		assert.Equal("https://example.com:8443", gw.GetAddress(),
			"gateway address should be expanded from ${EXPR_TEST_PROTOCOL}://${EXPR_TEST_HOST}:${EXPR_TEST_PORT}")

		ns := gw.GetMatchNamespaces()
		if assert.GreaterOrEqual(len(ns), 2) {
			assert.Equal("custom-namespace", ns[0], "namespace should be resolved from EXPR_TEST_NAMESPACE")
			assert.Equal("fallback-namespace", ns[1], "namespace should use default (EXPR_TEST_MISSING_NS not set)")
		}

		labels := gw.GetMatchLabels()
		assert.Equal("staging", labels["environment"],
			"label key 'environment' (from EXPR_TEST_LABEL_KEY) with value 'staging' (from EXPR_TEST_LABEL_VAL)")
		assert.Equal("us-west-2", labels["region"],
			"region label value should be expanded from EXPR_TEST_REGION")
	})

	t.Run("gateway 1 default expression", func(t *testing.T) {
		assert := assert.New(t)
		assert.Equal("tcp://0.0.0.0:8080", gateways[1].GetAddress(),
			"gateway address should use default when EXPR_TEST_GATEWAY_ADDR is not set")
	})

	t.Run("gateway 2 multi-level nested default", func(t *testing.T) {
		assert := assert.New(t)
		gw := gateways[2]
		assert.Equal("http://localhost:9090", gw.GetAddress(),
			"multi-level nested defaults should resolve to deepest fallback values")

		labels := gw.GetMatchLabels()
		assert.Equal("backend", labels["tier"],
			"multi-level nested default label key/value should resolve to deepest defaults")
	})

	t.Run("gateway 3 conditional word :+", func(t *testing.T) {
		assert := assert.New(t)
		gw := gateways[3]
		assert.Equal("secure://example.com:8443", gw.GetAddress(),
			"conditional word :+ should expand to word when var is set, empty when unset")

		ns := gw.GetMatchNamespaces()
		if assert.GreaterOrEqual(len(ns), 2) {
			assert.Equal("ns-when-set", ns[0], ":+ should expand to word when variable is set")
			assert.Equal("", ns[1], ":+ should expand to empty when variable is not set")
		}

		labels := gw.GetMatchLabels()
		assert.Equal("val-staging", labels["region-active"],
			":+ should expand label key and value, including nested expression in word")
	})
}

// verifyLoaderConditionalWordGateway verifies the :+ expression gateway in loader tests.
// Used by both TestConfigManagementFromYaml and TestConfigManagementFromEnvironment.
func verifyLoaderConditionalWordGateway(t *testing.T, gateways []*atframe_protocol.AtappGateway) {
	t.Helper()

	if !assert.New(t).GreaterOrEqual(len(gateways), 1, "should have at least 1 gateway for loader :+ test") {
		return
	}

	t.Run("conditional word :+ gateway", func(t *testing.T) {
		assert := assert.New(t)
		gw := gateways[0]
		assert.Equal("tcp://active-host:8080", gw.GetAddress(),
			":+ should expand to word when variable is set")

		ns := gw.GetMatchNamespaces()
		if assert.GreaterOrEqual(len(ns), 2) {
			assert.Equal("active-ns", ns[0], ":+ should expand to word when variable is set")
			assert.Equal("", ns[1], ":+ should expand to empty when variable is not set")
		}

		labels := gw.GetMatchLabels()
		assert.Equal("val-active-value", labels["status"],
			":+ should expand key and value, including nested expression in word")
	})
}

// TestExpandExpressionInConfigYaml tests expression expansion in actual config loading from YAML.
func TestExpandExpressionInConfigYaml(t *testing.T) {
	// Arrange: set environment variables used in expression test yaml
	envVars := map[string]string{
		"EXPR_TEST_PROTOCOL":  "https",
		"EXPR_TEST_HOST":      "example.com",
		"EXPR_TEST_PORT":      "8443",
		"EXPR_TEST_NAMESPACE": "custom-namespace",
		"EXPR_TEST_LABEL_KEY": "environment",
		"EXPR_TEST_LABEL_VAL": "staging",
		"EXPR_TEST_REGION":    "us-west-2",
	}
	for k, v := range envVars {
		os.Setenv(k, v)
	}
	defer func() {
		for k := range envVars {
			os.Unsetenv(k)
		}
	}()

	app := CreateAppInstance().(*AppInstance)

	// Act: load config from expression test yaml
	existedAppKeys := CreateConfigExistedIndex()
	if err := app.LoadConfig("atapp_configure_expression_test.yaml", "atapp", "ATAPP", existedAppKeys); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	cfg := app.config.ConfigPb
	if cfg == nil {
		t.Fatalf("config proto should be initialized after LoadConfig")
	}
	logCfg := app.config.ConfigLog
	if logCfg == nil {
		t.Fatalf("log config proto should be initialized after LoadConfig")
	}

	// Assert: verify base config (shared with loader tests)
	runConfigVerification(t, cfg, logCfg, true)

	// Assert: verify expression gateways (shared with env test)
	t.Run("expression gateways", func(t *testing.T) {
		verifyExpressionGateways(t, cfg.GetBus().GetGateways())
	})
}

// TestExpandExpressionInConfigEnv tests expression expansion when loading config from environment variables.
func TestExpandExpressionInConfigEnv(t *testing.T) {
	// Arrange: load env file (expression variables + ATAPP_* config with expressions)
	envKeys, err := loadEnvFile("atapp_configure_expression_test.env.txt")
	if err != nil {
		t.Fatalf("Failed to load env file: %v", err)
	}
	defer clearEnvVars(envKeys)

	app := CreateAppInstance().(*AppInstance)

	// Act: load config from environment only (no config file)
	existedAppKeys := CreateConfigExistedIndex()
	if err := app.LoadConfig("", "atapp", "ATAPP", existedAppKeys); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	cfg := app.config.ConfigPb
	if cfg == nil {
		t.Fatalf("config proto should be initialized after LoadConfig")
	}
	logCfg := app.config.ConfigLog
	if logCfg == nil {
		t.Fatalf("log config proto should be initialized after LoadConfig")
	}

	// Assert: verify base config (shared with loader tests)
	runConfigVerificationForEnv(t, cfg, logCfg)

	// Assert: verify expression gateways (shared with yaml test)
	t.Run("expression gateways", func(t *testing.T) {
		verifyExpressionGateways(t, cfg.GetBus().GetGateways())
	})

	// Verify separate path loading (shared with loader env test)
	existedEtcdKeys := CreateConfigExistedIndex()
	existedLogKeys := CreateConfigExistedIndex()
	etcdCfg := &atframe_protocol.AtappEtcd{}
	logsCfg := &atframe_protocol.AtappLog{}
	app.LoadConfigByPath(etcdCfg, "atapp.etcd", "ATAPP_ETCD", existedEtcdKeys, "")
	app.LoadLogConfigByPath(logsCfg, "atapp.log", "ATAPP_LOG", existedLogKeys, "")
	verifyEtcdConfig(t, etcdCfg)
	verifyLogConfig(t, logsCfg)
}

// TestExpandExpressionCombinedOperators tests combinations of operators in expressions.
func TestExpandExpressionCombinedOperators(t *testing.T) {
	assert := assert.New(t)

	// Arrange
	os.Setenv("EXPR_COMBO_A", "alpha")
	os.Setenv("EXPR_COMBO_B", "beta")
	defer os.Unsetenv("EXPR_COMBO_A")
	defer os.Unsetenv("EXPR_COMBO_B")

	// Act & Assert: multiple expressions in one string
	assert.Equal("alpha-beta", ExpandExpression("${EXPR_COMBO_A}-${EXPR_COMBO_B}"),
		"multiple expressions should all be expanded")

	// Act & Assert: expression with text around it
	assert.Equal("prefix_alpha_suffix", ExpandExpression("prefix_${EXPR_COMBO_A}_suffix"),
		"expression embedded in text should expand correctly")

	// Act & Assert: conditional with nested default
	assert.Equal("beta", ExpandExpression("${EXPR_COMBO_MISSING:+${EXPR_COMBO_A}}${EXPR_COMBO_MISSING:-${EXPR_COMBO_B}}"),
		":+ on missing var gives empty, then :- on missing gives beta")

	// Act & Assert: conditional (set) with variable in word
	assert.Equal("alpha", ExpandExpression("${EXPR_COMBO_A:+${EXPR_COMBO_A}}"),
		":+ on set var should expand the word which itself contains an expression")
}
