package libatapp

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSetupOptionsConfigExpandsEnvExpression verifies that --config expands environment variable expressions.
func TestSetupOptionsConfigExpandsEnvExpression(t *testing.T) {
	t.Setenv("TEST_CONFIG_DIR", "/etc/myapp")
	t.Setenv("TEST_CONFIG_NAME", "app.yaml")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--config", "${TEST_CONFIG_DIR}/${TEST_CONFIG_NAME}",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/etc/myapp/app.yaml", app.config.ConfigFile)
}

// TestSetupOptionsConfigExpandsEnvWithDefault verifies ${VAR:-default} syntax for --config.
func TestSetupOptionsConfigExpandsEnvWithDefault(t *testing.T) {
	// Ensure the variable is unset
	os.Unsetenv("TEST_UNSET_CONFIG_PATH")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--config", "${TEST_UNSET_CONFIG_PATH:-/default/config.yaml}",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/default/config.yaml", app.config.ConfigFile)
}

// TestSetupOptionsPidExpandsEnvExpression verifies that --pid expands environment variable expressions.
func TestSetupOptionsPidExpandsEnvExpression(t *testing.T) {
	t.Setenv("TEST_PID_DIR", "/var/run")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--pid", "${TEST_PID_DIR}/myapp.pid",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/var/run/myapp.pid", app.config.PidFile)
}

// TestSetupOptionsPidExpandsEnvWithDefault verifies ${VAR:-default} syntax for --pid.
func TestSetupOptionsPidExpandsEnvWithDefault(t *testing.T) {
	os.Unsetenv("TEST_UNSET_PID_PATH")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--pid", "${TEST_UNSET_PID_PATH:-/tmp/default.pid}",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/tmp/default.pid", app.config.PidFile)
}

// TestSetupOptionsStartupLogExpandsEnvExpression verifies that --startup-log expands environment variable expressions.
func TestSetupOptionsStartupLogExpandsEnvExpression(t *testing.T) {
	t.Setenv("TEST_LOG_FILE", "app_startup.log")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--startup-log", "stdout,${TEST_LOG_FILE}",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"stdout", "app_startup.log"}, app.config.StartupLog)
}

// TestSetupOptionsStartupLogExpandsEnvWithDefault verifies ${VAR:-default} syntax for --startup-log.
func TestSetupOptionsStartupLogExpandsEnvWithDefault(t *testing.T) {
	os.Unsetenv("TEST_UNSET_LOG_FILE")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--startup-log", "${TEST_UNSET_LOG_FILE:-stderr}",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"stderr"}, app.config.StartupLog)
}

// TestSetupOptionsCrashOutputFileExpandsEnvExpression verifies that --crash-output-file expands environment variable expressions.
func TestSetupOptionsCrashOutputFileExpandsEnvExpression(t *testing.T) {
	t.Setenv("TEST_CRASH_DIR", "/var/log/crash")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--crash-output-file", "${TEST_CRASH_DIR}/crash.log",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/var/log/crash/crash.log", app.config.CrashOutputFile)
}

// TestSetupOptionsCrashOutputFileExpandsEnvWithDefault verifies ${VAR:-default} syntax for --crash-output-file.
func TestSetupOptionsCrashOutputFileExpandsEnvWithDefault(t *testing.T) {
	os.Unsetenv("TEST_UNSET_CRASH_FILE")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--crash-output-file", "${TEST_UNSET_CRASH_FILE:-/tmp/crash_default.log}",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/tmp/crash_default.log", app.config.CrashOutputFile)
}

// TestSetupOptionsLiteralDollarEscape verifies that \$ produces a literal $ sign.
func TestSetupOptionsLiteralDollarEscape(t *testing.T) {
	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--config", `\$not_a_var/config.yaml`,
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "$not_a_var/config.yaml", app.config.ConfigFile)
}

// TestSetupOptionsNoExpansionWhenNoEnvSyntax verifies plain values pass through unchanged.
func TestSetupOptionsNoExpansionWhenNoEnvSyntax(t *testing.T) {
	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--config", "/plain/path/config.yaml",
		"--pid", "/plain/path/app.pid",
		"--startup-log", "stdout,stderr",
		"--crash-output-file", "/plain/path/crash.log",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/plain/path/config.yaml", app.config.ConfigFile)
	assert.Equal(t, "/plain/path/app.pid", app.config.PidFile)
	assert.Equal(t, []string{"stdout", "stderr"}, app.config.StartupLog)
	assert.Equal(t, "/plain/path/crash.log", app.config.CrashOutputFile)
}

// TestSetupOptionsNestedEnvExpression verifies nested ${OUTER:-${INNER:-default}} expansion.
func TestSetupOptionsNestedEnvExpression(t *testing.T) {
	os.Unsetenv("TEST_OUTER_VAR")
	t.Setenv("TEST_INNER_VAR", "inner_value")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--config", "${TEST_OUTER_VAR:-${TEST_INNER_VAR}}/config.yaml",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "inner_value/config.yaml", app.config.ConfigFile)
}

// TestSetupOptionsConditionalWord verifies ${VAR:+word} expansion for CLI params.
func TestSetupOptionsConditionalWord(t *testing.T) {
	t.Setenv("TEST_COND_VAR", "something")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--pid", "/run/${TEST_COND_VAR:+custom}/app.pid",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/run/custom/app.pid", app.config.PidFile)
}

// TestSetupOptionsConditionalWordUnset verifies ${VAR:+word} produces empty when VAR is unset.
func TestSetupOptionsConditionalWordUnset(t *testing.T) {
	os.Unsetenv("TEST_COND_UNSET")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--pid", "/run/${TEST_COND_UNSET:+custom}/app.pid",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/run//app.pid", app.config.PidFile)
}

// TestSetupOptionsMultipleEnvVarsInOneParam verifies multiple env vars in a single parameter value.
func TestSetupOptionsMultipleEnvVarsInOneParam(t *testing.T) {
	t.Setenv("TEST_HOST", "localhost")
	t.Setenv("TEST_PORT", "8080")

	app := CreateAppInstance().(*AppInstance)

	err := app.setupOptions([]string{
		"--config", "/srv/${TEST_HOST}/${TEST_PORT}/config.yaml",
		"start",
	})
	assert.NoError(t, err)
	assert.Equal(t, "/srv/localhost/8080/config.yaml", app.config.ConfigFile)
}
