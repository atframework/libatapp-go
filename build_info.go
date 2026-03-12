package libatapp

import (
	"fmt"
	"os"
)

// BuildInfo 存储编译注入的信息
type BuildInfo struct {
	Version       string // 版本号
	GitCommit     string // Git 提交哈希
	GitBranch     string // Git 分支
	BuildTime     string // 构建时间
	ConfigVersion string // 构建配置
	BuildMode     string // DEBUG 或 Release
}

var buildInfo = BuildInfo{
	Version:       "v0.0.0",
	GitCommit:     "unknown",
	GitBranch:     "unknown",
	BuildTime:     "unknown",
	ConfigVersion: "unknown",
	BuildMode:     "Release",
}

// 编译时注入的顶级变量（用于 ldflags）
var (
	Version   = "v0.0.0"
	Commit    = "unknown"
	Branch    = "unknown"
	BuildTime = "unknown"
	ConfigVer = "unknown"
	BuildMode = "DEBUG"
)

// initBuildInfo 初始化编译信息（从顶级变量同步到 buildInfo 结构体）
func initBuildInfo() {
	buildInfo.Version = Version
	buildInfo.GitCommit = Commit
	buildInfo.GitBranch = Branch
	buildInfo.BuildTime = BuildTime
	buildInfo.ConfigVersion = ConfigVer
	buildInfo.BuildMode = BuildMode
}

// SetBuildInfo 设置编译信息（由 build 命令注入）
func SetBuildInfo(version, gitCommit, gitBranch, buildTime, cfgVersion, buildMode string) {
	buildInfo.Version = version
	buildInfo.GitCommit = gitCommit
	buildInfo.GitBranch = gitBranch
	buildInfo.BuildTime = buildTime
	buildInfo.ConfigVersion = cfgVersion
	buildInfo.BuildMode = buildMode
}

// GetBuildInfo 获取编译信息
func GetBuildInfo() BuildInfo {
	return buildInfo
}

func (b BuildInfo) ToString() string {
	return fmt.Sprintf("Version: %s,\n Branch: %s,\n Commit: %s,\n ConfigVersion: %s, Mode: %s, Time: %s",
		b.Version, b.GitBranch, b.GitCommit, b.ConfigVersion, b.BuildMode, b.BuildTime)
}

// PrintBuildInfo 打印编译信息
func PrintBuildInfo() {
	fmt.Printf("%s\n", buildInfo.ToString())
}

// RegisterInfoCommand 处理 --info 标志
// 这个函数会在应用启动前被调用，处理 --info 标志
// 仅检查命令行参数中是否有 --info，不使用全局 flag 包避免污染其他参数
func RegisterBuildInfoCommand() {
	for _, arg := range os.Args[1:] {
		if arg == "--info" || arg == "-info" {
			// 确保 BuildInfo 被初始化
			initBuildInfo()
			PrintBuildInfo()
			os.Exit(0)
		}
	}
}
