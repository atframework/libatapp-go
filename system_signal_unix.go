//go:build !windows
// +build !windows

package libatapp

import (
	"os"
	"syscall"
)

var (
	atappStopSigs   map[os.Signal]struct{}
	atappReloadSigs map[os.Signal]struct{}
)

func atappSignalGetStopSigs() map[os.Signal]struct{} {
	if atappStopSigs == nil {
		atappStopSigs = make(map[os.Signal]struct{})
		atappStopSigs[syscall.SIGTERM] = struct{}{}
		atappStopSigs[syscall.SIGQUIT] = struct{}{}
	}

	return atappStopSigs
}

func atappSignalGetReloadSigs() map[os.Signal]struct{} {
	if atappReloadSigs == nil {
		atappReloadSigs = make(map[os.Signal]struct{})
		atappReloadSigs[syscall.SIGHUP] = struct{}{}
		atappReloadSigs[syscall.SIGUSR1] = struct{}{}
	}

	return atappReloadSigs
}
