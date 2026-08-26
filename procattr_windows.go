//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// hideConsole stops a spawned console child (mattermost/winstream) from popping its
// own console window under the windowless (-H windowsgui) proxy. CreationFlags is a
// Windows-only field, so this lives in a windows-tagged file.
func hideConsole(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x08000000} // CREATE_NO_WINDOW
}
