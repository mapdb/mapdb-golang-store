//go:build !linux && !windows

package store

import (
	"os"
	"syscall"
)

// Non-Linux fallback: OFD record locks do not exist, so this uses flock().
// flock and POSIX record locks are independent lock classes — cross-engine
// exclusion against a Java holder is NOT provided on this platform (it is on
// Linux, where D4's durability model applies anyway). Same-process exclusion
// is still enforced by the process-local claim table.
func tryOFDLock(f *os.File, exclusive bool) (bool, error) {
	how := syscall.LOCK_SH | syscall.LOCK_NB
	if exclusive {
		how = syscall.LOCK_EX | syscall.LOCK_NB
	}
	err := syscall.Flock(int(f.Fd()), how)
	if err == nil {
		return true, nil
	}
	if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
		return false, nil
	}
	return false, err
}

func fdatasync(f *os.File) error { return f.Sync() }

func isWritableDir(dir string) bool {
	return syscall.Access(dir, 2 /*W_OK*/) == nil
}

func lockIdentity(f *os.File) ([2]uint64, error) {
	fi, err := f.Stat()
	if err != nil {
		return [2]uint64{}, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return [2]uint64{}, nil
	}
	return [2]uint64{uint64(st.Dev), st.Ino}, nil
}
