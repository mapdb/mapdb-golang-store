//go:build linux

package store

import (
	"os"
	"syscall"
	"unsafe"
)

// fcntlOFDSetlk is F_OFD_SETLK: an open-file-description record lock, matching
// Java's FileChannel.tryLock — which is also a POSIX record lock. NOT flock:
// BSD locks and POSIX record locks are independent lock classes, so a flock
// here would not exclude a Java holder of the same store. OFD rather than
// F_SETLK because ownership is the open file description: a second open in
// this process is refused instead of silently upgrading the first lock.
const fcntlOFDSetlk = 37

// tryOFDLock takes a non-blocking whole-file OFD record lock. Returns
// (locked, error): false means another owner holds a conflicting lock.
func tryOFDLock(f *os.File, exclusive bool) (bool, error) {
	typ := int16(syscall.F_RDLCK)
	if exclusive {
		typ = syscall.F_WRLCK
	}
	lk := syscall.Flock_t{Type: typ, Whence: 0, Start: 0, Len: 0}
	for {
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(),
			fcntlOFDSetlk, uintptr(unsafe.Pointer(&lk)))
		switch errno {
		case 0:
			return true, nil
		case syscall.EACCES, syscall.EAGAIN:
			return false, nil
		case syscall.EINTR:
			continue
		default:
			return false, errno
		}
	}
}

// fdatasync persists file data (and the metadata needed to retrieve it,
// including an extended size) without a full sync.
func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// isWritableDir is access(dir, W_OK) — the same probe Java's Files.isWritable
// makes, answering for this process's credentials only.
func isWritableDir(dir string) bool {
	return syscall.Access(dir, 2 /*W_OK*/) == nil
}

// lockIdentity is the (dev, inode) of the lock FILE — opens naming the same
// store through different paths must collide.
func lockIdentity(f *os.File) ([2]uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		return [2]uint64{}, err
	}
	return [2]uint64{uint64(st.Dev), st.Ino}, nil
}
