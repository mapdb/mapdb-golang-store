//go:build windows

package store

import "os"

// Windows cannot express a directory fsync, so a durable writable open is
// refused by StoreWAL before a lock is ever attempted. Same-process exclusion
// is still provided by the process-local claim table.
func tryOFDLock(f *os.File, exclusive bool) (bool, error) { return true, nil }
func fdatasync(f *os.File) error                          { return f.Sync() }
func isWritableDir(dir string) bool                       { return true }
func lockIdentity(f *os.File) ([2]uint64, error)          { return [2]uint64{}, nil }
