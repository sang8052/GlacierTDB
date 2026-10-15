//go:build windows

package dirlock

import "os"

// Windows 无 flock：互斥依赖锁文件 PID 存活检测（见 readHolder/holderAlive）。
func tryFlock(f *os.File) error { return nil }
func tryUnflock(f *os.File)     {}
