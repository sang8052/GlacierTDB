//go:build linux

package kv

// 独立运算核的核绑定（v1.4.0，仅 Linux）：LockOSThread + SchedSetaffinity。

import (
	"runtime"

	"golang.org/x/sys/unix"

	"glaciertdb/internal/logx"
)

// bindExecCore 把当前 goroutine 所在 OS 线程锁定并绑到指定核。
// cpu<0 = 自动选末核（NumCPU-1）；越界或绑定失败回退不绑核（单线程串行执行
// 语义不受影响）。返回 (核号, 是否绑定成功)。
func (s *Store) bindExecCore(cpu int) (int, bool) {
	runtime.LockOSThread()
	n := runtime.NumCPU()
	if cpu < 0 {
		cpu = n - 1
	}
	if cpu >= n {
		runtime.UnlockOSThread()
		logx.Warnf("kv: core_cpu=%d out of range (numcpu=%d), running unpinned", cpu, n)
		return -1, false
	}
	var set unix.CPUSet
	set.Set(cpu)
	if err := unix.SchedSetaffinity(0, &set); err != nil {
		runtime.UnlockOSThread()
		logx.Warnf("kv: sched_setaffinity cpu=%d: %v, running unpinned", cpu, err)
		return -1, false
	}
	return cpu, true
}

// coreNote 非绑核场景的日志附注。
func coreNote() string { return "" }
