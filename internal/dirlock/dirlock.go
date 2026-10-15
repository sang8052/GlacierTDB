// Package dirlock 提供 db 目录唯一锁：同一目录同时只允许一个进程（服务或 CLI）。
// Linux 用 flock（进程退出自动释放，异常退出不留死锁）；Windows 退化为 PID 存活检测。
package dirlock

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Lock 持有的目录锁。
type Lock struct {
	f    *os.File
	path string
}

// Acquire 获取目录锁；force 时若持锁进程已死亡则清锁重试。
func Acquire(dir string, force bool) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, ".glacier.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := tryFlock(f); err != nil {
		holder := readHolder(f)
		if !force {
			return nil, fmt.Errorf("db 目录 %s 已被其他进程持有（%s）；确认后可用 --force-unlock 启动", dir, holder)
		}
		if pid, alive := holderAlive(holder); alive {
			return nil, fmt.Errorf("db 目录 %s 正被运行中的进程持有（pid %d），--force-unlock 拒绝抢锁", dir, pid)
		}
		f.Close()
		_ = os.Remove(path)
		f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		if err := tryFlock(f); err != nil {
			return nil, fmt.Errorf("目录锁获取失败: %w", err)
		}
	}
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(fmt.Sprintf("pid=%d start=%s\n", os.Getpid(), time.Now().Format(time.RFC3339))), 0)
	return &Lock{f: f, path: path}, nil
}

// Release 释放。
func (l *Lock) Release() {
	if l == nil || l.f == nil {
		return
	}
	tryUnflock(l.f)
	l.f.Close()
}



func readHolder(f *os.File) string {
	buf := make([]byte, 256)
	n, _ := f.ReadAt(buf, 0)
	return strings.TrimSpace(string(buf[:n]))
}

func holderAlive(info string) (int, bool) {
	for _, kv := range strings.Fields(info) {
		if strings.HasPrefix(kv, "pid=") {
			if pid, err := strconv.Atoi(strings.TrimPrefix(kv, "pid=")); err == nil {
				if p, err := os.FindProcess(pid); err == nil {
					if runtime.GOOS == "windows" {
						return pid, true
					}
					if err := p.Signal(syscall.Signal(0)); err == nil {
						return pid, true
					}
				}
			}
			return 0, false
		}
	}
	return 0, false
}
