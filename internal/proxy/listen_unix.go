//go:build !windows

package proxy

import (
	"net"
	"os"
	"path/filepath"

	"glaciertdb/internal/logx"
)

// listenUnix UDS 监听（v1.3.13 §5.1，仅 Linux/Unix）：3309 首字节协议多路复用
// 整体架在 Unix domain socket 上——mysql/redis/amqp/mqtt/ws 本地连接免 TCP 层
// 开销。鉴权照旧（各协议认证不变），纯传输加速。残留 socket 文件先删除。
func listenUnix(path string) (net.Listener, error) {
	if path == "" {
		return nil, nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	_ = os.Remove(path) // 残留 socket（进程崩溃遗留）幂等清理
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		logx.Warnf("unix socket chmod: %v", err)
	}
	return ln, nil
}
