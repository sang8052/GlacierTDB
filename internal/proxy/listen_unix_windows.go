//go:build windows

package proxy

import (
	"errors"
	"net"
)

// listenUnix UDS 监听：AF_UNIX 复用仅 Linux/Unix 部署（v1.3.13 §5.1），
// Windows 构建下显式拒绝（与 dirlock 的 build-tags 拆分先例一致）。
func listenUnix(path string) (net.Listener, error) {
	if path == "" {
		return nil, nil
	}
	return nil, errors.New("listen_unix is only supported on Linux/Unix")
}
