//go:build !windows

package s3

import (
	"net"
	"os"
	"path/filepath"
)

// s3ListenUnix S3 端点 UDS 监听（v1.3.13 §5.2，仅 Linux/Unix）：残留 socket
// 文件先删除；权限 0660（本机进程组可访问，鉴权仍走 SigV4）。
func s3ListenUnix(path string) (net.Listener, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	_ = os.Chmod(path, 0o660)
	return ln, nil
}
