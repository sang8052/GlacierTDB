//go:build windows

package s3

import (
	"errors"
	"net"
)

// s3ListenUnix S3 端点 UDS 监听：仅 Linux/Unix（Windows 构建显式拒绝）。
func s3ListenUnix(path string) (net.Listener, error) {
	return nil, errors.New("s3 listen_unix is only supported on Linux/Unix")
}
