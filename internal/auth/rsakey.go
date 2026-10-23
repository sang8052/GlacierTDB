package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
)

// RSAKeyFile RSA 私钥文件（<db_dir>/glacier_rsa.pem，0600）。
// caching_sha2_password / sha256_password 在非 TLS 连接下的密码交换需要服务端 RSA 密钥
//（客户端用公钥加密密码，服务端私钥解密）；首次启动自动生成 2048 位。
const RSAKeyFile = "glacier_rsa.pem"

// LoadOrGenerateRSAKey 加载（或首次生成）RSA 私钥。
func LoadOrGenerateRSAKey(dir string) (*rsa.PrivateKey, error) {
	path := filepath.Join(dir, RSAKeyFile)
	if data, err := os.ReadFile(path); err == nil {
		block, _ := pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("rsa key file %s: no PEM block", path)
		}
		if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
			return k, nil
		}
		anyKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("rsa key file %s: %w", path, err)
		}
		k, ok := anyKey.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("rsa key file %s: not an RSA key", path)
		}
		return k, nil
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		return nil, fmt.Errorf("write rsa key %s: %w", path, err)
	}
	return key, nil
}
