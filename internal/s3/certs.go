package s3

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"glaciertdb/internal/logx"
)

// certEntry 证书缓存项（mtime 变化即重载——文件替换即生效的热加载）。
type certEntry struct {
	certFile string
	keyFile  string
	mtimeC   time.Time
	mtimeK   time.Time
	cert     *tls.Certificate
}

// certRegistry 端点证书注册表：端点泛域名证书（SNI 兜底）+ bucket 自定义域名独立证书。
type certRegistry struct {
	mu          sync.Mutex
	defaultFile string
	defaultKey  string
	domains     map[string]string // host → certFile|keyFile（"cert|key"）
	cache       map[string]*certEntry
	store       *Store
}

func newCertRegistry(store *Store, defCert, defKey string) *certRegistry {
	c := &certRegistry{
		defaultFile: defCert,
		defaultKey:  defKey,
		domains:     map[string]string{},
		cache:       map[string]*certEntry{},
		store:       store,
	}
	c.reloadDomains()
	return c
}

// reloadDomains 从系统库刷新自定义域名 → 证书映射（建/删域名后调用）。
func (c *certRegistry) reloadDomains() {
	if c.store == nil {
		return
	}
	m := map[string]string{}
	for _, d := range c.store.Domains() {
		cf, kf := d.CertFile, d.KeyFile
		if cf == "" || kf == "" {
			// 未上传证书文件时用自签发文件（certs/<host>.pem/.key）
			base := filepath.Join(c.store.dir, "certs", d.Host)
			cf, kf = base+".pem", base+".key"
		}
		m[d.Host] = cf + "|" + kf
	}
	c.mu.Lock()
	c.domains = m
	c.mu.Unlock()
}

// GetCertificate tls.Config.GetCertificate 钩子：按 SNI 选证书，文件变化自动重载。
func (c *certRegistry) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	name := strings.ToLower(hello.ServerName)
	c.mu.Lock()
	pair, ok := c.domains[name]
	defC, defK := c.defaultFile, c.defaultKey
	c.mu.Unlock()
	if !ok {
		// 泛域名兜底：*.suffix 形式匹配（证书自身 SAN 匹配交给 crypto/tls）
		c.mu.Lock()
		for host, p := range c.domains {
			if strings.HasPrefix(host, "*.") && strings.HasSuffix(name, host[1:]) {
				pair, ok = p, true
				break
			}
		}
		c.mu.Unlock()
	}
	if !ok {
		if defC == "" || defK == "" {
			return nil, fmt.Errorf("no TLS certificate configured for SNI %q", name)
		}
		return c.load(defC, defK)
	}
	parts := strings.SplitN(pair, "|", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("bad cert pair for %q", name)
	}
	return c.load(parts[0], parts[1])
}

// load 带 mtime 热加载的证书读取。
func (c *certRegistry) load(certFile, keyFile string) (*tls.Certificate, error) {
	key := certFile + "|" + keyFile
	stC, errC := os.Stat(certFile)
	stK, errK := os.Stat(keyFile)
	if errC != nil || errK != nil {
		return nil, fmt.Errorf("cert/key file missing: %s / %s", certFile, keyFile)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.cache[key]; ok && e.mtimeC.Equal(stC.ModTime()) && e.mtimeK.Equal(stK.ModTime()) {
		return e.cert, nil
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair %s: %w", certFile, err)
	}
	c.cache[key] = &certEntry{certFile: certFile, keyFile: keyFile, mtimeC: stC.ModTime(), mtimeK: stK.ModTime(), cert: &pair}
	logx.Infof("s3: tls certificate loaded/refreshed: %s (hot reload)", certFile)
	return &pair, nil
}

// issueSelfSigned 为自定义域名签发自签证书（SAN 映射到 bucket 域名，365 天），
// 落盘 <db_dir>/certs/<host>.pem|.key。
func (c *certRegistry) issueSelfSigned(host string) (certFile, keyFile string, err error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", err
	}
	tmpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host, Organization: []string{"GlacierTDB S3"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{host},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return "", "", err
	}
	dir := filepath.Join(c.store.dir, "certs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", err
	}
	certFile = filepath.Join(dir, host+".pem")
	keyFile = filepath.Join(dir, host+".key")
	certOut, err := os.Create(certFile)
	if err != nil {
		return "", "", err
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		certOut.Close()
		return "", "", err
	}
	certOut.Close()
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return "", "", err
	}
	keyOut, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", "", err
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		keyOut.Close()
		return "", "", err
	}
	keyOut.Close()
	return certFile, keyFile, nil
}

// AddDomainCert 注册 bucket 自定义域名 + 证书（issue=true 自签发；否则用给定文件）。
func (c *certRegistry) AddDomainCert(host, bucket string, issue bool, certFile, keyFile string) error {
	if issue {
		cf, kf, err := c.issueSelfSigned(host)
		if err != nil {
			return err
		}
		certFile, keyFile = cf, kf
	}
	if err := c.store.AddDomain(host, bucket, certFile, keyFile); err != nil {
		return err
	}
	c.reloadDomains()
	return nil
}

// DropDomainCert 移除自定义域名。
func (c *certRegistry) DropDomainCert(host string) error {
	if err := c.store.DropDomain(host); err != nil {
		return err
	}
	c.reloadDomains()
	return nil
}
