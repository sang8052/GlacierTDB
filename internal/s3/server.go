package s3

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
)

// s3Error S3 XML 风格错误（Code 对齐 AWS）。
type s3Error struct {
	Code   string
	Status int
	Msg    string
}

func (e *s3Error) Error() string { return e.Code + ": " + e.Msg }

// ErrCode 提取 S3 错误码与消息（管理面映射 MySQL 错误用）。
func ErrCode(err error) (code, msg string, ok bool) {
	if e, is := err.(*s3Error); is {
		return e.Code, e.Msg, true
	}
	return "", "", false
}

// CertCtl 自定义域名 + 独立证书管理（管理面 ALTER BUCKET ... DOMAIN 用）。
type CertCtl struct {
	reg *certRegistry
}

// NewCertCtl 构造域名证书管理器（与端点注册表共享系统库，刷新走 Server.RefreshCerts）。
func NewCertCtl(store *Store) *CertCtl {
	return &CertCtl{reg: newCertRegistry(store, "", "")}
}

// AddDomainCert 注册域名 + 证书（issue=true 自签发）。
func (c *CertCtl) AddDomainCert(host, bucket string, issue bool, certFile, keyFile string) error {
	return c.reg.AddDomainCert(host, bucket, issue, certFile, keyFile)
}

// DropDomainCert 移除域名。
func (c *CertCtl) DropDomainCert(host string) error {
	return c.reg.DropDomainCert(host)
}

// Server 对象存储 HTTP(S) 服务（多 endpoint，virtual-host + path-style）。
type Server struct {
	cfg        *config.Config
	store      *Store
	auths      *auth.Store
	lim        *limiter
	httpClient *http.Client
	servers    []*http.Server
	certRegs   []*certRegistry
	started    bool
}

// NewServer 创建 S3 服务。
func NewServer(cfg *config.Config, store *Store, auths *auth.Store) *Server {
	s := &Server{
		cfg:   cfg,
		store: store,
		auths: auths,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
	s.lim = newLimiter(store.EffectiveLimit)
	return s
}

// Start 启动全部 endpoint 监听（任一失败立即返回；成功后后台服务）。
func (s *Server) Start() error {
	if s.started {
		return nil
	}
	for _, ep := range s.cfg.S3.Endpoints {
		ln, err := net.Listen("tcp", ep.Listen)
		if err != nil {
			return fmt.Errorf("s3 listen %s: %w", ep.Listen, err)
		}
		reg := newCertRegistry(s.store, ep.CertFile, ep.KeyFile)
		s.certRegs = append(s.certRegs, reg)
		if ep.TLS {
			tlsCfg := &tls.Config{
				GetCertificate: reg.GetCertificate,
				MinVersion:     tls.VersionTLS12,
			}
			ln = tls.NewListener(ln, tlsCfg)
		}
		srv := &http.Server{
			Handler:           &endpointHandler{srv: s, ep: ep, certs: reg},
			ReadHeaderTimeout: 30 * time.Second,
		}
		s.servers = append(s.servers, srv)
		proto := "http"
		if ep.TLS {
			proto = "https"
		}
		logx.Infof("s3: listening on %s://%s (region=%q, virtual-host suffixes=%v)", proto, ep.Listen, s.cfg.S3.Region, ep.ServerNames)
		go func(srv *http.Server, ln net.Listener) {
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				logx.Errorf("s3 serve: %v", err)
			}
		}(srv, ln)
	}
	s.started = true
	// 生命周期（自动删除）后台扫描
	go s.lifecycleLoop()
	return nil
}

func (s *Server) lifecycleLoop() {
	iv := s.cfg.S3.LifecycleIntervalSec
	if iv <= 0 {
		iv = 3600
	}
	t := time.NewTicker(time.Duration(iv) * time.Second)
	defer t.Stop()
	for range t.C {
		s.store.RunLifecycle()
	}
}

// RefreshCerts 管理面变更自定义域名 / 证书后刷新全部端点的域名证书映射。
func (s *Server) RefreshCerts() {
	for _, reg := range s.certRegs {
		reg.reloadDomains()
	}
}

// endpointHandler 单端点请求分发。
type endpointHandler struct {
	srv   *Server
	ep    config.S3Endpoint
	certs *certRegistry
}

func (h *endpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := h.srv
	reqID := genVersionID()[:12]
	w.Header().Set("x-amz-request-id", reqID)
	w.Header().Set("Server", "GlacierTDB")

	// 计费导出接口（供计费系统拉取）
	if strings.HasPrefix(r.URL.Path, "/_glacier/") {
		h.serveInternal(w, r, reqID)
		return
	}

	// 解析桶 / 对象键（virtual-host 或 path-style）
	bucket, key := h.resolveBucketKey(r)

	// SigV4 鉴权（表头或预签名 URL）
	cs, payloadHash, err := verifySigV4(r, s.cfg.S3.Region, s.store.KeyFor)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	s.store.TouchKey(cs.AK)
	owner, _, status, keyPolicy, _ := s.store.KeyFor(cs.AK)
	if status != "active" {
		writeErr(w, r, reqID, &s3Error{Code: "InvalidAccessKeyId", Status: 403, Msg: "The AWS Access Key Id is " + status})
		return
	}

	// 请求策略条件（密钥级 + 桶级，参考阿里云 OSS policy 条件匹配）
	ip := clientIP(r)
	if err := checkPolicies(keyPolicy, s.store.BucketPolicy(bucket), r, ip); err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}

	// 命名空间隔离：key 属主只能访问自己的桶（super 用户全放行）
	if bucket != "" {
		if b, berr := h.srv.store.Bucket(bucket); berr != nil {
			if !(r.Method == http.MethodPut && key == "") {
				// 建桶（PUT /bucket）允许桶不存在，其余 404
				writeErr(w, r, reqID, toS3Err(berr))
				return
			}
		} else if !s.canAccess(owner, b.Owner) {
			writeErr(w, r, reqID, &s3Error{Code: "AccessDenied", Status: 403,
				Msg: "Access Denied: bucket " + bucket + " belongs to another owner"})
			return
		}
	}

	// 限速 / 并发（用户级聚合：多连接合并不超过用户总带宽）
	lim := s.store.EffectiveLimit(bucket)
	release, userTB, lerr := s.lim.acquire(owner, bucket)
	if lerr != nil {
		writeErr(w, r, reqID, toS3Err(lerr))
		return
	}
	defer release()

	switch {
	case bucket == "" && key == "":
		h.handleService(w, r, owner, reqID)
	case key == "":
		h.handleBucket(w, r, bucket, owner, reqID)
	default:
		h.handleObject(w, r, bucket, key, owner, payloadHash, lim, userTB, reqID)
	}
}

// resolveBucketKey virtual-host / path-style 解析（返回已解码 bucket 与 key）。
func (h *endpointHandler) resolveBucketKey(r *http.Request) (bucket, key string) {
	host := r.Host
	if hh, _, err := net.SplitHostPort(host); err == nil {
		host = hh
	}
	host = strings.ToLower(host)
	path := strings.TrimPrefix(r.URL.Path, "/")

	// 1) 自定义域名（SAN 映射到 bucket）
	if d, ok := h.srv.store.DomainFor(host); ok {
		return d.Bucket, path
	}
	// 2) virtual-host：{bucket}[.{region}].{endpoint 后缀}
	for _, name := range h.ep.ServerNames {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		if host == name {
			return splitPathStyle(path)
		}
		if strings.HasSuffix(host, "."+name) {
			prefix := strings.TrimSuffix(host, "."+name)
			if prefix == "" {
				return splitPathStyle(path)
			}
			// 整段即桶名（含点桶名）优先；否则末段按 region 剥离
			if b, err := h.srv.store.Bucket(prefix); err == nil {
				_ = b
				return prefix, path
			}
			if i := strings.LastIndexByte(prefix, '.'); i > 0 {
				maybeBucket := prefix[:i]
				if _, err := h.srv.store.Bucket(maybeBucket); err == nil {
					return maybeBucket, path
				}
			}
			return prefix, path
		}
	}
	// 3) path-style：/{bucket}/{key...}
	return splitPathStyle(path)
}

func splitPathStyle(path string) (bucket, key string) {
	if path == "" {
		return "", ""
	}
	if i := strings.IndexByte(path, '/'); i >= 0 {
		return path[:i], strings.TrimPrefix(path[i:], "/")
	}
	return path, ""
}

// canAccess 命名空间隔离判定。
func (s *Server) canAccess(user, owner string) bool {
	if user == owner {
		return true
	}
	if s.auths != nil && s.auths.UserType(user) == auth.TypeSuper {
		return true
	}
	return false
}

// checkPolicies 密钥级 + 桶级策略条件校验（AND）。
func checkPolicies(keyPolicy, bucketPolicy string, r *http.Request, ip string) error {
	if p, err := parsePolicy(keyPolicy); err != nil {
		return err
	} else if err := p.check(r, ip); err != nil {
		return err
	}
	if p, err := parsePolicy(bucketPolicy); err != nil {
		return err
	} else if err := p.check(r, ip); err != nil {
		return err
	}
	return nil
}

// clientIP 取来源 IP。
func clientIP(r *http.Request) string {
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return host
}

// ---------------------------------------------------------------------------
// 服务级 / 桶级 / 对象级操作
// ---------------------------------------------------------------------------

// handleService GET / → 列桶。
func (h *endpointHandler) handleService(w http.ResponseWriter, r *http.Request, owner, reqID string) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		all := h.srv.store.ListBuckets("")
		super := h.srv.auths != nil && h.srv.auths.UserType(owner) == auth.TypeSuper
		type xmlBucket struct {
			Name         string `xml:"Name"`
			CreationDate string `xml:"CreationDate"`
		}
		var out struct {
			XMLName xml.Name `xml:"ListAllMyBucketsResult"`
			Xmlns   string   `xml:"xmlns,attr"`
			Owner   struct {
				ID          string `xml:"ID"`
				DisplayName string `xml:"DisplayName"`
			} `xml:"Owner"`
			Buckets struct {
				Bucket []xmlBucket `xml:"Bucket"`
			} `xml:"Buckets"`
		}
		out.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
		out.Owner.ID = owner
		out.Owner.DisplayName = owner
		for _, b := range all {
			if b.Owner != owner && !super {
				continue
			}
			out.Buckets.Bucket = append(out.Buckets.Bucket,
				xmlBucket{Name: b.Name, CreationDate: isoTime(b.CreatedTS)})
		}
		writeXML(w, http.StatusOK, out, reqID, r.Method == http.MethodHead)
	default:
		writeErr(w, r, reqID, &s3Error{Code: "MethodNotAllowed", Status: 405, Msg: "The specified method is not allowed against this resource"})
	}
}

// handleBucket 桶级操作（LIST / 建桶 / 删桶 / HEAD / versioning / location）。
func (h *endpointHandler) handleBucket(w http.ResponseWriter, r *http.Request, bucket, owner, reqID string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if _, ok := q["uploads"]; ok {
			h.listMultipartUploads(w, r, bucket, reqID)
			return
		}
		if _, ok := q["versions"]; ok {
			h.listVersions(w, r, bucket, reqID)
			return
		}
		if q.Has("location") {
			reg := ""
			if b, err := h.srv.store.Bucket(bucket); err == nil {
				reg = b.Region
			}
			writeXML(w, http.StatusOK, struct {
				XMLName xml.Name `xml:"LocationConstraint"`
				Xmlns   string   `xml:"xmlns,attr"`
				Value   string   `xml:",chardata"`
			}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Value: reg}, reqID, r.Method == http.MethodHead)
			return
		}
		if q.Has("versioning") {
			status := ""
			if b, err := h.srv.store.Bucket(bucket); err == nil && b.Versioning {
				status = "Enabled"
			}
			writeXML(w, http.StatusOK, struct {
				XMLName xml.Name `xml:"VersioningConfiguration"`
				Xmlns   string   `xml:"xmlns,attr"`
				Status  string   `xml:"Status,omitempty"`
			}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Status: status}, reqID, r.Method == http.MethodHead)
			return
		}
		// HEAD 桶：存在性
		if r.Method == http.MethodHead {
			if _, err := h.srv.store.Bucket(bucket); err != nil {
				writeErr(w, r, reqID, toS3Err(err))
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		h.listObjects(w, r, bucket, reqID)

	case http.MethodPut:
		// 建桶 / PutBucketVersioning
		if q.Has("versioning") {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			enable := strings.Contains(strings.ToUpper(string(body)), "ENABLED")
			b, err := h.srv.store.Bucket(bucket)
			if err != nil {
				writeErr(w, r, reqID, toS3Err(err))
				return
			}
			if _, err := h.srv.store.UpdateBucket(b.Name, nil, &enable, nil, nil, nil, nil, nil, nil); err != nil {
				writeErr(w, r, reqID, toS3Err(err))
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		reg := ""
		if b, err := h.srv.store.CreateBucket(bucket, owner, reg, false, false); err != nil {
			writeErr(w, r, reqID, toS3Err(err))
			return
		} else {
			_ = b
		}
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		force := q.Has("force") || r.Header.Get("X-Glacier-Force") == "1"
		if err := h.srv.store.DropBucket(bucket, force); err != nil {
			writeErr(w, r, reqID, toS3Err(err))
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		writeErr(w, r, reqID, &s3Error{Code: "MethodNotAllowed", Status: 405, Msg: "The specified method is not allowed against this resource"})
	}
}

// listObjects ListObjects V1 / V2。
func (h *endpointHandler) listObjects(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		fmt.Sscanf(v, "%d", &maxKeys)
	}
	marker := q.Get("marker")
	startAfter := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		startAfter = tok
	}
	res, err := h.srv.store.ListObjects(bucket, prefix, delimiter, marker, startAfter, maxKeys)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	type xmlContent struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	type xmlPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	out := struct {
		XMLName        xml.Name `xml:"ListBucketResult"`
		Xmlns          string   `xml:"xmlns,attr"`
		Name           string   `xml:"Name"`
		Prefix         string   `xml:"Prefix"`
		Delimiter      string   `xml:"Delimiter,omitempty"`
		MaxKeys        int      `xml:"MaxKeys"`
		KeyCount       int      `xml:"KeyCount"`
		IsTruncated    bool     `xml:"IsTruncated"`
		NextMarker     string   `xml:"NextMarker,omitempty"`
		Contents       []xmlContent `xml:"Contents"`
		CommonPrefixes []xmlPrefix  `xml:"CommonPrefixes,omitempty"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket, Prefix: prefix,
		Delimiter: delimiter, MaxKeys: maxKeys, IsTruncated: res.IsTruncated, NextMarker: res.NextMarker}
	for _, e := range res.Entries {
		out.Contents = append(out.Contents, xmlContent{
			Key: e.Key, LastModified: isoTime(e.LastModified),
			ETag: "\"" + e.ETag + "\"", Size: e.Size, StorageClass: "STANDARD",
		})
	}
	for _, cp := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, xmlPrefix{Prefix: cp})
	}
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	writeXML(w, http.StatusOK, out, reqID, r.Method == http.MethodHead)
}

// listVersions ListObjectVersions。
func (h *endpointHandler) listVersions(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		fmt.Sscanf(v, "%d", &maxKeys)
	}
	res, err := h.srv.store.ListVersions(bucket, prefix, q.Get("key-marker"), q.Get("version-id-marker"), maxKeys)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	type xmlVer struct {
		Key          string `xml:"Key"`
		VersionID    string `xml:"VersionId"`
		IsLatest     bool   `xml:"IsLatest"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag,omitempty"`
		Size         int64  `xml:"Size,omitempty"`
	}
	type xmlDel struct {
		Key          string `xml:"Key"`
		VersionID    string `xml:"VersionId"`
		IsLatest     bool   `xml:"IsLatest"`
		LastModified string `xml:"LastModified"`
	}
	out := struct {
		XMLName           xml.Name `xml:"ListVersionsResult"`
		Xmlns             string   `xml:"xmlns,attr"`
		Name              string   `xml:"Name"`
		Prefix            string   `xml:"Prefix"`
		MaxKeys           int      `xml:"MaxKeys"`
		IsTruncated       bool     `xml:"IsTruncated"`
		KeyMarker         string   `xml:"KeyMarker,omitempty"`
		VersionIDMarker   string   `xml:"VersionIdMarker,omitempty"`
		NextKeyMarker     string   `xml:"NextKeyMarker,omitempty"`
		NextVersionMarker string   `xml:"NextVersionIdMarker,omitempty"`
		Versions          []xmlVer `xml:"Version"`
		DeleteMarkers     []xmlDel `xml:"DeleteMarker"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket, Prefix: prefix,
		MaxKeys: maxKeys, IsTruncated: res.IsTruncated,
		KeyMarker: q.Get("key-marker"), VersionIDMarker: q.Get("version-id-marker"),
		NextKeyMarker: res.NextKeyMarker, NextVersionMarker: res.NextVerMarker}
	for _, e := range res.Entries {
		ts := isoTime(e.LastModified)
		if e.DeleteMarker {
			out.DeleteMarkers = append(out.DeleteMarkers, xmlDel{
				Key: e.Key, VersionID: e.VersionID, IsLatest: e.IsLatest, LastModified: ts})
			continue
		}
		out.Versions = append(out.Versions, xmlVer{
			Key: e.Key, VersionID: e.VersionID, IsLatest: e.IsLatest,
			LastModified: ts, ETag: "\"" + e.ETag + "\"", Size: e.Size})
	}
	writeXML(w, http.StatusOK, out, reqID, r.Method == http.MethodHead)
}

// handleObject 对象级操作（PUT / GET / HEAD / DELETE / COPY / 分片上传）。
func (h *endpointHandler) handleObject(w http.ResponseWriter, r *http.Request, bucket, key, owner,
	payloadHash string, lim config.S3Limit, userTB *tokenBucket, reqID string) {
	q := r.URL.Query()
	versionID := q.Get("versionId")
	uploadID := q.Get("uploadId")
	img, hasImg := parseImgParams(map[string][]string(q))

	// 分片上传（multipart，v1.3.1）：uploadId 维度路由
	if uploadID != "" {
		switch r.Method {
		case http.MethodPut:
			h.uploadPart(w, r, bucket, key, uploadID, payloadHash, reqID)
		case http.MethodPost:
			h.completeMultipart(w, r, bucket, key, uploadID, owner, reqID)
		case http.MethodDelete:
			h.abortMultipart(w, r, bucket, key, uploadID, reqID)
		case http.MethodGet, http.MethodHead:
			h.listParts(w, r, bucket, key, uploadID, reqID)
		default:
			writeErr(w, r, reqID, &s3Error{Code: "MethodNotAllowed", Status: 405, Msg: "The specified method is not allowed against this resource"})
		}
		return
	}

	switch r.Method {
	case http.MethodPut:
		if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
			h.copyObject(w, r, bucket, key, src, reqID)
			return
		}
		h.putObject(w, r, bucket, key, owner, payloadHash, img, hasImg, reqID)

	case http.MethodPost:
		// POST /{bucket}/{key}?uploads → InitiateMultipartUpload
		if q.Has("uploads") {
			h.initiateMultipart(w, r, bucket, key, reqID)
			return
		}
		writeErr(w, r, reqID, &s3Error{Code: "MethodNotAllowed", Status: 405, Msg: "The specified method is not allowed against this resource"})

	case http.MethodGet, http.MethodHead:
		h.getObject(w, r, bucket, key, versionID, owner, img, hasImg, lim, userTB, reqID)

	case http.MethodDelete:
		bypass := strings.EqualFold(r.Header.Get("X-Amz-Bypass-Governance-Retention"), "true")
		m, err := h.srv.store.DeleteObject(bucket, key, versionID, bypass)
		if err != nil {
			writeErr(w, r, reqID, toS3Err(err))
			return
		}
		if m.VersionID != "" {
			w.Header().Set("x-amz-version-id", m.VersionID)
		}
		if m.DeleteMarker {
			w.Header().Set("x-amz-delete-marker", "true")
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		writeErr(w, r, reqID, &s3Error{Code: "MethodNotAllowed", Status: 405, Msg: "The specified method is not allowed against this resource"})
	}
}

// putObject 上传对象（支持 base64 上传解码、aws-chunked 流式、负载哈希校验）。
func (h *endpointHandler) putObject(w http.ResponseWriter, r *http.Request, bucket, key, owner,
	payloadHash string, img imgParams, hasImg bool, reqID string) {
	body := io.Reader(r.Body)
	contentEnc := r.Header.Get("Content-Encoding")
	if payloadHash == payloadStreaming || strings.Contains(strings.ToLower(contentEnc), "aws-chunked") {
		body = newAWSChunkedReader(r.Body)
	}
	// 负载哈希校验（BadDigest）：签名已覆盖声明值，此处验证实际字节
	var hr io.Reader = body
	if payloadHash != "" && payloadHash != payloadUnsigned && payloadHash != payloadStreaming {
		hr = &hashCheckReader{r: body, h: sha256.New(), expected: payloadHash}
	}

	attrs := ObjectAttrs{
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		Metadata:           map[string]string{},
	}
	for k, vs := range r.Header {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-meta-") && len(vs) > 0 {
			attrs.Metadata[strings.TrimPrefix(lk, "x-amz-meta-")] = vs[0]
		}
	}
	if attrs.ContentEncoding == "aws-chunked" {
		attrs.ContentEncoding = ""
	}

	// base64 图片直传：解码后二进制落盘
	if hasImg && img.Base64 {
		var raw []byte
		var err error
		if h.srv.store.maxObject > 0 {
			raw, err = io.ReadAll(io.LimitReader(hr, h.srv.store.maxObject*2))
		} else {
			raw, err = io.ReadAll(hr) // 不限上限（v1.3.5：max_object_mb 负值）
		}
		if err != nil {
			writeErr(w, r, reqID, toS3Err(err))
			return
		}
		decoded, err := decodeBase64Body(raw)
		if err != nil {
			writeErr(w, r, reqID, &s3Error{Code: "InvalidArgument", Status: 400, Msg: err.Error()})
			return
		}
		m, replaced, perr := h.srv.store.PutObject(bucket, key, bytes.NewReader(decoded), int64(len(decoded)), attrs)
		if perr != nil {
			writeErr(w, r, reqID, toS3Err(perr))
			return
		}
		h.finishPut(w, r, bucket, key, m, replaced, owner, reqID)
		return
	}

	m, replaced, err := h.srv.store.PutObject(bucket, key, hr, r.ContentLength, attrs)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	h.finishPut(w, r, bucket, key, m, replaced, owner, reqID)
}

func (h *endpointHandler) finishPut(w http.ResponseWriter, r *http.Request, bucket, key string,
	m ObjectMeta, replaced bool, owner, reqID string) {
	if b, err := h.srv.store.Bucket(bucket); err == nil {
		h.srv.notifyPut(b, m, replaced)
	}
	// 记账（PUT + bytes_in）已在 store.PutObject 内按桶属主完成
	w.Header().Set("ETag", "\""+m.ETag+"\"")
	if m.VersionID != "" {
		w.Header().Set("x-amz-version-id", m.VersionID)
	}
	w.WriteHeader(http.StatusOK)
}

// getObject 下载对象（限速 + 图片加工 + 记账）。
func (h *endpointHandler) getObject(w http.ResponseWriter, r *http.Request, bucket, key, versionID, owner string,
	img imgParams, hasImg bool, lim config.S3Limit, userTB *tokenBucket, reqID string) {
	headOnly := r.Method == http.MethodHead

	// 图片加工链路（裁剪 / 缩放 / 格式转换 / base64 输出）需整体加工
	if hasImg && (img.ResizeW > 0 || img.ResizeH > 0 || !img.Crop.Empty() || img.Format != "" || img.Base64) {
		m, data, err := h.srv.store.GetObjectBytes(bucket, key, versionID)
		if err != nil {
			writeErr(w, r, reqID, toS3Err(err))
			return
		}
		out, ct, err := processImage(data, img)
		if err != nil {
			writeErr(w, r, reqID, &s3Error{Code: "InvalidArgument", Status: 400, Msg: err.Error()})
			return
		}
		h.srv.store.AddUsage(usageOwner(h.srv, bucket, owner), bucket, "GET", 0, int64(len(out)))
		if ct != "" {
			w.Header().Set("Content-Type", ct)
		} else {
			w.Header().Set("Content-Type", orDefault(m.ContentType, "application/octet-stream"))
		}
		w.Header().Set("ETag", "\""+m.ETag+"\"")
		w.Header().Set("Content-Length", strconv.Itoa(len(out)))
		if headOnly {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, cw := newLimitedWriter(w, lim.PerConnBps, userTB)
		_, _ = cw.Write(out)
		return
	}

	m, rc, err := h.srv.store.GetObject(bucket, key, versionID)
	if err != nil {
		if se, ok := err.(*s3Error); ok && se.Code == "NoSuchKey" && versionID == "" {
			// 删除标记 GET：404 + x-amz-delete-marker
			if dm, derr := h.srv.store.HeadObject(bucket, key, ""); derr == nil && dm.DeleteMarker {
				w.Header().Set("x-amz-delete-marker", "true")
			}
		}
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	defer rc.Close()

	// 内容协商头
	if ct := r.Header.Get("Response-Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", orDefault(m.ContentType, "application/octet-stream"))
	}
	if m.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", m.ContentEncoding)
	}
	if m.CacheControl != "" {
		w.Header().Set("Cache-Control", m.CacheControl)
	}
	if m.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", m.ContentDisposition)
	}
	for k, v := range m.Metadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	w.Header().Set("ETag", "\""+m.ETag+"\"")
	if m.VersionID != "" {
		w.Header().Set("x-amz-version-id", m.VersionID)
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", m.CreatedTS, time.Local); err == nil {
		w.Header().Set("Last-Modified", t.UTC().Format(http.TimeFormat))
	}
	if m.RetentionMode != "" {
		w.Header().Set("x-amz-object-lock-mode", m.RetentionMode)
		w.Header().Set("x-amz-object-lock-retain-until-date", m.RetainUntil)
	}
	if m.LegalHold {
		w.Header().Set("x-amz-object-lock-legal-hold", "ON")
	}
	if headOnly {
		w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
	w.WriteHeader(http.StatusOK)
	_, cw := newLimitedWriter(w, lim.PerConnBps, userTB)
	_, _ = io.Copy(cw, rc)
	h.srv.store.AddUsage(usageOwner(h.srv, bucket, owner), bucket, "GET", 0, cw.count)
}

// usageOwner 记账归属：桶属主（计费实体），查不到回退请求用户。
func usageOwner(s *Server, bucket, fallback string) string {
	if b, err := s.store.Bucket(bucket); err == nil {
		return b.Owner
	}
	return fallback
}

// copyObject x-amz-copy-source 复制。
func (h *endpointHandler) copyObject(w http.ResponseWriter, r *http.Request, bucket, key, src, reqID string) {
	src = strings.TrimPrefix(src, "/")
	srcBucket, srcKey := splitPathStyle(src)
	srcVersion := ""
	if i := strings.Index(srcKey, "?"); i >= 0 {
		qs := srcKey[i+1:]
		srcKey = srcKey[:i]
		for _, kv := range strings.Split(qs, "&") {
			if strings.HasPrefix(kv, "versionId=") {
				srcVersion = strings.TrimPrefix(kv, "versionId=")
			}
		}
	}
	attrs := ObjectAttrs{
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		Metadata:           map[string]string{},
	}
	if strings.EqualFold(r.Header.Get("X-Amz-Metadata-Directive"), "COPY") {
		attrs = ObjectAttrs{Metadata: map[string]string{}}
	}
	m, err := h.srv.store.CopyObject(srcBucket, srcKey, srcVersion, bucket, key, attrs)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	writeXML(w, http.StatusOK, struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
	}{ETag: "\"" + m.ETag + "\"", LastModified: isoTime(m.CreatedTS)}, reqID, false)
}

// ---------------------------------------------------------------------------
// 分片上传（multipart，v1.3.1）
// ---------------------------------------------------------------------------

// putAttrs 从请求头提取对象写入属性（PUT / InitiateMultipartUpload 共用）。
func putAttrs(r *http.Request) ObjectAttrs {
	attrs := ObjectAttrs{
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		Metadata:           map[string]string{},
	}
	for k, vs := range r.Header {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-meta-") && len(vs) > 0 {
			attrs.Metadata[strings.TrimPrefix(lk, "x-amz-meta-")] = vs[0]
		}
	}
	if attrs.ContentEncoding == "aws-chunked" {
		attrs.ContentEncoding = ""
	}
	return attrs
}

// initiateMultipart POST /{bucket}/{key}?uploads → InitiateMultipartUpload。
func (h *endpointHandler) initiateMultipart(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	uploadID, err := h.srv.store.InitiateMultipart(bucket, key, putAttrs(r))
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	writeXML(w, http.StatusOK, struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Bucket: bucket, Key: key, UploadID: uploadID}, reqID, false)
}

// uploadPart PUT /{bucket}/{key}?partNumber=N&uploadId=... → UploadPart（ETag = 分片 MD5）。
func (h *endpointHandler) uploadPart(w http.ResponseWriter, r *http.Request, bucket, key, uploadID,
	payloadHash, reqID string) {
	partNumber, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if partNumber <= 0 {
		writeErr(w, r, reqID, &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: "partNumber must be a positive integer"})
		return
	}
	// UploadPartCopy（v1.3.5）：PUT ?partNumber 带 x-amz-copy-source → 从源对象拷贝分片
	if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
		h.uploadPartCopy(w, r, bucket, key, uploadID, partNumber, src, reqID)
		return
	}
	body := io.Reader(r.Body)
	chunked := payloadHash == payloadStreaming ||
		strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked")
	if chunked {
		body = newAWSChunkedReader(r.Body)
	}
	var hr io.Reader = body
	if payloadHash != "" && payloadHash != payloadUnsigned && payloadHash != payloadStreaming {
		hr = &hashCheckReader{r: body, h: sha256.New(), expected: payloadHash}
	}
	sizeHint := r.ContentLength
	if chunked {
		sizeHint = -1
	}
	etag, err := h.srv.store.UploadPart(bucket, key, uploadID, partNumber, hr, sizeHint)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	w.Header().Set("ETag", "\""+etag+"\"")
	w.WriteHeader(http.StatusOK)
}

// uploadPartCopy PUT ?partNumber + x-amz-copy-source → UploadPartCopy（v1.3.5）。
// 可选 x-amz-copy-source-range: bytes=first-last（闭区间）；响应为 CopyPartResult XML。
func (h *endpointHandler) uploadPartCopy(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string,
	partNumber int, src, reqID string) {
	src = strings.TrimPrefix(src, "/")
	srcBucket, srcKey := splitPathStyle(src)
	srcVersion := ""
	if i := strings.Index(srcKey, "?"); i >= 0 {
		for _, kv := range strings.Split(srcKey[i+1:], "&") {
			if strings.HasPrefix(kv, "versionId=") {
				srcVersion = strings.TrimPrefix(kv, "versionId=")
			}
		}
		srcKey = srcKey[:i]
	}
	start, end := int64(0), int64(0)
	hasRange := false
	if cr := r.Header.Get("X-Amz-Copy-Source-Range"); cr != "" {
		m := regexp.MustCompile(`^bytes=(\d+)-(\d+)$`).FindStringSubmatch(strings.TrimSpace(cr))
		if m == nil {
			writeErr(w, r, reqID, &s3Error{Code: "InvalidArgument", Status: 400,
				Msg: "The x-amz-copy-source-range value is invalid"})
			return
		}
		start, _ = strconv.ParseInt(m[1], 10, 64)
		end, _ = strconv.ParseInt(m[2], 10, 64)
		hasRange = true
	}
	etag, lastMod, err := h.srv.store.UploadPartCopy(bucket, key, uploadID, partNumber,
		srcBucket, srcKey, srcVersion, start, end, hasRange)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	writeXML(w, http.StatusOK, struct {
		XMLName      xml.Name `xml:"CopyPartResult"`
		LastModified string   `xml:"LastModified"`
		ETag         string   `xml:"ETag"`
	}{LastModified: lastMod, ETag: "\"" + etag + "\""}, reqID, false)
}

// completeMultipart POST /{bucket}/{key}?uploadId=... → CompleteMultipartUpload。
func (h *endpointHandler) completeMultipart(w http.ResponseWriter, r *http.Request, bucket, key, uploadID,
	owner, reqID string) {
	var body struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&body); err != nil {
		writeErr(w, r, reqID, &s3Error{Code: "MalformedXML", Status: 400,
			Msg: "The XML you provided was not well-formed"})
		return
	}
	parts := make([]PartSpec, 0, len(body.Parts))
	for _, p := range body.Parts {
		parts = append(parts, PartSpec{PartNumber: p.PartNumber, ETag: p.ETag})
	}
	m, replaced, err := h.srv.store.CompleteMultipart(bucket, key, uploadID, parts)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	if b, berr := h.srv.store.Bucket(bucket); berr == nil {
		h.srv.notifyPut(b, m, replaced)
	}
	writeXML(w, http.StatusOK, struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Location: "/" + bucket + "/" + key,
		Bucket: bucket, Key: key, ETag: "\"" + m.ETag + "\""}, reqID, false)
}

// abortMultipart DELETE /{bucket}/{key}?uploadId=... → AbortMultipartUpload。
func (h *endpointHandler) abortMultipart(w http.ResponseWriter, r *http.Request, bucket, key, uploadID, reqID string) {
	if err := h.srv.store.AbortMultipart(bucket, key, uploadID); err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// listParts GET /{bucket}/{key}?uploadId=... → ListParts。
func (h *endpointHandler) listParts(w http.ResponseWriter, r *http.Request, bucket, key, uploadID, reqID string) {
	parts, err := h.srv.store.ListParts(bucket, key, uploadID)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	type xmlPart struct {
		PartNumber   int    `xml:"PartNumber"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	}
	out := struct {
		XMLName  xml.Name  `xml:"ListPartsResult"`
		Xmlns    string    `xml:"xmlns,attr"`
		Bucket   string    `xml:"Bucket"`
		Key      string    `xml:"Key"`
		UploadID string    `xml:"UploadId"`
		MaxParts int       `xml:"MaxParts"`
		IsTruncated bool   `xml:"IsTruncated"`
		Parts    []xmlPart `xml:"Part"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Bucket: bucket, Key: key,
		UploadID: uploadID, MaxParts: maxParts}
	for _, p := range parts {
		out.Parts = append(out.Parts, xmlPart{
			PartNumber: p.PartNumber, LastModified: isoTime(p.CreatedTS),
			ETag: "\"" + p.ETag + "\"", Size: p.Size,
		})
	}
	writeXML(w, http.StatusOK, out, reqID, r.Method == http.MethodHead)
}

// listMultipartUploads GET /{bucket}?uploads → ListMultipartUploads。
func (h *endpointHandler) listMultipartUploads(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	q := r.URL.Query()
	uploads := h.srv.store.ListMultipartUploads(bucket, q.Get("prefix"))
	type xmlUpload struct {
		Key       string `xml:"Key"`
		UploadID  string `xml:"UploadId"`
		Initiated string `xml:"Initiated"`
	}
	out := struct {
		XMLName     xml.Name    `xml:"ListMultipartUploadsResult"`
		Xmlns       string      `xml:"xmlns,attr"`
		Bucket      string      `xml:"Bucket"`
		Prefix      string      `xml:"Prefix,omitempty"`
		MaxUploads  int         `xml:"MaxUploads"`
		IsTruncated bool        `xml:"IsTruncated"`
		Uploads     []xmlUpload `xml:"Upload"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Bucket: bucket,
		Prefix: q.Get("prefix"), MaxUploads: 1000}
	for _, u := range uploads {
		out.Uploads = append(out.Uploads, xmlUpload{
			Key: u.Key, UploadID: u.UploadID, Initiated: isoTime(u.CreatedTS)})
	}
	writeXML(w, http.StatusOK, out, reqID, r.Method == http.MethodHead)
}

// ---------------------------------------------------------------------------
// 计费导出接口（供计费系统拉取）
// ---------------------------------------------------------------------------

func (h *endpointHandler) serveInternal(w http.ResponseWriter, r *http.Request, reqID string) {
	s := h.srv
	cs, _, err := verifySigV4(r, s.cfg.S3.Region, s.store.KeyFor)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	owner, _, _, _, _ := s.store.KeyFor(cs.AK)
	target := r.URL.Query().Get("owner")
	format := strings.ToLower(r.URL.Query().Get("format"))
	super := s.auths != nil && s.auths.UserType(owner) == auth.TypeSuper
	if !super {
		target = owner // 非超管只能拉自己的账单
	}
	if strings.HasSuffix(r.URL.Path, "/usage") {
		if format == "csv" {
			w.Header().Set("Content-Type", "text/csv; charset=utf-8")
			w.Header().Set("Content-Disposition", "attachment; filename=\"s3_usage.csv\"")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(s.store.UsageCSV(target)))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(s.store.Usage(target))
		return
	}
	writeErr(w, r, reqID, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "unknown internal resource"})
}

// ---------------------------------------------------------------------------
// XML 应答工具
// ---------------------------------------------------------------------------

func writeXML(w http.ResponseWriter, status int, v any, reqID string, headOnly bool) {
	w.Header().Set("Content-Type", "application/xml")
	body, err := xml.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	payload := append([]byte(xml.Header), body...)
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	if !headOnly {
		_, _ = w.Write(payload)
	}
}

func writeErr(w http.ResponseWriter, r *http.Request, reqID string, e *s3Error) {
	if e == nil {
		e = &s3Error{Code: "InternalError", Status: 500, Msg: "internal error"}
	}
	logx.Debugf("s3 error %d %s (req %s %s): %s", e.Status, e.Code, r.Method, r.URL.Path, e.Msg)
	writeXML(w, e.Status, struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Resource  string   `xml:"Resource"`
		RequestID string   `xml:"RequestId"`
	}{Code: e.Code, Message: e.Msg, Resource: r.URL.Path, RequestID: reqID}, reqID, r.Method == http.MethodHead)
}

// toS3Err 统一错误转换。
func toS3Err(err error) *s3Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*s3Error); ok {
		return e
	}
	return &s3Error{Code: "InternalError", Status: 500, Msg: err.Error()}
}

// isoTime "2006-01-02 15:04:05"（本地）→ ISO8601 UTC。
func isoTime(ts string) string {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.Local)
	if err != nil {
		return ts
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// hashCheckReader 读取时同步哈希，EOF 时校验与声明一致（BadDigest）。
type hashCheckReader struct {
	r        io.Reader
	h        hash.Hash
	expected string
}

func (c *hashCheckReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.h.Write(p[:n])
	}
	if err == io.EOF {
		if got := fmt.Sprintf("%x", c.h.Sum(nil)); got != strings.ToLower(c.expected) {
			return n, &s3Error{Code: "BadDigest", Status: 400,
				Msg: "The Content-MD5 or x-amz-content-sha256 you specified did not match what we received"}
		}
	}
	return n, err
}
