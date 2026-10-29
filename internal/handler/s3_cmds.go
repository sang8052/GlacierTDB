package handler

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
	"glaciertdb/internal/s3"
)

// handleS3Admin 对象存储管理命令（v1.1.0，USE GlacierTDB 管理面）。
// 语法（关键词大小写不敏感，字符串值单引号）：
//
//	CREATE BUCKET <name> [OWNER '<u>'] [REGION '<r>'] [VERSIONING ON|OFF] [OBJECT_LOCK ON|OFF]
//	DROP BUCKET <name> [FORCE]                    SHOW BUCKETS
//	ALTER BUCKET <name> (VERSIONING|OBJECT_LOCK) ON|OFF
//	ALTER BUCKET <name> OWNER '<u>'
//	ALTER BUCKET <name> LIMIT (CONN_BPS|USER_BPS|USER_CONNS) = <n>
//	ALTER BUCKET <name> ADD DOMAIN '<host>' (ISSUE | CERT FILE '<pem>' KEY FILE '<key>')
//	ALTER BUCKET <name> DROP DOMAIN '<host>'      SHOW S3 DOMAINS
//	SET S3 CALLBACK <bucket> URL '<url>' [SECRET '<s>']   CLEAR S3 CALLBACK <bucket>
//	SET S3 LIFECYCLE <bucket> PREFIX '<p>' DAYS <n>       CLEAR S3 LIFECYCLE <bucket> [PREFIX '<p>']
//	SHOW S3 LIFECYCLE [bucket]
//	CREATE S3 KEY [FOR <user>]   RENEW S3 KEY <ak>   (DISABLE|ENABLE|DROP) S3 KEY <ak>
//	SHOW S3 KEYS [FOR <user>]
//	SET S3 POLICY (KEY <ak>|BUCKET <b>) '<json>'    CLEAR S3 POLICY (KEY <ak>|BUCKET <b>)
//	SHOW S3 POLICIES
//	SHOW S3 USAGE [FOR <user>]   EXPORT S3 USAGE [FOR <user>] [TO '<file>']
//	SHOW S3 OBJECTS <bucket> [LIKE '<prefix>'] [LIMIT <n>]
//	SHOW S3 UPLOADS [<bucket>]（分片上传会话，v1.3.1）
//	SHOW S3 SPACE（系统库页/空闲页统计，v1.3.11）
//	S3 VACUUM [FULL]（空间回收：默认增量免长锁；FULL 排他重建并切换增量模式，v1.3.11）
//	S3 RETAIN <bucket>/<key> UNTIL '<date>' (COMPLIANCE|GOVERNANCE) [VERSION '<v>']
//	S3 LEGAL_HOLD <bucket>/<key> (ON|OFF) [VERSION '<v>']
//	GRANT S3 <bucket> TO <user> [READONLY|READWRITE]   REVOKE S3 <bucket> FROM <user>
//	SHOW S3 GRANTS [FOR <user> | FOR BUCKET <b>]（桶共享，v1.3.14 §1）
//	CREATE S3 KEY [FOR <u>] [BUCKETS (...)] [PREFIXES (...)] [READONLY]
//	ALTER S3 KEY <ak> SET SCOPE [BUCKETS (...)] [PREFIXES (...)] [READONLY|READWRITE|CLEAR]
//	SHOW S3 KEYS（含 scope 摘要列，v1.3.14 §2）
//	CREATE S3 TOKEN ON <bucket>/<key> [PUT|GET|GIMG('<params>')] [EXPIRE <s≤3600>]
//	REVOKE S3 TOKEN '<t>'   SHOW S3 TOKENS [FOR <u> | ON <b>]（魔法 Token，v1.3.14 §4）
//	SHOW S3 USAGE OPERATORS（操作者维度分摊，v1.3.14 §1）
func (h *Handler) handleS3Admin(q string) (*mysql.Result, bool, error) {
	toks := lexSQL(q)
	if len(toks) == 0 {
		return nil, false, nil
	}
	up := func(i int) string {
		if i < len(toks) {
			return strings.ToUpper(toks[i])
		}
		return ""
	}
	if !isS3Command(up) {
		return nil, false, nil
	}
	if h.s3store == nil {
		return nil, true, mysql.NewError(1105, "S3 object storage module is disabled (set s3.enable: true)")
	}

	switch up(0) {
	case "CREATE":
		if up(1) == "BUCKET" {
			return h.s3CreateBucket(toks)
		}
		if up(1) == "S3" && up(2) == "KEY" {
			return h.s3CreateKey(toks)
		}
		if up(1) == "S3" && up(2) == "TOKEN" {
			return h.s3CreateToken(toks)
		}
	case "GRANT":
		if up(1) == "S3" {
			return h.s3GrantBucket(toks)
		}
	case "REVOKE":
		if up(1) == "S3" && up(2) == "TOKEN" {
			return h.s3RevokeToken(toks)
		}
		if up(1) == "S3" {
			return h.s3RevokeBucket(toks)
		}
	case "DROP":
		if up(1) == "BUCKET" {
			return h.s3DropBucket(toks)
		}
		if up(1) == "S3" && up(2) == "KEY" {
			return h.s3KeyStatus(toks, "revoked")
		}
	case "SHOW":
		switch {
		case up(1) == "BUCKETS":
			return h.s3ShowBuckets()
		case up(1) == "S3" && up(2) == "KEYS":
			return h.s3ShowKeys(toks)
		case up(1) == "S3" && up(2) == "USAGE":
			return h.s3ShowUsage(toks)
		case up(1) == "S3" && up(2) == "GRANTS":
			return h.s3ShowGrants(toks)
		case up(1) == "S3" && up(2) == "TOKENS":
			return h.s3ShowTokens(toks)
		case up(1) == "S3" && up(2) == "LIFECYCLE":
			return h.s3ShowLifecycle(toks)
		case up(1) == "S3" && up(2) == "DOMAINS":
			return h.s3ShowDomains()
		case up(1) == "S3" && up(2) == "POLICIES":
			return h.s3ShowPolicies()
		case up(1) == "S3" && up(2) == "OBJECTS":
			return h.s3ShowObjects(toks)
		case up(1) == "S3" && up(2) == "UPLOADS":
			return h.s3ShowUploads(toks)
		case up(1) == "S3" && up(2) == "SPACE":
			return h.s3ShowSpace()
		case up(1) == "S3" && up(2) == "SYNC":
			return h.s3ShowSync()
		}
	case "ALTER":
		if up(1) == "BUCKET" {
			return h.s3AlterBucket(toks)
		}
		if up(1) == "S3" && up(2) == "KEY" {
			return h.s3AlterKeyScope(toks)
		}
	case "SET":
		if up(1) == "S3" && up(2) == "CALLBACK" {
			return h.s3SetCallback(toks)
		}
		if up(1) == "S3" && up(2) == "LIFECYCLE" {
			return h.s3SetLifecycle(toks)
		}
		if up(1) == "S3" && up(2) == "POLICY" {
			return h.s3SetPolicy(toks)
		}
	case "CLEAR":
		if up(1) == "S3" && up(2) == "CALLBACK" {
			return h.s3ClearCallback(toks)
		}
		if up(1) == "S3" && up(2) == "LIFECYCLE" {
			return h.s3ClearLifecycle(toks)
		}
		if up(1) == "S3" && up(2) == "POLICY" {
			return h.s3ClearPolicy(toks)
		}
	case "RENEW":
		if up(1) == "S3" && up(2) == "KEY" {
			return h.s3RenewKey(toks)
		}
	case "DISABLE", "ENABLE":
		if up(1) == "S3" && up(2) == "KEY" {
			st := "disabled"
			if up(0) == "ENABLE" {
				st = "active"
			}
			return h.s3KeyStatus(toks, st)
		}
	case "EXPORT":
		if up(1) == "S3" && up(2) == "USAGE" {
			return h.s3ExportUsage(toks)
		}
	case "S3":
		if up(1) == "RETAIN" {
			return h.s3Retain(toks)
		}
		if up(1) == "LEGAL_HOLD" {
			return h.s3LegalHold(toks)
		}
		if up(1) == "VACUUM" {
			return h.s3Vacuum(toks)
		}
	}
	return nil, false, nil
}

// isS3Command 判断是否 S3 管理命令（先于分发确认，便于模块关闭时精确报错）。
func isS3Command(up func(int) string) bool {
	switch up(0) {
	case "CREATE", "DROP":
		if up(1) == "BUCKET" {
			return true
		}
		if up(1) == "S3" {
			return up(2) == "KEY" || up(2) == "TOKEN" // TOKEN（v1.3.14 §4）
		}
		return false
	case "GRANT", "REVOKE":
		// 桶共享授权 / token 吊销（v1.3.14 §1/§4）；DB 授权 GRANT 不带 S3 前缀不冲突
		if up(1) == "S3" {
			return true
		}
		return false
	case "SHOW":
		return up(1) == "BUCKETS" || (up(1) == "S3" && (up(2) == "KEYS" || up(2) == "USAGE" ||
			up(2) == "LIFECYCLE" || up(2) == "DOMAINS" || up(2) == "POLICIES" || up(2) == "OBJECTS" ||
			up(2) == "UPLOADS" || up(2) == "SPACE" || up(2) == "SYNC" ||
			up(2) == "GRANTS" || up(2) == "TOKENS")) // v1.3.14 §1/§4
	case "ALTER":
		return up(1) == "BUCKET" || (up(1) == "S3" && up(2) == "KEY") // SET SCOPE（v1.3.14 §2）
	case "SET", "CLEAR":
		return up(1) == "S3" && (up(2) == "CALLBACK" || up(2) == "LIFECYCLE" || up(2) == "POLICY")
	case "RENEW":
		return up(1) == "S3" && up(2) == "KEY"
	case "DISABLE", "ENABLE":
		return up(1) == "S3" && up(2) == "KEY"
	case "EXPORT":
		return up(1) == "S3" && up(2) == "USAGE"
	case "S3":
		return up(1) == "RETAIN" || up(1) == "LEGAL_HOLD" || up(1) == "VACUUM"
	}
	return false
}

// lexSQL 词法：单引号字符串为一个 token（保留原值），其余按空白切分。
func lexSQL(q string) []string {
	var toks []string
	i := 0
	for i < len(q) {
		c := q[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		if c == '\'' {
			j := i + 1
			var sb strings.Builder
			for j < len(q) {
				if q[j] == '\'' {
					if j+1 < len(q) && q[j+1] == '\'' {
						sb.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				sb.WriteByte(q[j])
				j++
			}
			toks = append(toks, sb.String())
			i = j + 1
			continue
		}
		j := i
		for j < len(q) && q[j] != ' ' && q[j] != '\t' && q[j] != '\n' && q[j] != '\r' {
			j++
		}
		toks = append(toks, strings.Trim(q[i:j], "`\""))
		i = j
	}
	return toks
}

// argVal 取第 i 个 token（可选语义：越界返回空）。
func argVal(toks []string, i int) string {
	if i < len(toks) {
		return toks[i]
	}
	return ""
}

// ---------------------------------------------------------------------------
// 桶
// ---------------------------------------------------------------------------

func (h *Handler) s3CreateBucket(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	if len(toks) < 3 {
		return nil, true, mysql.NewError(1064, "CREATE BUCKET <name> [OWNER '<u>'] [REGION '<r>'] [VERSIONING ON|OFF] [OBJECT_LOCK ON|OFF]")
	}
	name := toks[2]
	owner := h.cfg.S3.DefaultOwner
	versioning, objectLock := false, false
	for i := 3; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "OWNER":
			i++
			owner = argVal(toks, i)
		case "REGION":
			// v1.3.12：桶不再单独存 region（签名校验与存储均走全局配置），
			// 子句仅为兼容旧脚本保留，值被忽略
			i++
			_ = argVal(toks, i)
		case "VERSIONING":
			i++
			versioning = strings.EqualFold(argVal(toks, i), "ON")
		case "OBJECT_LOCK":
			i++
			objectLock = strings.EqualFold(argVal(toks, i), "ON")
		}
	}
	// 属主必须是系统账号（命名空间隔离）
	if h.auths != nil {
		if _, ok, _ := h.auths.Lookup(owner); !ok {
			return nil, true, mysql.NewError(1105, fmt.Sprintf("no such account %q (bucket owner must be an existing user)", owner))
		}
	}
	b, err := h.s3store.CreateBucket(name, owner, "", versioning, objectLock)
	if err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: bucket %s created by conn %d (owner=%s)", b.Name, h.connID, b.Owner)
	res, err := meta.SimpleRS([]string{"bucket", "owner", "region", "versioning", "object_lock"},
		[][]any{{b.Name, b.Owner, h.s3store.DisplayRegion(), b2s(b.Versioning), b2s(b.ObjectLock)}})
	return res, true, err
}

func (h *Handler) s3DropBucket(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	if len(toks) < 3 {
		return nil, true, mysql.NewError(1064, "DROP BUCKET <name> [FORCE]")
	}
	force := false
	for i := 3; i < len(toks); i++ {
		if strings.EqualFold(toks[i], "FORCE") {
			force = true
		}
	}
	if err := h.s3store.DropBucket(toks[2], force); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: bucket %s dropped by conn %d (force=%v)", toks[2], h.connID, force)
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3ShowBuckets() (*mysql.Result, bool, error) {
	reg := h.s3store.DisplayRegion() // v1.3.12：region 全局统一展示
	var rows [][]any
	for _, b := range h.s3store.ListBuckets("") {
		rows = append(rows, []any{b.Name, b.Owner, reg, b2s(b.Versioning), b2s(b.ObjectLock),
			b.ObjectCount, b.StorageBytes, b.CallbackURL, b.CreatedTS})
	}
	res, err := meta.SimpleRS([]string{"bucket", "owner", "region", "versioning", "object_lock",
		"objects", "storage_bytes", "callback_url", "created"}, rows)
	return res, true, err
}

func (h *Handler) s3AlterBucket(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	if len(toks) < 4 {
		return nil, true, mysql.NewError(1064, "ALTER BUCKET <name> (VERSIONING|OBJECT_LOCK) ON|OFF | OWNER '<u>' | LIMIT (CONN_BPS|USER_BPS|USER_CONNS)=<n> | ADD/DROP DOMAIN ...")
	}
	name := toks[2]
	switch strings.ToUpper(toks[3]) {
	case "VERSIONING":
		v := strings.EqualFold(argVal(toks, 4), "ON")
		if _, err := h.s3store.UpdateBucket(name, nil, &v, nil, nil, nil, nil, nil, nil); err != nil {
			return nil, true, s3ErrToMySQL(err)
		}
		logx.Infof("s3: bucket %s versioning=%v (conn %d)", name, v, h.connID)
		return meta.OKResultPub(), true, nil

	case "OBJECT_LOCK":
		v := strings.EqualFold(argVal(toks, 4), "ON")
		if _, err := h.s3store.UpdateBucket(name, nil, nil, &v, nil, nil, nil, nil, nil); err != nil {
			return nil, true, s3ErrToMySQL(err)
		}
		logx.Infof("s3: bucket %s object_lock=%v (conn %d)", name, v, h.connID)
		return meta.OKResultPub(), true, nil

	case "OWNER":
		owner := argVal(toks, 4)
		if h.auths != nil {
			if _, ok, _ := h.auths.Lookup(owner); !ok {
				return nil, true, mysql.NewError(1105, fmt.Sprintf("no such account %q", owner))
			}
		}
		if _, err := h.s3store.UpdateBucket(name, &owner, nil, nil, nil, nil, nil, nil, nil); err != nil {
			return nil, true, s3ErrToMySQL(err)
		}
		return meta.OKResultPub(), true, nil

	case "LIMIT":
		which := strings.ToUpper(argVal(toks, 4))
		valStr := argVal(toks, 5)
		if valStr == "=" {
			valStr = argVal(toks, 6)
		}
		val := int64(0)
		fmt.Sscanf(valStr, "%d", &val)
		switch which {
		case "CONN_BPS":
			if _, err := h.s3store.UpdateBucket(name, nil, nil, nil, nil, nil, &val, nil, nil); err != nil {
				return nil, true, s3ErrToMySQL(err)
			}
		case "USER_BPS":
			if _, err := h.s3store.UpdateBucket(name, nil, nil, nil, nil, nil, nil, &val, nil); err != nil {
				return nil, true, s3ErrToMySQL(err)
			}
		case "USER_CONNS":
			n := int(val)
			if _, err := h.s3store.UpdateBucket(name, nil, nil, nil, nil, nil, nil, nil, &n); err != nil {
				return nil, true, s3ErrToMySQL(err)
			}
		default:
			return nil, true, mysql.NewError(1064, "ALTER BUCKET <name> LIMIT (CONN_BPS|USER_BPS|USER_CONNS) = <n>")
		}
		logx.Infof("s3: bucket %s limit %s=%d (conn %d)", name, which, val, h.connID)
		return meta.OKResultPub(), true, nil

	case "ADD":
		if strings.ToUpper(argVal(toks, 4)) != "DOMAIN" {
			return nil, true, mysql.NewError(1064, "ALTER BUCKET <name> ADD DOMAIN '<host>' (ISSUE | CERT FILE '<pem>' KEY FILE '<key>')")
		}
		domain := argVal(toks, 5)
		issue := false
		certFile, keyFile := "", ""
		for i := 6; i < len(toks); i++ {
			switch strings.ToUpper(toks[i]) {
			case "ISSUE":
				issue = true
			case "CERT":
				i++
				if strings.ToUpper(argVal(toks, i)) == "FILE" {
					i++
				}
				certFile = argVal(toks, i)
			case "KEY":
				i++
				if strings.ToUpper(argVal(toks, i)) == "FILE" {
					i++
				}
				keyFile = argVal(toks, i)
			}
		}
		if !issue && (certFile == "" || keyFile == "") {
			return nil, true, mysql.NewError(1064, "ADD DOMAIN requires ISSUE or CERT FILE '<pem>' KEY FILE '<key>'")
		}
		if err := h.s3certs().AddDomainCert(domain, name, issue, certFile, keyFile); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		if h.s3srv != nil {
			h.s3srv.RefreshCerts()
		}
		logx.Infof("s3: bucket %s domain %s added (issue=%v) by conn %d", name, domain, issue, h.connID)
		return meta.OKResultPub(), true, nil

	case "DROP":
		if strings.ToUpper(argVal(toks, 4)) != "DOMAIN" {
			return nil, true, mysql.NewError(1064, "ALTER BUCKET <name> DROP DOMAIN '<host>'")
		}
		domain := argVal(toks, 5)
		if err := h.s3certs().DropDomainCert(domain); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		if h.s3srv != nil {
			h.s3srv.RefreshCerts()
		}
		return meta.OKResultPub(), true, nil
	}
	return nil, true, mysql.NewError(1064, "unsupported ALTER BUCKET clause")
}

// s3certs 构造证书注册表（域名管理用；与各端点共享 store，刷新走 s3srv.RefreshCerts）。
func (h *Handler) s3certs() *s3.CertCtl {
	return s3.NewCertCtl(h.s3store)
}

// ---------------------------------------------------------------------------
// 密钥（AK/SK）
// ---------------------------------------------------------------------------

func (h *Handler) s3CreateKey(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	owner := h.cfg.S3.DefaultOwner
	var ks s3.KeyScope
	for i := 3; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "FOR":
			i++
			owner = argVal(toks, i)
		case "BUCKETS": // v1.3.14 §2：资源限定（('a','b*') 或 'a'）
			var vals []string
			vals, i = takeSQLList(toks, i+1)
			ks.Buckets = vals
		case "PREFIXES":
			var vals []string
			vals, i = takeSQLList(toks, i+1)
			ks.Prefixes = vals
		case "READONLY":
			ks.Perm = "ro"
		}
	}
	if h.auths != nil {
		if _, ok, _ := h.auths.Lookup(owner); !ok {
			return nil, true, mysql.NewError(1105, fmt.Sprintf("no such account %q", owner))
		}
	}
	ak, sk, err := h.s3store.CreateKeyScoped(owner, ks.MarshalScope())
	if err != nil {
		return nil, true, mysql.NewError(1105, err.Error())
	}
	logx.Infof("s3: access key %s issued for %s (scope=%s) by conn %d", ak, owner, ks.String(), h.connID)
	res, err := meta.SimpleRS([]string{"access_key", "secret_key", "owner", "scope", "note"},
		[][]any{{ak, sk, owner, ks.String(), "SK 仅此一次可见，请立即保存；落表仅存哈希与密文"}})
	return res, true, err
}

// takeSQLList 收集括号列表参数（容忍 ('a', 'b') 内空格：从 i 起拼到闭括号），
// 返回 (值列表, 消费到的末 token 下标)。
func takeSQLList(toks []string, i int) ([]string, int) {
	if i >= len(toks) {
		return nil, i
	}
	joined := toks[i]
	end := i
	if strings.HasPrefix(toks[i], "(") && !strings.HasSuffix(toks[i], ")") {
		for end+1 < len(toks) {
			end++
			joined += "," + toks[end]
			if strings.HasSuffix(toks[end], ")") {
				break
			}
		}
	}
	return parseSQLList(joined), end
}

// parseSQLList ('a','b*') / 'a' / a → []string。
func parseSQLList(v string) []string {
	v = strings.TrimSpace(v)
	v = strings.TrimSuffix(strings.TrimPrefix(v, "("), ")")
	if v == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		p = strings.Trim(strings.TrimSpace(p), "'\"`")
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (h *Handler) s3RenewKey(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	ak := argVal(toks, 3)
	sk, err := h.s3store.RotateKey(ak)
	if err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: access key %s rotated by conn %d", ak, h.connID)
	res, err := meta.SimpleRS([]string{"access_key", "secret_key", "note"},
		[][]any{{ak, sk, "SK 已轮换，仅此一次可见"}})
	return res, true, err
}

func (h *Handler) s3KeyStatus(toks []string, status string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// toks: DROP|DISABLE|ENABLE S3 KEY <ak>
	ak := argVal(toks, 3)
	var err error
	if status == "revoked" {
		err = h.s3store.DropKey(ak)
	} else {
		err = h.s3store.SetKeyStatus(ak, status)
	}
	if err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: access key %s -> %s by conn %d", ak, status, h.connID)
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3ShowKeys(toks []string) (*mysql.Result, bool, error) {
	owner := ""
	for i := 3; i < len(toks); i++ {
		if strings.EqualFold(toks[i], "FOR") {
			i++
			owner = argVal(toks, i)
		}
	}
	var rows [][]any
	for _, k := range h.s3store.Keys(owner) {
		scope := s3.KeyScope{}
		if k.Scope != "" {
			if ks, err := s3.ParseKeyScope(k.Scope); err == nil {
				scope = ks
			}
		}
		rows = append(rows, []any{k.AK, k.Owner, k.Status, s3.KeyFingerprint(k.SKHash),
			scope.String(), truncate(k.Policy, 60), k.CreatedTS, k.LastUsedTS})
	}
	res, err := meta.SimpleRS([]string{"access_key", "owner", "status", "sk_fingerprint", "scope",
		"policy", "created", "last_used"}, rows)
	return res, true, err
}

// ---------------------------------------------------------------------------
// 请求策略 / 回调 / 生命周期 / 域名
// ---------------------------------------------------------------------------

func (h *Handler) s3SetPolicy(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// SET S3 POLICY (KEY <ak>|BUCKET <b>) '<json>'
	kind := strings.ToUpper(argVal(toks, 3))
	target := argVal(toks, 4)
	policy := argVal(toks, 5)
	if target == "" || policy == "" {
		return nil, true, mysql.NewError(1064, `SET S3 POLICY (KEY <ak>|BUCKET <name>) '<json>'`)
	}
	var err error
	switch kind {
	case "KEY":
		err = h.s3store.SetKeyPolicy(target, policy)
	case "BUCKET":
		err = h.s3store.SetBucketPolicy(target, policy)
	default:
		return nil, true, mysql.NewError(1064, "policy target must be KEY or BUCKET")
	}
	if err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3ClearPolicy(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	kind := strings.ToUpper(argVal(toks, 3))
	target := argVal(toks, 4)
	var err error
	switch kind {
	case "KEY":
		err = h.s3store.ClearKeyPolicy(target)
	case "BUCKET":
		err = h.s3store.ClearBucketPolicy(target)
	default:
		return nil, true, mysql.NewError(1064, "policy target must be KEY or BUCKET")
	}
	if err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3ShowPolicies() (*mysql.Result, bool, error) {
	var rows [][]any
	for _, p := range h.s3store.Policies() {
		rows = append(rows, []any{p[0], p[1]})
	}
	res, err := meta.SimpleRS([]string{"target", "policy"}, rows)
	return res, true, err
}

func (h *Handler) s3SetCallback(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// SET S3 CALLBACK <bucket> URL '<url>' [SECRET '<s>']
	bucket := argVal(toks, 3)
	url, secret := "", ""
	for i := 4; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "URL":
			i++
			url = argVal(toks, i)
		case "SECRET":
			i++
			secret = argVal(toks, i)
		}
	}
	if url == "" {
		return nil, true, mysql.NewError(1064, "SET S3 CALLBACK <bucket> URL '<url>' [SECRET '<s>']")
	}
	if _, err := h.s3store.UpdateBucket(bucket, nil, nil, nil, &url, &secret, nil, nil, nil); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3ClearCallback(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	bucket := argVal(toks, 3)
	empty := ""
	if _, err := h.s3store.UpdateBucket(bucket, nil, nil, nil, &empty, &empty, nil, nil, nil); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3SetLifecycle(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// SET S3 LIFECYCLE <bucket> PREFIX '<p>' DAYS <n>
	bucket := argVal(toks, 3)
	prefix, days := "", 0
	for i := 4; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "PREFIX":
			i++
			prefix = argVal(toks, i)
		case "DAYS":
			i++
			fmt.Sscanf(argVal(toks, i), "%d", &days)
		}
	}
	if days <= 0 {
		return nil, true, mysql.NewError(1064, "SET S3 LIFECYCLE <bucket> PREFIX '<p>' DAYS <n>")
	}
	if err := h.s3store.SetLifecycle(bucket, prefix, days); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: lifecycle bucket=%s prefix=%q days=%d (conn %d)", bucket, prefix, days, h.connID)
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3ClearLifecycle(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	bucket := argVal(toks, 3)
	prefix := ""
	for i := 4; i < len(toks); i++ {
		if strings.EqualFold(toks[i], "PREFIX") {
			i++
			prefix = argVal(toks, i)
		}
	}
	if err := h.s3store.ClearLifecycle(bucket, prefix); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3ShowLifecycle(toks []string) (*mysql.Result, bool, error) {
	var rows [][]any
	for _, r := range h.s3store.ListLifecycle(argVal(toks, 3)) {
		rows = append(rows, []any{r.Bucket, r.Prefix, int64(r.Days), b2s(r.Enabled)})
	}
	res, err := meta.SimpleRS([]string{"bucket", "prefix", "days", "enabled"}, rows)
	return res, true, err
}

func (h *Handler) s3ShowDomains() (*mysql.Result, bool, error) {
	var rows [][]any
	for _, d := range h.s3store.Domains() {
		rows = append(rows, []any{d.Host, d.Bucket, d.CertFile, d.KeyFile, d.CreatedTS})
	}
	res, err := meta.SimpleRS([]string{"host", "bucket", "cert_file", "key_file", "created"}, rows)
	return res, true, err
}

// ---------------------------------------------------------------------------
// 用量 / 对象 / 对象锁
// ---------------------------------------------------------------------------

func (h *Handler) s3ShowUsage(toks []string) (*mysql.Result, bool, error) {
	owner := ""
	for i := 3; i < len(toks); i++ {
		if strings.EqualFold(toks[i], "FOR") {
			i++
			owner = argVal(toks, i)
		}
	}
	// SHOW S3 USAGE OPERATORS：操作者维度分摊（v1.3.14 §1——共享桶场景下
	// 各操作者的请求计数；存储字节仍记桶属主，见主视图）
	if len(toks) > 3 && strings.EqualFold(toks[3], "OPERATORS") {
		var rows [][]any
		for _, u := range h.s3store.OpUsage(owner) {
			rows = append(rows, []any{u.Operator, u.Bucket, u.Get, u.Put, u.List, u.Del, u.UpdatedTS})
		}
		res, err := meta.SimpleRS([]string{"operator", "bucket", "get_count", "put_count",
			"list_count", "delete_count", "updated"}, rows)
		return res, true, err
	}
	var rows [][]any
	for _, u := range h.s3store.Usage(owner) {
		rows = append(rows, []any{u.Owner, u.Bucket, u.GetCount, u.PutCount, u.ListCount, u.DeleteCount,
			u.BytesIn, u.BytesOut, u.StorageBytes, u.ObjectCount, u.UpdatedTS})
	}
	res, err := meta.SimpleRS([]string{"owner", "bucket", "get_count", "put_count", "list_count",
		"delete_count", "bytes_in", "bytes_out", "storage_bytes", "object_count", "updated"}, rows)
	return res, true, err
}

func (h *Handler) s3ExportUsage(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	owner, file := "", ""
	for i := 3; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "FOR":
			i++
			owner = argVal(toks, i)
		case "TO":
			i++
			file = argVal(toks, i)
		}
	}
	if file == "" {
		file = "s3_usage_" + timeNowFile() + ".csv"
	}
	// 防路径穿越：仅允许纯文件名，落在 <db_dir>/exports/
	file = filepath.Base(file)
	dir := filepath.Join(h.cfg.DBDir, "exports")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, true, mysql.NewError(1105, err.Error())
	}
	full := filepath.Join(dir, file)
	csv := h.s3store.UsageCSV(owner)
	if err := os.WriteFile(full, []byte(csv), 0o600); err != nil {
		return nil, true, mysql.NewError(1105, err.Error())
	}
	logx.Infof("s3: usage exported to %s by conn %d", full, h.connID)
	res, err := meta.SimpleRS([]string{"file", "bytes", "note"},
		[][]any{{full, int64(len(csv)), "计费系统可直接拉取该 CSV；HTTP 导出接口 GET /_glacier/s3/usage（SigV4 鉴权）"}})
	return res, true, err
}

func (h *Handler) s3ShowObjects(toks []string) (*mysql.Result, bool, error) {
	// SHOW S3 OBJECTS <bucket> [LIKE '<prefix>'] [LIMIT <n>]
	if len(toks) < 4 {
		return nil, true, mysql.NewError(1064, "SHOW S3 OBJECTS <bucket> [LIKE '<prefix>'] [LIMIT <n>]")
	}
	bucket := toks[3]
	prefix := ""
	limit := 100
	for i := 4; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "LIKE":
			i++
			prefix = argVal(toks, i)
		case "LIMIT":
			i++
			fmt.Sscanf(argVal(toks, i), "%d", &limit)
		}
	}
	objs, err := h.s3store.ListObjectsRaw(bucket, prefix, limit)
	if err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	var rows [][]any
	for _, m := range objs {
		rows = append(rows, []any{m.Key, m.VersionID, m.Size, m.ETag, m.ContentType, m.Storage,
			b2s(m.LegalHold), m.RetentionMode, m.RetainUntil, m.CreatedTS})
	}
	res, err2 := meta.SimpleRS([]string{"key", "version_id", "size", "etag", "content_type", "storage",
		"legal_hold", "retention_mode", "retain_until", "created"}, rows)
	return res, true, err2
}

func (h *Handler) s3ShowUploads(toks []string) (*mysql.Result, bool, error) {
	// SHOW S3 UPLOADS [<bucket>]（分片上传会话）
	res, err := meta.SimpleRS([]string{"upload_id", "bucket", "key", "parts", "bytes", "created"},
		h.s3UploadRows(argVal(toks, 3)))
	return res, true, err
}

// s3ShowSpace SHOW S3 SPACE：系统库空间统计（纯 PRAGMA 读，不锁库）。
func (h *Handler) s3ShowSpace() (*mysql.Result, bool, error) {
	st := h.s3store.SpaceStats()
	auto := "none"
	switch st.AutoVacuum {
	case 1:
		auto = "full"
	case 2:
		auto = "incremental"
	}
	note := "incremental_vacuum 可用（S3 VACUUM 走增量免长锁）"
	if st.AutoVacuum != 2 {
		note = "增量模式未启用：执行一次 S3 VACUUM FULL 切换（排他，维护窗口执行）"
	}
	ps := h.s3store.PoolStats()
	res, err := meta.SimpleRS([]string{"page_size", "page_count", "freelist_pages", "free_bytes",
		"file_bytes", "auto_vacuum", "pool_chunks", "pool_live_bytes", "pool_shared",
		"pool_tombstones", "pool_tomb_bytes", "note"},
		[][]any{{st.PageSize, st.PageCount, st.FreelistCount, st.FreeBytes, st.FileBytes, auto,
			ps.Chunks, ps.LiveBytes, ps.SharedRefs, ps.Tombstones, ps.TombBytes, note}})
	return res, true, err
}

// s3ShowSync SHOW S3 SYNC：数据主动同步引擎观测（v1.3.13）。
func (h *Handler) s3ShowSync() (*mysql.Result, bool, error) {
	if h.s3srv == nil {
		return nil, true, mysql.NewError(1105, "S3 module is disabled")
	}
	st := h.s3srv.SyncStatsView()
	if st == nil {
		return nil, true, mysql.NewError(1105, "sync engine not configured (s3.sync rules empty)")
	}
	res, err := meta.SimpleRS([]string{"rules", "queued", "synced", "failed", "skipped", "last_sync_ts", "last_error"},
		[][]any{{st.Rules, st.Queued, st.Synced, st.Failed, st.Skipped, st.LastSyncTS, st.LastErr}})
	return res, true, err
}

// s3Vacuum S3 VACUUM [FULL]：空间回收（SQLite 删除只挂 freelist 不缩文件）。
// 默认增量：每步 256 页的短写事务，WAL 下读不受影响；FULL 排他重建整个库文件
// （时长与库大小成正比），并把存量库切换到增量模式。
func (h *Handler) s3Vacuum(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	full := false
	for i := 2; i < len(toks); i++ {
		if strings.EqualFold(toks[i], "FULL") {
			full = true
		}
	}
	before := h.s3store.SpaceStats()
	if full {
		if n := h.s3store.GCChunkTombstones(); n > 0 { // 全量重建前先清墓碑（v1.3.13）
			logx.Infof("s3: VACUUM FULL pre-GC removed %d chunk tombstones", n)
		}
		if err := h.s3store.VacuumFull(); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		after := h.s3store.SpaceStats()
		logx.Infof("s3: full VACUUM by conn %d (file %d -> %d bytes)", h.connID, before.FileBytes, after.FileBytes)
		res, err := meta.SimpleRS([]string{"mode", "file_bytes_before", "file_bytes_after"},
			[][]any{{"full", before.FileBytes, after.FileBytes}})
		return res, true, err
	}
	freed := h.s3store.VacuumIncremental(0)
	after := h.s3store.SpaceStats()
	logx.Infof("s3: incremental vacuum by conn %d (freed %d bytes, freelist %d -> %d pages)",
		h.connID, freed, before.FreelistCount, after.FreelistCount)
	res, err := meta.SimpleRS([]string{"mode", "freed_bytes", "freelist_pages_before", "freelist_pages_after"},
		[][]any{{"incremental", freed, before.FreelistCount, after.FreelistCount}})
	return res, true, err
}

func (h *Handler) s3Retain(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// S3 RETAIN <bucket>/<key> UNTIL '<date>' (COMPLIANCE|GOVERNANCE) [VERSION '<v>']
	target := argVal(toks, 2)
	bucket, key := splitBucketKey(target)
	until, mode, version := "", "", ""
	for i := 3; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "UNTIL":
			i++
			until = argVal(toks, i)
		case "VERSION":
			i++
			version = argVal(toks, i)
		case "COMPLIANCE", "GOVERNANCE":
			mode = strings.ToUpper(toks[i])
		}
	}
	if bucket == "" || key == "" || until == "" || mode == "" {
		return nil, true, mysql.NewError(1064, "S3 RETAIN <bucket>/<key> UNTIL '<YYYY-MM-DD[ HH:MM:SS]>' (COMPLIANCE|GOVERNANCE) [VERSION '<v>']")
	}
	if len(until) == 10 {
		until += " 00:00:00"
	}
	if err := h.s3store.SetRetain(bucket, key, version, mode, until); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: retain %s/%s until %s mode %s (conn %d)", bucket, key, until, mode, h.connID)
	return meta.OKResultPub(), true, nil
}

func (h *Handler) s3LegalHold(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// S3 LEGAL_HOLD <bucket>/<key> (ON|OFF) [VERSION '<v>']
	target := argVal(toks, 2)
	bucket, key := splitBucketKey(target)
	on := strings.EqualFold(argVal(toks, 3), "ON")
	version := ""
	for i := 4; i < len(toks); i++ {
		if strings.EqualFold(toks[i], "VERSION") {
			i++
			version = argVal(toks, i)
		}
	}
	if bucket == "" || key == "" {
		return nil, true, mysql.NewError(1064, "S3 LEGAL_HOLD <bucket>/<key> (ON|OFF) [VERSION '<v>']")
	}
	if err := h.s3store.SetLegalHold(bucket, key, version, on); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: legal_hold %s/%s = %v (conn %d)", bucket, key, on, h.connID)
	return meta.OKResultPub(), true, nil
}

// splitBucketKey "bucket/key" → (bucket, key)。
func splitBucketKey(s string) (string, string) {
	if i := strings.IndexByte(s, '/'); i > 0 {
		return s[:i], s[i+1:]
	}
	return "", ""
}

// ---------------------------------------------------------------------------
// 行集辅助（SHOW / SELECT * FROM s3_* 共用）
// ---------------------------------------------------------------------------

func (h *Handler) s3UsageRows(owner string) [][]any {
	var rows [][]any
	for _, u := range h.s3store.Usage(owner) {
		rows = append(rows, []any{u.Owner, u.Bucket, u.GetCount, u.PutCount, u.ListCount, u.DeleteCount,
			u.BytesIn, u.BytesOut, u.StorageBytes, u.ObjectCount, u.UpdatedTS})
	}
	return rows
}

func (h *Handler) s3BucketRows() [][]any {
	var rows [][]any
	for _, b := range h.s3store.ListBuckets("") {
		rows = append(rows, []any{b.Name, b.Owner, b.Region, b2s(b.Versioning), b2s(b.ObjectLock),
			b.ObjectCount, b.StorageBytes, b.CallbackURL, b.CreatedTS})
	}
	return rows
}

func (h *Handler) s3KeyRows() [][]any {
	var rows [][]any
	for _, k := range h.s3store.Keys("") {
		rows = append(rows, []any{k.AK, k.Owner, k.Status, s3.KeyFingerprint(k.SKHash),
			truncate(k.Policy, 60), k.CreatedTS, k.LastUsedTS})
	}
	return rows
}

func (h *Handler) s3LifecycleRows() [][]any {
	var rows [][]any
	for _, r := range h.s3store.ListLifecycle("") {
		rows = append(rows, []any{r.Bucket, r.Prefix, int64(r.Days), b2s(r.Enabled)})
	}
	return rows
}

func (h *Handler) s3DomainRows() [][]any {
	var rows [][]any
	for _, d := range h.s3store.Domains() {
		rows = append(rows, []any{d.Host, d.Bucket, d.CertFile, d.KeyFile, d.CreatedTS})
	}
	return rows
}

// s3UploadRows 分片上传会话行（SHOW S3 UPLOADS / s3_uploads 表）。
func (h *Handler) s3UploadRows(bucket string) [][]any {
	var rows [][]any
	for _, u := range h.s3store.ListMultipartUploads(bucket, "") {
		rows = append(rows, []any{u.UploadID, u.Bucket, u.Key, int64(u.Parts), u.Bytes, u.CreatedTS})
	}
	return rows
}

// s3ErrToMySQL s3 错误 → MySQL 协议错误。
func s3ErrToMySQL(err error) error {
	if err == nil {
		return nil
	}
	if code, msg, ok := s3.ErrCode(err); ok {
		switch code {
		case "NoSuchBucket":
			return mysql.NewError(1146, msg)
		case "NoSuchKey", "NoSuchVersion":
			return mysql.NewError(1054, msg)
		case "BucketNotEmpty":
			return mysql.NewError(1051, msg)
		case "AccessDenied":
			return mysql.NewError(1142, msg)
		}
		return mysql.NewError(1105, code+": "+msg)
	}
	return mysql.NewError(1105, err.Error())
}

// timeNowFile 导出文件名时间戳（安全格式）。
func timeNowFile() string {
	return time.Now().Format("20060102_150405")
}

// ---------------------------------------------------------------------------
// 桶共享授权 / AK scope / 魔法 Token（v1.3.14 §1/§2/§4）
// ---------------------------------------------------------------------------

// s3GrantBucket GRANT S3 <bucket> TO <user> [READONLY | READWRITE]
//（默认 READWRITE；仅管理面 super——授权变更挂桶缓存失效体系即时生效）。
func (h *Handler) s3GrantBucket(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// toks: GRANT S3 <bucket> TO <user> [READONLY|READWRITE]
	bucket := argVal(toks, 2)
	user := argVal(toks, 4)
	perm := "rw"
	for i := 5; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "READONLY", "RO":
			perm = "ro"
		case "READWRITE", "RW":
			perm = "rw"
		}
	}
	if bucket == "" || !strings.EqualFold(argVal(toks, 3), "TO") || user == "" {
		return nil, true, mysql.NewError(1064,
			"GRANT S3 <bucket> TO <user> [READONLY | READWRITE]  (不支持转授：被共享方不能再授权第三人)")
	}
	if h.auths != nil {
		if _, ok, _ := h.auths.Lookup(user); !ok {
			return nil, true, mysql.NewError(1105, fmt.Sprintf("no such account %q", user))
		}
	}
	if err := h.s3store.GrantBucket(bucket, user, perm, h.user); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: bucket %s granted %s to %s by %s (conn %d)", bucket, perm, user, h.user, h.connID)
	return meta.OKResultPub(), true, nil
}

// s3RevokeBucket REVOKE S3 <bucket> FROM <user>（级联失效该用户在该桶的魔法 token）。
func (h *Handler) s3RevokeBucket(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// toks: REVOKE S3 <bucket> FROM <user>
	bucket := argVal(toks, 2)
	user := argVal(toks, 4)
	if bucket == "" || !strings.EqualFold(argVal(toks, 3), "FROM") || user == "" {
		return nil, true, mysql.NewError(1064, "REVOKE S3 <bucket> FROM <user>")
	}
	if err := h.s3store.RevokeBucketGrant(bucket, user); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: bucket %s grant revoked from %s by %s (conn %d)", bucket, user, h.user, h.connID)
	return meta.OKResultPub(), true, nil
}

// s3ShowGrants SHOW S3 GRANTS [FOR <user> | FOR BUCKET <bucket>]
func (h *Handler) s3ShowGrants(toks []string) (*mysql.Result, bool, error) {
	user, bucket := "", ""
	for i := 3; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "FOR":
			if strings.EqualFold(argVal(toks, i+1), "BUCKET") {
				bucket = argVal(toks, i+2)
				i += 2
			} else {
				user = argVal(toks, i+1)
				i++
			}
		case "ON":
			bucket = argVal(toks, i+1)
			i++
		}
	}
	var rows [][]any
	for _, g := range h.s3store.BucketGrants(bucket, user) {
		rows = append(rows, []any{g.Bucket, g.User, g.Perm, g.GrantedBy, g.CreatedTS})
	}
	res, err := meta.SimpleRS([]string{"bucket", "username", "perm", "granted_by", "created"}, rows)
	return res, true, err
}

// s3AlterKeyScope ALTER S3 KEY <ak> SET SCOPE [BUCKETS (...)] [PREFIXES (...)] [READONLY|READWRITE|CLEAR]
//（全量替换式；CLEAR = 恢复全量）。
func (h *Handler) s3AlterKeyScope(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// toks: ALTER S3 KEY <ak> SET SCOPE ...
	ak := argVal(toks, 3)
	if ak == "" || !strings.EqualFold(argVal(toks, 4), "SET") || !strings.EqualFold(argVal(toks, 5), "SCOPE") {
		return nil, true, mysql.NewError(1064,
			"ALTER S3 KEY <ak> SET SCOPE [BUCKETS (...)] [PREFIXES (...)] [READONLY|READWRITE|CLEAR]")
	}
	var ks s3.KeyScope
	cleared := false
	for i := 6; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "BUCKETS":
			var vals []string
			vals, i = takeSQLList(toks, i+1)
			ks.Buckets = vals
		case "PREFIXES":
			var vals []string
			vals, i = takeSQLList(toks, i+1)
			ks.Prefixes = vals
		case "READONLY":
			ks.Perm = "ro"
		case "READWRITE":
			ks.Perm = ""
		case "CLEAR":
			ks = s3.KeyScope{}
			cleared = true
		}
	}
	scopeJSON := ks.MarshalScope()
	if cleared {
		scopeJSON = ""
	}
	if err := h.s3store.SetKeyScope(ak, scopeJSON); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	note := ks.String()
	if cleared {
		note = "full (cleared)"
	}
	res, err := meta.SimpleRS([]string{"access_key", "scope"},
		[][]any{{ak, note}})
	return res, true, err
}

// s3CreateToken CREATE S3 TOKEN ON <bucket>/<key> [PUT|GET|GIMG('<params>')]... [EXPIRE <s≤3600>]
// 返回 uuid token（前端 ?token=<uuid> 直访；动作与 gimg 参数签发即钉死）。
func (h *Handler) s3CreateToken(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// toks: CREATE S3 TOKEN ON <bucket>/<key> ...
	target := argVal(toks, 4)
	if target == "" || !strings.EqualFold(argVal(toks, 3), "ON") {
		return nil, true, mysql.NewError(1064,
			`CREATE S3 TOKEN ON <bucket>/<key> [PUT|GET|GIMG('resize=100,0&quality=80')] [EXPIRE <s<=3600>]`)
	}
	bucket, key, ok := strings.Cut(target, "/")
	if !ok || bucket == "" || key == "" {
		return nil, true, mysql.NewError(1064, "token target must be <bucket>/<key>")
	}
	var actions []string
	gimg := ""
	expire := 0
	for i := 5; i < len(toks); i++ {
		t := toks[i]
		up := strings.ToUpper(t)
		switch {
		case up == "EXPIRE" || up == "EXPIRES":
			i++
			if n, err := strconv.Atoi(argVal(toks, i)); err == nil {
				expire = n
			}
		case strings.HasPrefix(up, "GIMG(") && strings.HasSuffix(t, ")"):
			gimg = strings.TrimSuffix(strings.TrimPrefix(t, "GIMG("), ")")
			if gimg == "" {
				gimg = strings.TrimSuffix(strings.TrimPrefix(t, "gimg("), ")")
			}
			actions = append(actions, "GIMG")
		case up == "PUT", up == "GET":
			actions = append(actions, up)
		default:
			return nil, true, mysql.NewError(1064, "unknown token action "+strconv.Quote(t)+" (PUT|GET|GIMG('<params>'))")
		}
	}
	mt, err := h.s3store.CreateMagicToken(bucket, key, strings.Join(actions, ","), gimg, expire, h.user)
	if err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: magic token %s created on %s/%s (actions=%s expire=%s) by %s conn %d",
		mt.Token, bucket, key, mt.Actions, mt.ExpiresTS, h.user, h.connID)
	res, err := meta.SimpleRS([]string{"token", "bucket", "key", "actions", "gimg_params", "expires", "note"},
		[][]any{{mt.Token, mt.Bucket, mt.Key, mt.Actions, mt.GimgParams, mt.ExpiresTS,
			"GET/PUT /<bucket>/<key>?token=<token>；≤1h 强制过期，REVOKE S3 TOKEN 可吊销"}})
	return res, true, err
}

// s3RevokeToken REVOKE S3 TOKEN '<t>'（主动吊销）。
func (h *Handler) s3RevokeToken(toks []string) (*mysql.Result, bool, error) {
	if err := h.adminMutateGate(); err != nil {
		return nil, true, err
	}
	// toks: REVOKE S3 TOKEN <t>
	t := argVal(toks, 3)
	if t == "" {
		return nil, true, mysql.NewError(1064, "REVOKE S3 TOKEN '<token>'")
	}
	if err := h.s3store.RevokeMagicToken(t); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: magic token %s revoked by %s (conn %d)", t, h.user, h.connID)
	return meta.OKResultPub(), true, nil
}

// s3ShowTokens SHOW S3 TOKENS [FOR <user> | ON <bucket>]。
func (h *Handler) s3ShowTokens(toks []string) (*mysql.Result, bool, error) {
	user, bucket := "", ""
	for i := 3; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "FOR":
			user = argVal(toks, i+1)
			i++
		case "ON":
			bucket = argVal(toks, i+1)
			i++
		}
	}
	var rows [][]any
	for _, t := range h.s3store.MagicTokens(user, bucket) {
		expired := ""
		if t.ExpiresTS != "" && time.Now().Format("2006-01-02 15:04:05") > t.ExpiresTS {
			expired = "expired"
		}
		rows = append(rows, []any{t.Token, t.Bucket, t.Key, t.Actions, t.GimgParams,
			t.ExpiresTS, t.CreatedBy, t.CreatedTS, expired})
	}
	res, err := meta.SimpleRS([]string{"token", "bucket", "key", "actions", "gimg_params",
		"expires", "created_by", "created", "state"}, rows)
	return res, true, err
}
