package handler

import (
	"fmt"
	"os"
	"path/filepath"
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
//	S3 RETAIN <bucket>/<key> UNTIL '<date>' (COMPLIANCE|GOVERNANCE) [VERSION '<v>']
//	S3 LEGAL_HOLD <bucket>/<key> (ON|OFF) [VERSION '<v>']
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
		}
	case "ALTER":
		if up(1) == "BUCKET" {
			return h.s3AlterBucket(toks)
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
	}
	return nil, false, nil
}

// isS3Command 判断是否 S3 管理命令（先于分发确认，便于模块关闭时精确报错）。
func isS3Command(up func(int) string) bool {
	switch up(0) {
	case "CREATE", "DROP":
		return up(1) == "BUCKET" || (up(1) == "S3" && up(2) == "KEY")
	case "SHOW":
		return up(1) == "BUCKETS" || (up(1) == "S3" && (up(2) == "KEYS" || up(2) == "USAGE" ||
			up(2) == "LIFECYCLE" || up(2) == "DOMAINS" || up(2) == "POLICIES" || up(2) == "OBJECTS" ||
			up(2) == "UPLOADS"))
	case "ALTER":
		return up(1) == "BUCKET"
	case "SET", "CLEAR":
		return up(1) == "S3" && (up(2) == "CALLBACK" || up(2) == "LIFECYCLE" || up(2) == "POLICY")
	case "RENEW":
		return up(1) == "S3" && up(2) == "KEY"
	case "DISABLE", "ENABLE":
		return up(1) == "S3" && up(2) == "KEY"
	case "EXPORT":
		return up(1) == "S3" && up(2) == "USAGE"
	case "S3":
		return up(1) == "RETAIN" || up(1) == "LEGAL_HOLD"
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
	region := ""
	versioning, objectLock := false, false
	for i := 3; i < len(toks); i++ {
		switch strings.ToUpper(toks[i]) {
		case "OWNER":
			i++
			owner = argVal(toks, i)
		case "REGION":
			i++
			region = argVal(toks, i)
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
	b, err := h.s3store.CreateBucket(name, owner, region, versioning, objectLock)
	if err != nil {
		return nil, true, s3ErrToMySQL(err)
	}
	logx.Infof("s3: bucket %s created by conn %d (owner=%s)", b.Name, h.connID, b.Owner)
	res, err := meta.SimpleRS([]string{"bucket", "owner", "region", "versioning", "object_lock"},
		[][]any{{b.Name, b.Owner, b.Region, b2s(b.Versioning), b2s(b.ObjectLock)}})
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
	var rows [][]any
	for _, b := range h.s3store.ListBuckets("") {
		rows = append(rows, []any{b.Name, b.Owner, b.Region, b2s(b.Versioning), b2s(b.ObjectLock),
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
	for i := 3; i < len(toks); i++ {
		if strings.EqualFold(toks[i], "FOR") {
			i++
			owner = argVal(toks, i)
		}
	}
	if h.auths != nil {
		if _, ok, _ := h.auths.Lookup(owner); !ok {
			return nil, true, mysql.NewError(1105, fmt.Sprintf("no such account %q", owner))
		}
	}
	ak, sk, err := h.s3store.CreateKey(owner)
	if err != nil {
		return nil, true, mysql.NewError(1105, err.Error())
	}
	logx.Infof("s3: access key %s issued for %s by conn %d", ak, owner, h.connID)
	res, err := meta.SimpleRS([]string{"access_key", "secret_key", "owner", "note"},
		[][]any{{ak, sk, owner, "SK 仅此一次可见，请立即保存；落表仅存哈希与密文"}})
	return res, true, err
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
		rows = append(rows, []any{k.AK, k.Owner, k.Status, s3.KeyFingerprint(k.SKHash),
			truncate(k.Policy, 60), k.CreatedTS, k.LastUsedTS})
	}
	res, err := meta.SimpleRS([]string{"access_key", "owner", "status", "sk_fingerprint", "policy",
		"created", "last_used"}, rows)
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
