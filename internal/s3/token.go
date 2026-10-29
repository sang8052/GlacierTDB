// Package s3 — 魔法 Token：前端对象级直访凭证（v1.3.14 §4）。
//
// 定位：把「这次请求要干什么」提前在管理面签发成 128bit uuid 短 token，
// 前端 URL 只带 ?token=<uuid>，不暴露 AK/SK 与 SigV4 签名。三类动作：
//
//	PUT  —— 上传（PUT 对象 / multipart 直传链路）；
//	GET  —— 下载（GET/HEAD，含 Range）；
//	GIMG —— 图片处理（x-gimg 参数在签发时钉死，使用时注入——前端 URL
//	        不再出现处理参数明文）。
//
// 安全语义：token 绑定 bucket+key+动作集（+gimg 参数），签发后不可改写，
// 不允许持 token 反查/扩展到其他对象或动作（杜绝万能钥匙）；过期上限
// 3600s 强制；可主动吊销（REVOKE S3 TOKEN）；对象硬删 / 删桶 / 撤销桶共享
// 时级联失效。使用计入 usage 操作者维度（created_by 归因）。
//
// 与预签名 URL 互补不替代：预签名 = 权限客户端本地计算、签出不可收回；
// 魔法 token = 服务端记账、可吊销、URL 极短、可绑定图片处理参数。
package s3

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// magicTokenExpireMax 过期上限（1h，§4 定稿）。
const magicTokenExpireMax = 3600

// MagicToken token 行。
type MagicToken struct {
	Token      string
	Bucket     string
	Key        string
	Actions    string // 逗号分隔动作集（GET/PUT/GIMG）
	GimgParams string // 钉死的 x-gimg 参数串（k=v&k2=v2，键不带 x-gimg- 前缀）
	ExpiresTS  string
	CreatedBy  string
	CreatedTS  string
}

// CreateMagicToken 签发（管理面 super 调用；动作集非空、EXPIRE ≤ 1h）。
// actions 形如 "GET" / "PUT" / "GET,GIMG"；gimgParams 非空要求 actions 含 GIMG。
func (s *Store) CreateMagicToken(bucket, key, actions, gimgParams string, expireSec int, createdBy string) (MagicToken, error) {
	t := MagicToken{Bucket: bucket, Key: key, CreatedBy: createdBy}
	if _, err := s.Bucket(bucket); err != nil {
		return t, err
	}
	if key == "" {
		return t, fmt.Errorf("magic token requires <bucket>/<key>")
	}
	if expireSec <= 0 {
		expireSec = 600 // 缺省 10 分钟（上限 1h）
	}
	if expireSec > magicTokenExpireMax {
		return t, fmt.Errorf("magic token expire %ds exceeds max %ds", expireSec, magicTokenExpireMax)
	}
	// 动作集规范化（去重、大写、合法值）
	var acts []string
	seen := map[string]bool{}
	for _, a := range strings.Split(strings.ToUpper(actions), ",") {
		a = strings.TrimSpace(a)
		switch a {
		case "GET", "PUT", "GIMG":
			if !seen[a] {
				seen[a] = true
				acts = append(acts, a)
			}
		case "":
		default:
			return t, fmt.Errorf("invalid token action %q (GET|PUT|GIMG)", a)
		}
	}
	if len(acts) == 0 {
		acts = []string{"GET"} // 缺省 GET
	}
	if gimgParams != "" && !seen["GIMG"] {
		return t, fmt.Errorf("GIMG params require GIMG action")
	}
	if gimgParams != "" && !validGimgParams(gimgParams) {
		return t, fmt.Errorf("invalid gimg params %q (expect k=v&k2=v2)", gimgParams)
	}
	t.Token = uuid.NewString()
	t.Actions = strings.Join(acts, ",")
	t.GimgParams = gimgParams
	t.CreatedTS = nowTS()
	t.ExpiresTS = time.Now().Add(time.Duration(expireSec) * time.Second).Format("2006-01-02 15:04:05")

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.db.Exec(`INSERT INTO s3_magic_tokens(token,bucket,key,actions,gimg_params,expires_ts,created_by,created_ts)
		VALUES(?,?,?,?,?,?,?,?)`,
		t.Token, t.Bucket, t.Key, t.Actions, t.GimgParams, t.ExpiresTS, t.CreatedBy, t.CreatedTS); err != nil {
		return MagicToken{}, err
	}
	return t, nil
}

// validGimgParams 校验钉死参数形态：k=v&k2=v2，键 ∈ 允许集（与 x-gimg-* 一致）。
func validGimgParams(s string) bool {
	for _, pair := range strings.Split(s, "&") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok || v == "" {
			return false
		}
		switch k {
		case "crop", "resize", "format", "quality", "ops", "save-as":
		default:
			return false
		}
	}
	return true
}

// GimgQueryParams 钉死参数 → x-gimg-* 查询对（注入请求用）。
func (t MagicToken) GimgQueryParams() [][2]string {
	var out [][2]string
	for _, pair := range strings.Split(t.GimgParams, "&") {
		if k, v, ok := strings.Cut(pair, "="); ok && k != "" {
			out = append(out, [2]string{"x-gimg-" + k, v})
		}
	}
	return out
}

// LookupMagicToken 取 token 行（过期/不存在 → 错误；惰性清理过期行）。
func (s *Store) LookupMagicToken(token, bucket, key string) (MagicToken, error) {
	var t MagicToken
	err := s.db.QueryRow(`SELECT token,bucket,key,actions,gimg_params,expires_ts,created_by,created_ts
		FROM s3_magic_tokens WHERE token=?`, token).
		Scan(&t.Token, &t.Bucket, &t.Key, &t.Actions, &t.GimgParams, &t.ExpiresTS, &t.CreatedBy, &t.CreatedTS)
	if err != nil {
		return t, &s3Error{Code: "InvalidToken", Status: 403, Msg: "The specified token does not exist"}
	}
	if t.Bucket != bucket || t.Key != key {
		return t, &s3Error{Code: "InvalidToken", Status: 403,
			Msg: "Access Denied: token is bound to another object (tokens cannot be reused across objects)"}
	}
	if nowTS() > t.ExpiresTS {
		_ = s.RevokeMagicToken(token)
		return t, &s3Error{Code: "ExpiredToken", Status: 403, Msg: "The specified token has expired"}
	}
	return t, nil
}

// RevokeMagicToken 主动吊销。
func (s *Store) RevokeMagicToken(token string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`DELETE FROM s3_magic_tokens WHERE token=?`, token)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("token '%s' not found", token)
	}
	return nil
}

// dropObjectTokensLocked 对象硬删级联（调用方持 writeMu）。
func (s *Store) dropObjectTokensLocked(bucket, key string) {
	_, _ = s.db.Exec(`DELETE FROM s3_magic_tokens WHERE bucket=? AND key=?`, bucket, key)
}

// MagicTokens token 列表（user / bucket 过滤；含过期未清理行——展示态）。
func (s *Store) MagicTokens(user, bucket string) []MagicToken {
	q := `SELECT token,bucket,key,actions,gimg_params,expires_ts,created_by,created_ts FROM s3_magic_tokens`
	var conds []string
	var args []any
	if user != "" {
		conds = append(conds, "created_by=?")
		args = append(args, user)
	}
	if bucket != "" {
		conds = append(conds, "bucket=?")
		args = append(args, bucket)
	}
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	q += " ORDER BY created_ts DESC"
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []MagicToken
	for rows.Next() {
		var t MagicToken
		if err := rows.Scan(&t.Token, &t.Bucket, &t.Key, &t.Actions, &t.GimgParams, &t.ExpiresTS, &t.CreatedBy, &t.CreatedTS); err == nil {
			out = append(out, t)
		}
	}
	return out
}

// GCMagicTokens 清理过期 token（挂 lifecycleLoop 周期调用）。
func (s *Store) GCMagicTokens() int64 {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`DELETE FROM s3_magic_tokens WHERE expires_ts<>'' AND expires_ts<?`, nowTS())
	if err != nil {
		return 0
	}
	n, _ := res.RowsAffected()
	return n
}
