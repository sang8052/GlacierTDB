package s3

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"

	"fmt"
	"strings"
	"time"
)

// AK 32 位（前缀 AS + 30 位大写字母数字）；SK 48 位（前缀 SK + 46 位字母数字）。
// SK 明文仅签发 / 轮换时返回一次；落表为 SHA-256 指纹 + AES-256-GCM 密文
//（SigV4 校验需 HMAC 派生，密文可还原为明文参与派生）。
const (
	akPrefix  = "AS"
	skPrefix  = "SK"
	akBodyLen = 30
	skBodyLen = 46
)

// KeyRow 密钥行（不含 SK）。
type KeyRow struct {
	AK         string
	Owner      string
	SKHash     string // SHA-256(SK) 指纹（审计/展示）
	Status     string // active | disabled | revoked
	Policy     string // 请求策略条件 JSON
	CreatedTS  string
	UpdatedTS  string
	LastUsedTS string
}

const keyAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const skAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randString(n int, alphabet string) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

// encryptSK AES-256-GCM 加密 SK（返回 base64 密文，nonce 前置）。
func (s *Store) encryptSK(sk string) (string, error) {
	block, err := aes.NewCipher(s.masterKey)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, []byte(sk), nil)), nil
}

// decryptSK 解出 SK 明文。
func (s *Store) decryptSK(enc string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(s.masterKey)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", fmt.Errorf("sk ciphertext too short")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

// CreateKey 签发密钥：返回 (AK, SK 明文)——SK 仅此一次可见。
func (s *Store) CreateKey(owner string) (string, string, error) {
	ak := akPrefix + randString(akBodyLen, keyAlphabet)
	sk := skPrefix + randString(skBodyLen, skAlphabet)
	enc, err := s.encryptSK(sk)
	if err != nil {
		return "", "", err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := nowTS()
	if _, err := s.db.Exec(`INSERT INTO s3_keys(ak,owner,sk_hash,sk_enc,status,created_ts,updated_ts)
		VALUES(?,?,?,?,'active',?,?)`, ak, owner, hexSHA256([]byte(sk)), enc, now, now); err != nil {
		return "", "", err
	}
	return ak, sk, nil
}

// RotateKey 轮换 SK（返回新 SK 明文，仅此一次可见）。
func (s *Store) RotateKey(ak string) (string, error) {
	sk := skPrefix + randString(skBodyLen, skAlphabet)
	enc, err := s.encryptSK(sk)
	if err != nil {
		return "", err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`UPDATE s3_keys SET sk_hash=?, sk_enc=?, updated_ts=? WHERE ak=?`,
		hexSHA256([]byte(sk)), enc, nowTS(), ak)
	if err != nil {
		return "", err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return "", &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified access key does not exist"}
	}
	return sk, nil
}

// SetKeyStatus 禁用 / 启用 / 吊销密钥。
func (s *Store) SetKeyStatus(ak, status string) error {
	switch status {
	case "active", "disabled", "revoked":
	default:
		return fmt.Errorf("invalid key status %q (active|disabled|revoked)", status)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`UPDATE s3_keys SET status=?, updated_ts=? WHERE ak=?`, status, nowTS(), ak)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified access key does not exist"}
	}
	return nil
}

// DropKey 吊销并删除密钥。
func (s *Store) DropKey(ak string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`DELETE FROM s3_keys WHERE ak=?`, ak)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified access key does not exist"}
	}
	return nil
}

// TouchKey 标记密钥最近使用（v1.3.12：内存脏标记 + 定时批量落库——此前每个
// S3 请求一次 UPDATE 写事务，是并行上传路径上与对象写竞争的固定开销；
// last_used_ts 仅为展示元数据，30 秒级延迟无碍）。
func (s *Store) TouchKey(ak string) {
	s.touchMu.Lock()
	s.touchBuf[ak] = struct{}{}
	s.touchMu.Unlock()
}

// flushTouchKeys 批量落库脏标记的 last_used_ts。
func (s *Store) flushTouchKeys() {
	s.touchMu.Lock()
	if len(s.touchBuf) == 0 {
		s.touchMu.Unlock()
		return
	}
	buf := s.touchBuf
	s.touchBuf = make(map[string]struct{})
	s.touchMu.Unlock()
	now := nowTS()
	for ak := range buf {
		_, _ = s.db.Exec(`UPDATE s3_keys SET last_used_ts=? WHERE ak=?`, now, ak)
	}
}

// KeyFor SigV4 查询回调：返回 (owner, SK 明文, status, policy, 存在)。
func (s *Store) KeyFor(ak string) (string, string, string, string, bool) {
	var owner, skHash, enc, status, policy string
	err := s.db.QueryRow(`SELECT owner,sk_hash,sk_enc,status,policy FROM s3_keys WHERE ak=?`, ak).
		Scan(&owner, &skHash, &enc, &status, &policy)
	if err != nil {
		return "", "", "", "", false
	}
	sk, err := s.decryptSK(enc)
	if err != nil {
		return "", "", "", "", false
	}
	return owner, sk, status, policy, true
}

// KeyRowFor 查询密钥行（不含 SK）。
func (s *Store) KeyRowFor(ak string) (KeyRow, bool) {
	var k KeyRow
	err := s.db.QueryRow(`SELECT ak,owner,sk_hash,status,policy,created_ts,updated_ts,last_used_ts
		FROM s3_keys WHERE ak=?`, ak).
		Scan(&k.AK, &k.Owner, &k.SKHash, &k.Status, &k.Policy, &k.CreatedTS, &k.UpdatedTS, &k.LastUsedTS)
	if err != nil {
		return k, false
	}
	return k, true
}

// Keys 密钥列表（owner 为空 = 全部）。
func (s *Store) Keys(owner string) []KeyRow {
	q := `SELECT ak,owner,sk_hash,status,policy,created_ts,updated_ts,last_used_ts FROM s3_keys`
	var args []any
	if owner != "" {
		q += ` WHERE owner=?`
		args = append(args, owner)
	}
	q += ` ORDER BY owner,ak`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []KeyRow
	for rows.Next() {
		var k KeyRow
		if err := rows.Scan(&k.AK, &k.Owner, &k.SKHash, &k.Status, &k.Policy, &k.CreatedTS, &k.UpdatedTS, &k.LastUsedTS); err == nil {
			out = append(out, k)
		}
	}
	return out
}

// SetKeyPolicy 设置密钥请求策略（条件 JSON）。
func (s *Store) SetKeyPolicy(ak, policy string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`UPDATE s3_keys SET policy=?, updated_ts=? WHERE ak=?`, policy, nowTS(), ak)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified access key does not exist"}
	}
	return nil
}

// SetBucketPolicy 设置桶请求策略（s3_policies 表，scope = bucket:<name>）。
func (s *Store) SetBucketPolicy(bucket, policy string) error {
	if _, err := s.Bucket(bucket); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.invalidatePolicy(bucket)
	_, err := s.db.Exec(`INSERT INTO s3_policies(scope,policy) VALUES(?,?)
		ON CONFLICT(scope) DO UPDATE SET policy=excluded.policy`, "bucket:"+bucket, policy)
	return err
}

// ClearPolicy 清除策略（scope = KEY <ak> 或 BUCKET <name> 的规范化形式）。
func (s *Store) ClearKeyPolicy(ak string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.Exec(`UPDATE s3_keys SET policy='' WHERE ak=?`, ak)
	return err
}

// ClearBucketPolicy 清除桶策略。
func (s *Store) ClearBucketPolicy(bucket string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.invalidatePolicy(bucket)
	_, err := s.db.Exec(`DELETE FROM s3_policies WHERE scope=?`, "bucket:"+bucket)
	return err
}

// BucketPolicy 取桶请求策略（v1.3.12：带 TTL 缓存，策略写路径主动失效）。
func (s *Store) BucketPolicy(bucket string) string {
	now := time.Now()
	s.cacheMu.Lock()
	if e, ok := s.policyCache[bucket]; ok && now.Before(e.expires) {
		s.cacheMu.Unlock()
		return e.val
	}
	s.cacheMu.Unlock()
	var p string
	if err := s.db.QueryRow(`SELECT policy FROM s3_policies WHERE scope=?`, "bucket:"+bucket).Scan(&p); err != nil {
		p = ""
	}
	s.cacheMu.Lock()
	s.policyCache[bucket] = policyCacheEntry{val: p, expires: now.Add(bucketCacheTTL)}
	s.cacheMu.Unlock()
	return p
}

// invalidatePolicy 桶策略缓存失效。
func (s *Store) invalidatePolicy(bucket string) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	delete(s.policyCache, bucket)
}

// Policies 全部请求策略（密钥 + 桶）。
func (s *Store) Policies() [][2]string {
	var out [][2]string
	for _, k := range s.Keys("") {
		if strings.TrimSpace(k.Policy) != "" {
			out = append(out, [2]string{"KEY " + k.AK, k.Policy})
		}
	}
	rows, err := s.db.Query(`SELECT scope, policy FROM s3_policies ORDER BY scope`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var scope, p string
		if err := rows.Scan(&scope, &p); err == nil {
			out = append(out, [2]string{strings.TrimPrefix(scope, "bucket:"), p})
		}
	}
	return out
}

// KeyFingerprint SK 指纹展示（前 8 位）。
func KeyFingerprint(skHash string) string {
	if len(skHash) > 8 {
		return skHash[:8]
	}
	return skHash
}
