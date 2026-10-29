// Package s3 — 管理面异步检索/删除任务执行（SUBMIT S3 ...，v1.3.11）。
//
// 任务由 handler 解析为 SearchTask JSON 提交进 asyncq（Kind="s3"），
// 执行器在此实现：文本搜索（内容级）/ 文件检索（元数据级）/ 异步删除。
// 提交入口已要求 super 账号；执行走 Store 直连，结果与 SQL 异步任务同构
// （列 + 行），复用落盘 / FETCH / 回调 / 周期链管线。
package s3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// textScanMaxBytes 文本搜索单对象扫描上限（超过按非文本跳过，防止超大二进制误扫）。
const textScanMaxBytes = 64 << 20

// SearchTask SUBMIT S3 任务载荷。
type SearchTask struct {
	Action string `json:"action"` // text | file | delete | copy（v1.3.14 §3）
	Bucket string `json:"bucket"`
	Match  string `json:"match,omitempty"`  // text：大小写不敏感包含匹配
	DryRun bool   `json:"dry_run,omitempty"` // delete/copy：只报告不执行
	DstBucket  string `json:"dst_bucket,omitempty"`  // copy：目标桶
	DstPrefix  string `json:"dst_prefix,omitempty"`  // copy：目标前缀（空 = 保持原 key）
	SearchFilter
}

// RunSearchTask 执行检索/删除任务，返回列头与行（asyncq runner 协议）。
func (s *Store) RunSearchTask(payload string) ([]string, [][]any, error) {
	var t SearchTask
	if err := json.Unmarshal([]byte(payload), &t); err != nil {
		return nil, nil, fmt.Errorf("bad s3 task payload: %w", err)
	}
	if t.Action == "" || t.Bucket == "" {
		return nil, nil, fmt.Errorf("s3 task requires action and bucket")
	}
	cands, err := s.SearchObjects(t.Bucket, t.SearchFilter)
	if err != nil {
		return nil, nil, err
	}
	switch t.Action {
	case "file":
		cols := []string{"bucket", "key", "size", "content_type", "etag", "modified", "created"}
		var rows [][]any
		for _, c := range cands {
			rows = append(rows, []any{t.Bucket, c.Key, c.Size, c.ContentType, c.ETag, c.ModifiedTS, c.CreatedTS})
		}
		return cols, rows, nil
	case "text":
		if strings.TrimSpace(t.Match) == "" {
			return nil, nil, fmt.Errorf("text search requires MATCH '...'")
		}
		return s.runTextSearch(t, cands)
	case "delete":
		return s.runDelete(t, cands)
	case "copy":
		return s.runCopy(t, cands)
	}
	return nil, nil, fmt.Errorf("unknown s3 task action %q (text|file|delete|copy)", t.Action)
}

// runTextSearch 文本搜索：候选对象逐个判定文本性后按行做大小写不敏感包含匹配。
func (s *Store) runTextSearch(t SearchTask, cands []SearchCandidate) ([]string, [][]any, error) {
	cols := []string{"bucket", "key", "size", "line_no", "line_text"}
	var rows [][]any
	needle := strings.ToLower(t.Match)
	for _, c := range cands {
		if c.Size <= 0 || c.Size > textScanMaxBytes {
			continue
		}
		m, rc, err := s.GetObject(t.Bucket, c.Key, "")
		if err != nil {
			continue
		}
		_ = m
		data := make([]byte, c.Size)
		if _, err := io.ReadFull(rc, data); err != nil {
			rc.Close()
			continue
		}
		rc.Close()
		if !looksTexty(c.ContentType, data) {
			continue
		}
		for ln, line := range bytes.Split(data, []byte{'\n'}) {
			if bytes.Contains(bytes.ToLower(line), []byte(needle)) {
				rows = append(rows, []any{t.Bucket, c.Key, c.Size, int64(ln + 1), snippet(line)})
			}
		}
	}
	return cols, rows, nil
}

// runDelete 异步删除：逐对象硬删（走 DeleteObject，尊重对象锁 / WORM）。
func (s *Store) runDelete(t SearchTask, cands []SearchCandidate) ([]string, [][]any, error) {
	cols := []string{"bucket", "key", "size", "result"}
	var rows [][]any
	for _, c := range cands {
		if t.DryRun {
			rows = append(rows, []any{t.Bucket, c.Key, c.Size, "dry-run"})
			continue
		}
		_, err := s.DeleteObject(t.Bucket, c.Key, "", false)
		st := "deleted"
		if err != nil {
			if code, _, ok := ErrCode(err); ok && code == "AccessDenied" {
				st = "locked" // 对象锁 / legal hold 拒绝
			} else {
				st = "error: " + err.Error()
			}
		}
		rows = append(rows, []any{t.Bucket, c.Key, c.Size, st})
	}
	return cols, rows, nil
}

// runCopy 异步批量复制（SUBMIT S3 COPY，v1.3.14 §3）：逐对象走 CopyObject
//（chunk 池零拷贝——大数据量批量迁移 = 元数据级操作）；目标 key = DstPrefix +
// 剥离源前缀后的相对路径（源未给前缀则原 key 拼接）。DRY RUN 只列不拷。
func (s *Store) runCopy(t SearchTask, cands []SearchCandidate) ([]string, [][]any, error) {
	if t.DstBucket == "" {
		return nil, nil, fmt.Errorf("copy task requires TO <dst-bucket>")
	}
	cols := []string{"src_bucket", "key", "dst_bucket", "dst_key", "size", "result"}
	var rows [][]any
	for _, c := range cands {
		dstKey := c.Key
		if t.Prefix != "" {
			dstKey = strings.TrimPrefix(c.Key, t.Prefix)
		}
		if t.DstPrefix != "" {
			dstKey = t.DstPrefix + dstKey
		}
		if t.DryRun {
			rows = append(rows, []any{t.Bucket, c.Key, t.DstBucket, dstKey, c.Size, "dry-run"})
			continue
		}
		_, err := s.CopyObject(t.Bucket, c.Key, "", t.DstBucket, dstKey, ObjectAttrs{Metadata: map[string]string{}})
		st := "copied"
		if err != nil {
			st = "error: " + err.Error()
		}
		rows = append(rows, []any{t.Bucket, c.Key, t.DstBucket, dstKey, c.Size, st})
	}
	return cols, rows, nil
}

// looksTexty 文本性判定：content_type 语义 + 头部字节嗅探（可打印比例）。
func looksTexty(contentType string, head []byte) bool {
	ct := strings.ToLower(contentType)
	if strings.HasPrefix(ct, "text/") {
		return true
	}
	for _, sub := range []string{"json", "xml", "csv", "javascript", "yaml", "sql", "plain", "markdown"} {
		if strings.Contains(ct, sub) {
			return true
		}
	}
	h := head
	if len(h) > 512 {
		h = h[:512]
	}
	if len(h) == 0 {
		return false
	}
	if !utf8.Valid(h) {
		return false
	}
	printable := 0
	for _, b := range h {
		if b == '\t' || b == '\n' || b == '\r' || (b >= 0x20 && b != 0x7f) {
			printable++
		}
	}
	return printable*100/len(h) >= 85
}

// snippet 行文本截断（≤200 rune，控制结果行宽）。
func snippet(line []byte) string {
	s := strings.TrimSpace(string(line))
	r := []rune(s)
	if len(r) > 200 {
		r = r[:200]
	}
	return string(r)
}
