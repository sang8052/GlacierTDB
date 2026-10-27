// Package handler — S3 管理面异步任务提交（SUBMIT S3 ...，v1.3.11）。
//
//	SUBMIT S3 TEXT SEARCH <bucket> MATCH '<text>'
//	  [PREFIX '<p>'] [LIKE '<pat>'] [SIZE (>|>=|=|<|<=) n | SIZE BETWEEN a AND b]
//	  [MODIFIED (BEFORE|AFTER) '<ts>'] [CREATED (BEFORE|AFTER) '<ts>'] [LIMIT n]
//	  [CALLBACK url [SIGN s]] [TIMEOUT s] [AT ...] [WINDOW ...] [WHEN ...] [EVERY ...] [RETAIN ...]
//	SUBMIT S3 FILE SEARCH <bucket> [同上过滤（无 MATCH）] [LIMIT n] [通用选项]
//	SUBMIT S3 DELETE <bucket> [同上过滤] [DRY RUN] [LIMIT n] [通用选项]
//
// 过滤语义：SIZE 按 latest 版本大小；MODIFIED = 最新版本写入时间；
// CREATED = 全版本最早写入时间（同表对象首次出现）。任务经 asyncq（Kind="s3"）
// 执行，FETCH QUERY RESULT / 回调 / EVERY 周期链与 SQL 异步任务一致。
// 提交要求 super 账号（检索/删除横贯整桶对象，属管理面能力）。
package handler

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/asyncq"
	"glaciertdb/internal/meta"
	"glaciertdb/internal/s3"
)

var reSubmitS3 = regexp.MustCompile(`(?is)^SUBMIT\s+S3\s+(TEXT\s+SEARCH|FILE\s+SEARCH|DELETE)\s+(.*)$`)

// genericOptStart 通用 SUBMIT 选项关键词（出现即视为 S3 过滤段结束）。
var genericOptStart = map[string]bool{
	"CALLBACK": true, "SIGN": true, "TIMEOUT": true, "AT": true, "WINDOW": true,
	"WHEN": true, "EVERY": true, "PARTITION": true, "SCRIPT": true, "RETAIN": true,
	"OUTPUT": true, "TOPIC": true, "QUEUE": true,
}

// tok 带原始偏移的词法单元（引号串保持单 token，记住原文区间以便切出通用选项尾段）。
type tok struct {
	val    string
	quoted bool
	off    int
}

// scanS3Tokens 引号感知切词（单引号内 '' 转义；记录字节偏移）。
func scanS3Tokens(s string) []tok {
	var out []tok
	i := 0
	for i < len(s) {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		start := i
		if c == '\'' {
			var sb strings.Builder
			j := i + 1
			for j < len(s) {
				if s[j] == '\'' {
					if j+1 < len(s) && s[j+1] == '\'' {
						sb.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				sb.WriteByte(s[j])
				j++
			}
			out = append(out, tok{val: sb.String(), quoted: true, off: start})
			i = j + 1
			continue
		}
		j := i
		for j < len(s) && s[j] != ' ' && s[j] != '\t' && s[j] != '\n' && s[j] != '\r' {
			j++
		}
		out = append(out, tok{val: s[i:j], off: start})
		i = j
	}
	return out
}

// handleSubmitS3 SUBMIT S3 ... 解析与提交；handled=false 表示非本类命令。
func (h *Handler) handleSubmitS3(q string) (*mysql.Result, bool, error) {
	m := reSubmitS3.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	if h.async == nil {
		return nil, true, mysql.NewError(1235, "async query is disabled")
	}
	if h.s3store == nil {
		return nil, true, mysql.NewError(1105, "S3 object storage module is disabled (set s3.enable: true)")
	}
	if err := h.adminMutateGate(); err != nil { // 只读拒绝 + super 要求
		return nil, true, err
	}
	action := ""
	switch strings.ToUpper(m[1]) {
	case "TEXT SEARCH":
		action = "text"
	case "FILE SEARCH":
		action = "file"
	default:
		action = "delete"
	}
	rest := m[2]
	toks := scanS3Tokens(rest)
	if len(toks) == 0 || toks[0].val == "" {
		return nil, true, mysql.NewError(1064, "SUBMIT S3 "+strings.ToUpper(action)+" requires <bucket>")
	}
	task := s3.SearchTask{Action: action, Bucket: toks[0].val}

	genericOff := len(rest)
	parseErr := ""
	i := 1
	nextTok := func() (tok, bool) {
		if i < len(toks) {
			t := toks[i]
			i++
			return t, true
		}
		return tok{}, false
	}
	for i < len(toks) {
		t := toks[i]
		i++
		if !t.quoted && genericOptStart[strings.ToUpper(t.val)] {
			genericOff = t.off
			i = len(toks) // 尾段交给通用解析
			break
		}
		up := strings.ToUpper(t.val)
		switch up {
		case "MATCH":
			v, ok := nextTok()
			if !ok || v.val == "" {
				parseErr = "MATCH expects a quoted string"
			} else {
				task.Match = v.val
			}
		case "PREFIX":
			v, ok := nextTok()
			if !ok {
				parseErr = "PREFIX expects a value"
			} else {
				task.Prefix = v.val
			}
		case "LIKE":
			v, ok := nextTok()
			if !ok {
				parseErr = "LIKE expects a pattern"
			} else {
				task.Like = v.val
			}
		case "SIZE":
			op, ok1 := nextTok()
			if !ok1 {
				parseErr = "SIZE expects an operator (>|>=|=|<|<=|BETWEEN)"
				break
			}
			if strings.EqualFold(op.val, "BETWEEN") {
				lo, ok2 := nextTok()
				_, ok3 := nextTok() // AND
				hi, ok4 := nextTok()
				if !ok2 || !ok3 || !ok4 {
					parseErr = "SIZE BETWEEN expects 'a AND b'"
					break
				}
				if a, e := strconv.ParseInt(lo.val, 10, 64); e == nil {
					task.SizeMin = a
				} else {
					parseErr = "SIZE BETWEEN expects integers"
				}
				if b, e := strconv.ParseInt(hi.val, 10, 64); e == nil {
					task.SizeMax = b
				} else {
					parseErr = "SIZE BETWEEN expects integers"
				}
				break
			}
			v, ok2 := nextTok()
			if !ok2 {
				parseErr = "SIZE expects a number"
				break
			}
			n, e := strconv.ParseInt(v.val, 10, 64)
			if e != nil {
				parseErr = "SIZE expects an integer"
				break
			}
			switch strings.TrimSpace(op.val) {
			case ">":
				task.SizeMin = n + 1
			case ">=":
				task.SizeMin = n
			case "=":
				task.SizeMin, task.SizeMax = n, n
			case "<":
				task.SizeMax = n - 1
			case "<=":
				task.SizeMax = n
			default:
				parseErr = "SIZE operator must be one of > >= = < <= BETWEEN"
			}
		case "MODIFIED", "CREATED":
			dir, ok1 := nextTok()
			ts, ok2 := nextTok()
			if !ok1 || !ok2 || (!strings.EqualFold(dir.val, "BEFORE") && !strings.EqualFold(dir.val, "AFTER")) {
				parseErr = up + " expects BEFORE|AFTER '<ts>'"
				break
			}
			if _, e := time.Parse("2006-01-02", ts.val); e != nil {
				if _, e2 := time.Parse("2006-01-02 15:04:05", ts.val); e2 != nil {
					parseErr = up + " timestamp must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'"
					break
				}
			}
			if strings.EqualFold(dir.val, "BEFORE") {
				if up == "MODIFIED" {
					task.ModBefore = ts.val
				} else {
					task.CreBefore = ts.val
				}
			} else {
				if up == "MODIFIED" {
					task.ModAfter = ts.val
				} else {
					task.CreAfter = ts.val
				}
			}
		case "LIMIT":
			v, ok := nextTok()
			n, e := strconv.Atoi(v.val)
			if !ok || e != nil || n <= 0 {
				parseErr = "LIMIT expects a positive integer"
			} else {
				task.Limit = n
			}
		case "DRY":
			v, ok := nextTok()
			if !ok || !strings.EqualFold(v.val, "RUN") {
				parseErr = "DRY must be followed by RUN"
			} else {
				task.DryRun = true
			}
		default:
			parseErr = "unknown SUBMIT S3 option near " + strconv.Quote(t.val)
		}
		if parseErr != "" {
			break
		}
	}
	if parseErr != "" {
		return nil, true, mysql.NewError(1064, parseErr)
	}
	if action == "text" && strings.TrimSpace(task.Match) == "" {
		return nil, true, mysql.NewError(1064, "TEXT SEARCH requires MATCH '<text>'")
	}
	if _, err := h.s3store.Bucket(task.Bucket); err != nil {
		return nil, true, s3ErrToMySQL(err)
	}

	payload, _ := json.Marshal(task)
	job := &asyncq.Job{Kind: "s3", Payload: string(payload), SQL: q}
	cb, cbOpt, sign, timeout, at, win, weekWd, when, part, partBytes, script, scriptOut,
		retainSec, everySec, everyAt, everyWd, everyWeeks, everyMonths, everyMonthDay,
		everyYears, everyYearDay, perr := parseSubmitOpts(rest[genericOff:])
	if perr != nil {
		return nil, true, mysql.NewError(1064, perr.Error())
	}
	if part != "" || partBytes != "" || script != "" || scriptOut != "" {
		return nil, true, mysql.NewError(1064, "PARTITION/SCRIPT are SQL-task options (not supported by SUBMIT S3)")
	}
	job.CallbackURL, job.SignSecret, job.TimeoutSec = cb, sign, timeout
	job.At, job.Window, job.WindowWd, job.WhenExpr = at, win, weekWd, when
	job.RetainSec, job.EverySec, job.EveryAt = retainSec, everySec, everyAt
	job.EveryWd, job.EveryWeeks = everyWd, everyWeeks
	job.EveryMonths, job.EveryMonthDay = everyMonths, everyMonthDay
	job.EveryYears, job.EveryYearDay = everyYears, everyYearDay
	lcb := strings.ToLower(cb)
	switch {
	case cb == "":
	case strings.HasPrefix(lcb, "tcp://") || strings.HasPrefix(lcb, "tcp:"):
		job.CallbackProto = "tcp"
		job.CallbackURL = strings.TrimPrefix(strings.TrimPrefix(cb, "tcp://"), "tcp:")
	case strings.HasPrefix(lcb, "amqp://"):
		job.CallbackProto = "amqp"
	case strings.HasPrefix(lcb, "kafka://"):
		job.CallbackProto = "kafka"
		job.CallbackURL = strings.TrimPrefix(cb, "kafka://")
	default:
		job.CallbackProto = "http"
	}
	job.CallbackOpt = cbOpt
	job.Owner = h.user
	id := h.async.Submit(job)
	res, err := meta.SimpleRS([]string{"job_id", "state", "kind", "bucket"},
		[][]any{{id, string(asyncq.StatePending), "s3", task.Bucket}})
	return res, true, err
}
