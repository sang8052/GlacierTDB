// Package handler — 异步查询命令（v1.0.5）：SUBMIT / SHOW QUERY JOBS / query_jobs /
// query_result / FETCH / CANCEL / DROP。
package handler

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/asyncq"
	"glaciertdb/internal/meta"
)

var (
	reSubmit     = regexp.MustCompile(`(?is)^SUBMIT\s+QUERY\s+'((?:[^']|'')*)'\s*(.*)$`)
	reFetchRes   = regexp.MustCompile(`(?is)^FETCH\s+QUERY\s+RESULT\s+'([^']+)'\s*(?:LIMIT\s+(\d+))?\s*(?:OFFSET\s+(\d+))?\s*$`)
	reQResultFn  = regexp.MustCompile(`(?is)^SELECT\s+\*\s+FROM\s+query_result\s*\(\s*'([^']+)'\s*\)\s*(?:LIMIT\s+(\d+))?\s*(?:OFFSET\s+(\d+))?\s*$`)
	reCancelJob  = regexp.MustCompile(`(?i)^CANCEL\s+QUERY\s+'?([A-Za-z0-9_]+)'?\s*$`)
	reDropJob    = regexp.MustCompile(`(?i)^DROP\s+QUERY\s+RESULT\s+'?([A-Za-z0-9_]+)'?\s*$`)
	reShowJobs   = regexp.MustCompile(`(?i)^SHOW\s+QUERY\s+JOBS\s*$`)
	reJobsTable  = regexp.MustCompile(`(?is)^SELECT\s+(.+?)\s+FROM\s+query_jobs(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+.+?)?(?:\s+LIMIT\s+\d+(?:\s*,\s*\d+)?)?\s*$`)
)

// parseSubmitOpts 解析 SUBMIT 之后的选项段。
func parseSubmitOpts(rest string) (cb, cbOpt, sign string, timeout int, at *time.Time, win [2]string, when, part, script, scriptOut string, retainSec, everySec int, everyAt string, err error) {
	s := strings.TrimSpace(rest)
	// WHEN 到 PARTITION/SCRIPT 前的整段
	var whenRaw string
	if i := regexp.MustCompile(`(?i)\s+(PARTITION|SCRIPT)\b`).FindStringIndex(s); i != nil {
		if w := regexp.MustCompile(`(?i)\s+WHEN\s+(.*)$`).FindStringSubmatch(s); w != nil {
			whenRaw = strings.TrimSpace(w[1])
			s = strings.TrimSpace(s[:strings.Index(strings.ToUpper(s), "WHEN")])
		}
	} else if w := regexp.MustCompile(`(?i)\s+WHEN\s+(.+)$`).FindStringSubmatch(s); w != nil {
		whenRaw = strings.TrimSpace(w[1])
		s = strings.TrimSpace(s[:len(s)-len(w[0])])
	}
	when = whenRaw

	toks := regexp.MustCompile(`(?i)'(?:[^']*)'|\S+`).FindAllString(s, -1)
	for i := 0; i < len(toks); i++ {
		up := strings.ToUpper(toks[i])
		next := func() string {
			if i+1 < len(toks) {
				return toks[i+1]
			}
			return ""
		}
		switch up {
		case "CALLBACK":
			v := strings.Trim(next(), "'")
			cb = v
			switch {
			case strings.HasPrefix(strings.ToLower(v), "tcp:"):
				cb = v
			case strings.HasPrefix(strings.ToLower(v), "tcp://"):
				cb = v
			}
			// TCP 形式：CALLBACK TCP host:port
			if strings.ToUpper(v) == "TCP" {
				cb = "tcp://" + next()
				i++
			}
			i++
		case "SIGN":
			sign = strings.Trim(next(), "'")
			i++
		case "TIMEOUT":
			timeout, _ = strconv.Atoi(next())
			i++
		case "AT":
			if t, e := parseAtTime(strings.Trim(next(), "'")); e == nil {
				at = &t
			} else {
				err = fmt.Errorf("invalid AT time: %v", e)
				return
			}
			i++
		case "WINDOW":
			w := strings.Trim(next(), "'")
			parts := strings.SplitN(w, "-", 2)
			if len(parts) != 2 {
				err = fmt.Errorf("invalid WINDOW '%s' (expect 'HH:MM-HH:MM')", w)
				return
			}
			win = [2]string{strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])}
			i++
		case "PARTITION":
			// PARTITION BY ROWS n / BYTES n
			if strings.ToUpper(next()) == "BY" {
				i++
			}
			if i+1 < len(toks) && strings.ToUpper(next()) == "ROWS" {
				i++
			}
			part = next()
			i++
		case "BY", "ROWS", "BYTES":
			// 已由 PARTITION 消费
		case "TOPIC", "QUEUE":
			cbOpt = next()
			i++
		case "EVERY":
			v := strings.ToUpper(next())
			i++
			switch v {
			case "DAY":
				everySec = 86400
				// EVERY DAY AT 'HH:MM'
				if i+1 < len(toks) && strings.ToUpper(toks[i+1]) == "AT" {
					everyAt = strings.Trim(toks[i+2], "'")
					i += 2
				}
			case "HOUR":
				everySec = 3600
			case "MINUTE":
				everySec = 60
			case "SECOND":
				everySec = 1
			default:
				var n int
				if _, e := fmt.Sscanf(v, "%d", &n); e != nil || n <= 0 {
					err = fmt.Errorf("invalid EVERY %q (expect DAY [AT 'HH:MM'] / HOUR / MINUTE / SECOND / n SECONDS|MINUTES|HOURS)", v)
					return
				}
				u := strings.ToUpper(next())
				i++
				switch u {
				case "SECOND", "SECONDS":
					everySec = n
				case "MINUTE", "MINUTES":
					everySec = n * 60
				case "HOUR", "HOURS":
					everySec = n * 3600
				default:
					err = fmt.Errorf("invalid EVERY unit %q", u)
					return
				}
			}
		case "RETAIN":
			d := next()
			var n int
			if _, e := fmt.Sscanf(d, "%d", &n); e != nil || n <= 0 {
				err = fmt.Errorf("invalid RETAIN %q (expect e.g. 30 / 30s / 5m / 2h)", d)
				return
			}
			unit := strings.ToLower(strings.TrimLeft(fmt.Sprintf("%d", n), "0123456789"))
			dl := strings.ToLower(d)
			unit = strings.TrimSpace(dl[len(fmt.Sprintf("%d", n)):])
			if unit == "" {
				unit = "s" // 裸数字默认秒
			}
			switch unit {
			case "s", "sec", "secs", "second", "seconds":
			case "m", "min", "mins", "minute", "minutes":
				n *= 60
			case "h", "hour", "hours":
				n *= 3600
			default:
				err = fmt.Errorf("invalid RETAIN unit %q (s/m/h)", unit)
				return
			}
			retainSec = n
			i++
		case "SCRIPT":
			script = strings.Trim(next(), "'")
			i++
		case "OUTPUT":
			scriptOut = strings.ToLower(next())
			i++
		default:
			err = fmt.Errorf("unknown SUBMIT option near %q", toks[i])
			return
		}
	}
	return
}

// parseAtTime 解析 AT 时间："17:00"（今日已过则明日）或 "2026-10-16 17:00[:00]"。
func parseAtTime(s string) (time.Time, error) {
	now := time.Now()
	if t, err := time.ParseInLocation("15:04:05", s, now.Location()); err == nil {
		target := time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), 0, now.Location())
		if !target.After(now) {
			target = target.Add(24 * time.Hour)
		}
		return target, nil
	}
	if t, err := time.ParseInLocation("15:04", s, now.Location()); err == nil {
		target := time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), 0, 0, now.Location())
		if !target.After(now) {
			target = target.Add(24 * time.Hour)
		}
		return target, nil
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, now.Location()); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time %q (use HH:MM or YYYY-MM-DD HH:MM:SS)", s)
}

// handleAsyncQ 异步查询命令分发；handled=false 表示非本类命令。
func (h *Handler) handleAsyncQ(s string) (*mysql.Result, bool, error) {
	// SUBMIT QUERY
	if m := reSubmit.FindStringSubmatch(s); m != nil {
		if h.async == nil {
			return nil, true, mysql.NewError(1235, "async query is disabled")
		}
		if h.readonly {
			return nil, true, roErr()
		}
		sqlText := strings.ReplaceAll(m[1], "''", "'")
		cb, cbOpt, sign, timeout, at, win, when, part, script, scriptOut, retainSec, everySec, everyAt, perr := parseSubmitOpts(m[2])
		if perr != nil {
			return nil, true, mysql.NewError(1064, perr.Error())
		}
		if part != "" {
			if n, err := strconv.ParseInt(part, 10, 64); err != nil || n <= 0 {
				return nil, true, mysql.NewError(1064, "PARTITION expects a positive number")
			}
		}
		var partRows int64
		if part != "" {
			partRows, _ = strconv.ParseInt(part, 10, 64)
		}
		job := &asyncq.Job{
			SQL: sqlText, CallbackURL: cb, SignSecret: sign, TimeoutSec: timeout,
			At: at, Window: win, WhenExpr: when, PartRows: partRows,
			Script: script, ScriptOut: scriptOut, RetainSec: retainSec,
			EverySec: everySec, EveryAt: everyAt,
		}
		// 回调通道解析：http(s):// | tcp://host:port | amqp://… | kafka://broker
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
		id := h.async.Submit(job)
		res, err := meta.SimpleRS([]string{"job_id", "state"},
			[][]any{{id, string(asyncq.StatePending)}})
		return res, true, err
	}

	// SHOW QUERY JOBS / query_jobs 表
	if reShowJobs.MatchString(s) || reJobsTable.MatchString(s) {
		if h.async == nil {
			return nil, true, mysql.NewError(1235, "async query is disabled")
		}
		colsExpr, where := "*", ""
		if m := reJobsTable.FindStringSubmatch(s); m != nil {
			colsExpr, where = m[1], m[2]
		}
		rows := jobsRows(h.async.List())
		rows = filterAsyncRows(rows, where)
		if strings.TrimSpace(colsExpr) == "*" {
			res, err := meta.SimpleRS(asyncJobCols, rows)
			return res, true, err
		}
		// 投影：按请求列挑选，未知列补 NULL
		names := splitCols(colsExpr)
		idxm := func(name string) int {
			for i, c := range asyncJobCols {
				if strings.EqualFold(c, name) {
					return i
				}
			}
			return -1
		}
		var out [][]any
		for _, r := range rows {
			row := make([]any, len(names))
			for j, n := range names {
				if i := idxm(n); i >= 0 && i < len(r) {
					row[j] = r[i]
				}
			}
			out = append(out, row)
		}
		res, err := meta.SimpleRS(names, out)
		return res, true, err
	}

	// FETCH QUERY RESULT 'id' [LIMIT n [OFFSET m]]
	if m := reFetchRes.FindStringSubmatch(s); m != nil {
		return fetchResult(h, m[1], atoiOr(m[2], 1000), atoiOr(m[3], 0))
	}
	// SELECT * FROM query_result('id') [...]
	if m := reQResultFn.FindStringSubmatch(s); m != nil {
		return fetchResult(h, m[1], atoiOr(m[2], 1000), atoiOr(m[3], 0))
	}

	// CANCEL QUERY 'id'
	if m := reCancelJob.FindStringSubmatch(s); m != nil {
		if h.async == nil {
			return nil, true, mysql.NewError(1235, "async query is disabled")
		}
		if err := h.async.Cancel(m[1]); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		return meta.OKResultPub(), true, nil
	}
	// DROP QUERY RESULT 'id'
	if m := reDropJob.FindStringSubmatch(s); m != nil {
		if h.async == nil {
			return nil, true, mysql.NewError(1235, "async query is disabled")
		}
		if err := h.async.Drop(m[1]); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		return meta.OKResultPub(), true, nil
	}
	return nil, false, nil
}

var asyncJobCols = []string{"job_id", "state", "sql", "submit_ts", "elapsed_ms", "rows", "bytes",
	"error", "callback_url", "at", "window", "when", "every_sec", "recur_group"}

func jobsRows(jobs []asyncq.Job) [][]any {
	var out [][]any
	for _, j := range jobs {
		at, win := "", ""
		if j.At != nil {
			at = j.At.Format("2006-01-02 15:04:05")
		} else if j.EveryAt != "" {
			at = "daily " + j.EveryAt
		}
		if j.Window[0] != "" {
			win = j.Window[0] + "-" + j.Window[1]
		}
		out = append(out, []any{j.ID, string(j.State), j.SQL,
			j.SubmitTS.Format("2006-01-02 15:04:05"), j.ElapsedMs, j.Rows, j.Bytes,
			j.Error, j.CallbackURL, at, win, j.WhenExpr, j.EverySec, j.RecurGroup})
	}
	return out
}

// filterAsyncRows 支持 col = 'v' / col LIKE 'p' / col IN (...) AND 组合。
func filterAsyncRows(rows [][]any, where string) [][]any {
	where = strings.TrimSpace(where)
	if where == "" {
		return rows
	}
	where = regexp.MustCompile(`(?i)\bNOW\s*\(\s*\)`).ReplaceAllString(where, "'"+time.Now().Format("2006-01-02 15:04:05")+"'")
	type cond struct {
		col, op string
		vals    []string
	}
	var conds []cond
	reC := regexp.MustCompile(`(?i)(\w+)\s*(=|LIKE|IN)\s*('(?:[^']|'')*'(?:\s*,\s*'(?:[^']|'')*')*)`)
	for _, m := range reC.FindAllStringSubmatch(where, -1) {
		vals := []string{}
		for _, v := range regexp.MustCompile(`'((?:[^']|'')*)'`).FindAllStringSubmatch(m[3], -1) {
			vals = append(vals, strings.ReplaceAll(v[1], "''", "'"))
		}
		conds = append(conds, cond{strings.ToLower(m[1]), strings.ToUpper(m[2]), vals})
	}
	if len(conds) == 0 {
		return rows
	}
	var out [][]any
	for _, r := range rows {
		ok := true
		for _, c := range conds {
			idx := -1
			for i, name := range asyncJobCols {
				if strings.EqualFold(name, c.col) {
					idx = i
					break
				}
			}
			if idx < 0 || idx >= len(r) || len(c.vals) == 0 {
				continue
			}
			cell := fmt.Sprintf("%v", r[idx])
			switch c.op {
			case "=":
				ok = ok && cell == c.vals[0]
			case "LIKE":
				ok = ok && meta.LikeMatchPub(c.vals[0], cell)
			case "IN":
				hit := false
				for _, v := range c.vals {
					if cell == v {
						hit = true
						break
					}
				}
				ok = ok && hit
			}
		}
		if ok {
			out = append(out, r)
		}
	}
	return out
}

func fetchResult(h *Handler, id string, limit, offset int) (*mysql.Result, bool, error) {
	if h.async == nil {
		return nil, true, mysql.NewError(1235, "async query is disabled")
	}
	cols, rows, err := h.async.Fetch(id, limit, offset)
	if err != nil {
		return nil, true, mysql.NewError(1105, err.Error())
	}
	res, err2 := meta.SimpleRS(cols, rows)
	return res, true, err2
}

func atoiOr(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func splitCols(expr string) []string {
	var names []string
	for _, p := range strings.Split(expr, ",") {
		n := strings.TrimSpace(p)
		n = strings.Trim(n, "`\"'")
		if i := strings.LastIndex(n, "."); i >= 0 {
			n = n[i+1:]
		}
		if n == "" {
			n = "?"
		}
		names = append(names, n)
	}
	return names
}
