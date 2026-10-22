// Package handler — 异步查询命令（v1.0.5）：SUBMIT / SHOW QUERY JOBS / query_jobs /
// query_result / FETCH / CANCEL / DROP。
package handler

import (
	"fmt"
	"math"
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
	reShowQVars  = regexp.MustCompile(`(?i)^SHOW\s+QUERY\s+VARS\s*$`)
	reJobsTable  = regexp.MustCompile(`(?is)^SELECT\s+(.+?)\s+FROM\s+query_jobs(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+.+?)?(?:\s+LIMIT\s+\d+(?:\s*,\s*\d+)?)?\s*$`)
)

// parseSubmitOpts 解析 SUBMIT 之后的选项段。
func parseSubmitOpts(rest string) (cb, cbOpt, sign string, timeout int, at *time.Time, win [2]string, weekWd int, when, part, partBytes, script, scriptOut string, retainSec, everySec int, everyAt string, err error) {
	s := strings.TrimSpace(rest)
	// WHEN 到 PARTITION/SCRIPT 前的整段。v1.0.9 修复：WHEN 作为首个选项
	// （紧跟 SQL 引号）时，旧正则要求 WHEN 前必须有空白，导致报 unknown SUBMIT
	// option near "WHEN"——现允许 WHEN/PARTITION/SCRIPT 出现在选项串开头。
	var whenRaw string
	reWhenTail := regexp.MustCompile(`(?i)(?:^|\s)WHEN\s+(.*)$`)
	reWhenStop := regexp.MustCompile(`(?i)(?:^|\s)WHEN\s+(.*?)\s+(?:PARTITION|SCRIPT)\b`)
	rePartHead := regexp.MustCompile(`(?i)(?:^|\s)(?:PARTITION|SCRIPT)\b`)
	if rePartHead.MatchString(s) {
		if m := reWhenStop.FindStringSubmatch(s); m != nil {
			whenRaw = strings.TrimSpace(m[1])
		} else if w := reWhenTail.FindStringSubmatch(s); w != nil {
			whenRaw = strings.TrimSpace(w[1])
		}
	} else if w := reWhenTail.FindStringSubmatch(s); w != nil {
		whenRaw = strings.TrimSpace(w[1])
	}
	if whenRaw != "" {
		if i := strings.Index(strings.ToUpper(s), "WHEN"); i >= 0 {
			s = strings.TrimSpace(s[:i])
		}
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
			// 可选周几前缀：周一..周日 / MON..SUN（单日限定），如 '周五 17:00-20:00'
			if k := strings.IndexAny(w, " \t"); k > 0 {
				head, restW := w[:k], strings.TrimSpace(w[k+1:])
				if d, ok := parseWeekday(head); ok && restW != "" {
					weekWd = d
					w = restW
				}
			}
			parts := strings.SplitN(w, "-", 2)
			if len(parts) != 2 {
				err = fmt.Errorf("invalid WINDOW '%s' (expect 'HH:MM-HH:MM' or '周X HH:MM-HH:MM')", w)
				return
			}
			win = [2]string{strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])}
			i++
		case "PARTITION":
			// PARTITION BY ROWS n / BYTES n（v1.0.8：BYTES 按行序列化字节数切分）
			if i+1 < len(toks) && strings.ToUpper(toks[i+1]) == "BY" {
				i++
			}
			unit := ""
			if i+1 < len(toks) {
				unit = strings.ToUpper(toks[i+1])
			}
			if unit != "ROWS" && unit != "BYTES" {
				err = fmt.Errorf("invalid PARTITION (expect BY ROWS n or BY BYTES n)")
				return
			}
			i++
			val := strings.TrimSpace(next())
			if n, e := strconv.ParseInt(val, 10, 64); e != nil || n <= 0 {
				err = fmt.Errorf("PARTITION expects a positive number, got %q", val)
				return
			}
			if unit == "BYTES" {
				partBytes = val
			} else {
				part = val
			}
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

// parseWeekday 解析周几：周一..周日 / 周1..周7 / MON..SUN → 1..7（周一=1，周日=7）。
func parseWeekday(s string) (int, bool) {
	t := strings.ToUpper(strings.TrimSpace(s))
	if len(t) >= 2 && (t[0:1] == "周" || strings.HasPrefix(t, "星期")) {
		num := strings.TrimPrefix(strings.TrimPrefix(t, "星期"), "周")
		switch num {
		case "一", "1":
			return 1, true
		case "二", "2":
			return 2, true
		case "三", "3":
			return 3, true
		case "四", "4":
			return 4, true
		case "五", "5":
			return 5, true
		case "六", "6":
			return 6, true
		case "日", "天", "7":
			return 7, true
		}
		return 0, false
	}
	switch t {
	case "MON", "MONDAY":
		return 1, true
	case "TUE", "TUESDAY":
		return 2, true
	case "WED", "WEDNESDAY":
		return 3, true
	case "THU", "THURSDAY":
		return 4, true
	case "FRI", "FRIDAY":
		return 5, true
	case "SAT", "SATURDAY":
		return 6, true
	case "SUN", "SUNDAY":
		return 7, true
	}
	return 0, false
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
		cb, cbOpt, sign, timeout, at, win, weekWd, when, part, partBytes, script, scriptOut, retainSec, everySec, everyAt, perr := parseSubmitOpts(m[2])
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
		var partBytesN int64
		if partBytes != "" {
			partBytesN, _ = strconv.ParseInt(partBytes, 10, 64)
		}
		job := &asyncq.Job{
			SQL: sqlText, CallbackURL: cb, SignSecret: sign, TimeoutSec: timeout,
			At: at, Window: win, WindowWd: weekWd, WhenExpr: when, PartRows: partRows, PartBytes: partBytesN,
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

	// SHOW QUERY VARS —— WHEN 监测变量当前值（v1.0.9；含服务器级 DISK_MBPS）
	if reShowQVars.MatchString(s) {
		if h.async == nil {
			return nil, true, mysql.NewError(1235, "async query is disabled")
		}
		var rows [][]any
		for _, v := range h.async.Vars() {
			var val any
			if v.Value >= 0 { // <0 = 尚无有效样本（首采只建基线）
				val = math.Round(v.Value*100) / 100
			}
			rows = append(rows, []any{v.Name, val, v.Level, v.Desc})
		}
		res, err := meta.SimpleRS([]string{"Variable_name", "Value", "Level", "Description"}, rows)
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
		if h.readonly {
			return nil, true, roErr()
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
		if h.readonly {
			return nil, true, roErr()
		}
		if err := h.async.Drop(m[1]); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		return meta.OKResultPub(), true, nil
	}
	return nil, false, nil
}

var asyncJobCols = []string{"job_id", "state", "sql", "submit_ts", "elapsed_ms", "rows", "bytes",
	"error", "callback_url", "at", "window", "when", "every_sec", "recur_group", "partition_rows", "partition_bytes"}

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
			if j.WindowWd > 0 {
				win = weekdayCN(j.WindowWd) + " " + win
			}
		}
		out = append(out, []any{j.ID, string(j.State), j.SQL,
			j.SubmitTS.Format("2006-01-02 15:04:05"), j.ElapsedMs, j.Rows, j.Bytes,
			j.Error, j.CallbackURL, at, win, j.WhenExpr, j.EverySec, j.RecurGroup, j.PartRows, j.PartBytes})
	}
	return out
}

// weekdayCN 1..7 → 周一..周日（任务表展示用）。
func weekdayCN(wd int) string {
	names := []string{"", "周一", "周二", "周三", "周四", "周五", "周六", "周日"}
	if wd >= 1 && wd <= 7 {
		return names[wd]
	}
	return fmt.Sprintf("周%d", wd)
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
