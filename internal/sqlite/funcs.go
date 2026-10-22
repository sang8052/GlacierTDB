// Package sqlite — MySQL 常用标量函数的 Go 实现（v1.0.8）。
// 经 ncruces driver.Open 的连接初始化钩子注册到每个后端物理连接，
// 使 MD5/SHA 族与 LEFT/RIGHT/SUBSTRING_INDEX/DATEDIFF/TIMESTAMPDIFF/DATE_FORMAT
// 在任意表达式位置（SELECT / WHERE / UPDATE SET …）都可用；SQLite 按名解析，大小写不敏感。
package sqlite

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

// registerCompatFuncs 在新物理连接上注册 MySQL 兼容标量函数
// （MD5/SHA 族 + JSON 函数 + Geo/ST 函数，v1.0.8/v1.0.10）。
func registerCompatFuncs(c *sqlite3.Conn) error {
	reg := func(name string, nArg int, fn sqlite3.ScalarFunction) error {
		return c.CreateFunction(name, nArg, sqlite3.DETERMINISTIC, fn)
	}
	must := func(errs ...error) error {
		for _, e := range errs {
			if e != nil {
				return e
			}
		}
		return nil
	}
	if err := must(
		// 哈希族：入参 NULL → NULL，否则 32/40/64 位小写 hex
		reg("md5", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			sum := md5.Sum([]byte(arg[0].Text()))
			ctx.ResultText(hex.EncodeToString(sum[:]))
		}),
		reg("sha1", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			sum := sha1.Sum([]byte(arg[0].Text()))
			ctx.ResultText(hex.EncodeToString(sum[:]))
		}),
		reg("sha256", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			sum := sha256.Sum256([]byte(arg[0].Text()))
			ctx.ResultText(hex.EncodeToString(sum[:]))
		}),
		// SHA2(str, hash_length)：0/256/384/512（0 视为 256），其他长度 → NULL（MySQL 语义）
		reg("sha2", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL || arg[1].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			b := []byte(arg[0].Text())
			switch arg[1].Int64() {
			case 0, 256:
				sum := sha256.Sum256(b)
				ctx.ResultText(hex.EncodeToString(sum[:]))
			case 384:
				sum := sha512.Sum384(b)
				ctx.ResultText(hex.EncodeToString(sum[:]))
			case 512:
				sum := sha512.Sum512(b)
				ctx.ResultText(hex.EncodeToString(sum[:]))
			default:
				ctx.ResultNull()
			}
		}),
		// LEFT(str,len) / RIGHT(str,len)：负长度 → 空串（MySQL），超长取整串；UTF-8 安全
		reg("left", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			s, n, ok := strIntArgs(ctx, arg)
			if !ok {
				return
			}
			r := []rune(s)
			if n <= 0 || len(r) == 0 {
				ctx.ResultText("")
				return
			}
			if int64(len(r)) < n {
				n = int64(len(r))
			}
			ctx.ResultText(string(r[:n]))
		}),
		reg("right", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			s, n, ok := strIntArgs(ctx, arg)
			if !ok {
				return
			}
			r := []rune(s)
			if n <= 0 || len(r) == 0 {
				ctx.ResultText("")
				return
			}
			if int64(len(r)) < n {
				n = int64(len(r))
			}
			ctx.ResultText(string(r[int64(len(r))-n:]))
		}),
		// SUBSTRING_INDEX(str,delim,count)：count>0 取第 count 个 delim 之前（含）；
		// count<0 取倒数第 |count| 个 delim 之后；count=0 / 空 delim → ''
		reg("substring_index", 3, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			s, d := arg[0].Text(), arg[1].Text()
			n := valueInt(arg[2])
			if n == 0 || d == "" {
				ctx.ResultText("")
				return
			}
			if n > 0 {
				idx, hit := 0, int64(0)
				for i := 0; i+len(d) <= len(s); {
					j := strings.Index(s[i:], d)
					if j < 0 {
						break
					}
					idx = i + j + len(d)
					i = idx
					hit++
					if hit == n {
						break
					}
				}
				if hit < n {
					ctx.ResultText(s)
					return
				}
				ctx.ResultText(s[:idx])
				return
			}
			// n < 0：从右往左数
			want := -n
			idx, hit := -1, int64(0)
			for i := len(s); i-len(d) >= 0; {
				j := strings.LastIndex(s[:i], d)
				if j < 0 {
					break
				}
				idx = j + len(d)
				i = j
				hit++
				if hit == want {
					break
				}
			}
			if hit < want || idx < 0 || idx > len(s) {
				ctx.ResultText(s)
				return
			}
			ctx.ResultText(s[idx:])
		}),
		// DATEDIFF(a,b)：a-b 的整天数（按日期截断时间部分）
		reg("datediff", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			ta, oka := parseSQLTime(arg[0].Text())
			tb, okb := parseSQLTime(arg[1].Text())
			if !oka || !okb {
				ctx.ResultNull()
				return
			}
			da := time.Date(ta.Year(), ta.Month(), ta.Day(), 0, 0, 0, 0, time.UTC)
			db := time.Date(tb.Year(), tb.Month(), tb.Day(), 0, 0, 0, 0, time.UTC)
			ctx.ResultInt64(int64(da.Sub(db) / (24 * time.Hour)))
		}),
		// TIMESTAMPDIFF(unit, a, b)：b-a 按单位取整（日历感知的 MONTH/QUARTER/YEAR）
		reg("timestampdiff", 3, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			ta, oka := parseSQLTime(arg[1].Text())
			tb, okb := parseSQLTime(arg[2].Text())
			if !oka || !okb {
				ctx.ResultNull()
				return
			}
			d := tb.Sub(ta)
			unit := strings.ToUpper(strings.TrimSpace(arg[0].Text()))
			switch unit {
			case "MICROSECOND":
				ctx.ResultInt64(d.Microseconds())
			case "SECOND":
				ctx.ResultInt64(int64(d / time.Second))
			case "MINUTE":
				ctx.ResultInt64(int64(d / time.Minute))
			case "HOUR":
				ctx.ResultInt64(int64(d / time.Hour))
			case "DAY":
				ctx.ResultInt64(int64(d / (24 * time.Hour)))
			case "WEEK":
				ctx.ResultInt64(int64(d / (7 * 24 * time.Hour)))
			case "MONTH", "QUARTER", "YEAR":
				m := monthsBetween(ta, tb)
				switch unit {
				case "MONTH":
					ctx.ResultInt64(m)
				case "QUARTER":
					ctx.ResultInt64(m / 3)
				default:
					ctx.ResultInt64(m / 12)
				}
			default:
				ctx.ResultError(fmt.Errorf("unknown TIMESTAMPDIFF unit %s", unit))
			}
		}),
		// DATE_FORMAT(date, fmt)：MySQL 格式符子集（%Y %y %m %c %d %e %H %k %h %I %l
		// %i %s %S %f %p %r %T %W %a %M %b %j %w %%），未知格式符原样输出
		reg("date_format", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			t, ok := parseSQLTime(arg[0].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			ctx.ResultText(mysqlDateFormat(t, arg[1].Text()))
		}),
	); err != nil {
		return err
	}
	if err := must(
		// JSON 兼容函数（v1.0.10：json_length/contains/keys/search/merge_patch）
		registerJSONFuncs(c),
		// Geo/ST 函数族（v1.0.10：WKT/WKB/GeoJSON 自动识别）
		registerGeoFuncs(c),
	); err != nil {
		return err
	}
	return nil
}

// strIntArgs 取 (text, int) 参数对；任一为 NULL → 结果 NULL 返回 false。
func strIntArgs(ctx sqlite3.Context, arg []sqlite3.Value) (string, int64, bool) {
	if arg[0].Type() == sqlite3.NULL || arg[1].Type() == sqlite3.NULL {
		ctx.ResultNull()
		return "", 0, false
	}
	return arg[0].Text(), valueInt(arg[1]), true
}

func anyNull(arg []sqlite3.Value) bool {
	for _, a := range arg {
		if a.Type() == sqlite3.NULL {
			return true
		}
	}
	return false
}

// valueInt 按动态类型取整数（INTEGER 直取，TEXT/FLOAT 走解析）。
func valueInt(v sqlite3.Value) int64 {
	switch v.Type() {
	case sqlite3.INTEGER:
		return v.Int64()
	case sqlite3.FLOAT:
		return int64(v.Float())
	default:
		n, err := strconv.ParseInt(strings.TrimSpace(v.Text()), 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
}

// parseSQLTime 解析 SQLite/MySQL 常见日期时间文本（本地时区）。
func parseSQLTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
		"15:04:05",
	} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// monthsBetween 日历感知的整月差（b-a），向零取整。
func monthsBetween(a, b time.Time) int64 {
	m := int64(b.Year()-a.Year())*12 + int64(b.Month()) - int64(a.Month())
	// 未满整月则向零回退一步
	if m > 0 && b.AddDate(0, -int(m), 0).Before(a) {
		m--
	} else if m < 0 && b.AddDate(0, -int(m), 0).After(a) {
		m++
	}
	return m
}

var (
	weekLong  = []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
	weekShort = []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
	monLong   = []string{"January", "February", "March", "April", "May", "June",
		"July", "August", "September", "October", "November", "December"}
	monShort = []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
)

// mysqlDateFormat 按 MySQL DATE_FORMAT 格式符渲染时间。
func mysqlDateFormat(t time.Time, format string) string {
	var sb strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			sb.WriteByte(format[i])
			continue
		}
		i++
		switch format[i] {
		case 'Y':
			fmt.Fprintf(&sb, "%04d", t.Year())
		case 'y':
			fmt.Fprintf(&sb, "%02d", t.Year()%100)
		case 'm':
			fmt.Fprintf(&sb, "%02d", int(t.Month()))
		case 'c':
			fmt.Fprintf(&sb, "%d", int(t.Month()))
		case 'd':
			fmt.Fprintf(&sb, "%02d", t.Day())
		case 'e':
			fmt.Fprintf(&sb, "%d", t.Day())
		case 'H':
			fmt.Fprintf(&sb, "%02d", t.Hour())
		case 'k':
			fmt.Fprintf(&sb, "%d", t.Hour())
		case 'h', 'I':
			fmt.Fprintf(&sb, "%02d", hour12(t))
		case 'l':
			fmt.Fprintf(&sb, "%d", hour12(t))
		case 'i':
			fmt.Fprintf(&sb, "%02d", t.Minute())
		case 's', 'S':
			fmt.Fprintf(&sb, "%02d", t.Second())
		case 'f':
			fmt.Fprintf(&sb, "%06d", t.Nanosecond()/1000)
		case 'p':
			if t.Hour() >= 12 {
				sb.WriteString("PM")
			} else {
				sb.WriteString("AM")
			}
		case 'r':
			fmt.Fprintf(&sb, "%02d:%02d:%02d %s", hour12(t), t.Minute(), t.Second(), ampm(t))
		case 'T':
			fmt.Fprintf(&sb, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
		case 'W':
			sb.WriteString(weekLong[int(t.Weekday())])
		case 'a':
			sb.WriteString(weekShort[int(t.Weekday())])
		case 'M':
			sb.WriteString(monLong[int(t.Month())-1])
		case 'b':
			sb.WriteString(monShort[int(t.Month())-1])
		case 'j':
			fmt.Fprintf(&sb, "%03d", t.YearDay())
		case 'w':
			fmt.Fprintf(&sb, "%d", int(t.Weekday()))
		case '%':
			sb.WriteByte('%')
		default:
			sb.WriteByte('%')
			sb.WriteByte(format[i])
		}
	}
	return sb.String()
}

func hour12(t time.Time) int {
	h := t.Hour() % 12
	if h == 0 {
		h = 12
	}
	return h
}

func ampm(t time.Time) string {
	if t.Hour() >= 12 {
		return "PM"
	}
	return "AM"
}
