// Package sqlite — MySQL 8 兼容 M4 标量函数族（v1.3.3）：
// 字符串（LOCATE/CHAR_LENGTH/REPEAT/REVERSE/SPACE/STRCMP/ASCII/UNHEX/FORMAT/REGEXP/TRIM 方向）、
// 数学（MOD/FLOOR/CEIL/POW/SQRT/LOG 族/三角/RADIANS/DEGREES/SIGN/TRUNCATE）、
// 日期（YEAR/MONTH/DAY 族/WEEKDAY/WEEK/QUARTER/DAYNAME/MONTHNAME/LAST_DAY/TO_DAYS/
// FROM_DAYS/SEC_TO_TIME/TIME_TO_SEC/EXTRACT 单位/DATE_ADD 族）。注册方式同 funcs.go。
package sqlite

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

func registerM4Funcs(c *sqlite3.Conn) error {
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

	// timeFn 日期时间单参函数（解析失败 → NULL）
	timeFn := func(f func(t time.Time, s string) any) sqlite3.ScalarFunction {
		return func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			t, ok := parseSQLTime(arg[0].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			switch v := f(t, arg[0].Text()).(type) {
			case int64:
				ctx.ResultInt64(v)
			case string:
				ctx.ResultText(v)
			case time.Time:
				ctx.ResultText(v.Format("2006-01-02 15:04:05"))
			case nil:
				ctx.ResultNull()
			}
		}
	}
	// mathFn 数学函数（NULL 传播、整数优先输出）
	mathFn := func(f func(x float64) float64) sqlite3.ScalarFunction {
		return func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(f(valueFloat(arg[0])))
		}
	}

	if err := must(
		// ---- 字符串 ----
		// LOCATE(substr, str[, pos])：MySQL 参数序（substr 在前），1 起，0 = 未命中
		reg("locate", -1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if len(arg) < 2 || anyNull(arg[:2]) {
				ctx.ResultNull()
				return
			}
			sub, s := arg[0].Text(), arg[1].Text()
			from := 0
			if len(arg) >= 3 && arg[2].Type() != sqlite3.NULL {
				p := valueInt(arg[2])
				if p > 1 {
					from = int(p - 1)
				}
				if p <= 0 {
					ctx.ResultInt64(0)
					return
				}
			}
			if from > len(s) {
				ctx.ResultInt64(0)
				return
			}
			if sub == "" {
				ctx.ResultInt64(int64(from + 1))
				return
			}
			i := strings.Index(s[from:], sub)
			if i < 0 {
				ctx.ResultInt64(0)
				return
			}
			ctx.ResultInt64(int64(from + i + 1))
		}),
		// CHAR_LENGTH / CHARACTER_LENGTH：字符数（UTF-8 安全）
		reg("char_length", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			ctx.ResultInt64(int64(len([]rune(arg[0].Text()))))
		}),
		reg("character_length", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			ctx.ResultInt64(int64(len([]rune(arg[0].Text()))))
		}),
		// REPEAT(s, n)：n<=0 → ''
		reg("repeat", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			n := valueInt(arg[1])
			if n <= 0 {
				ctx.ResultText("")
				return
			}
			if int64(len(arg[0].Text()))*n > 16<<20 {
				ctx.ResultNull()
				return
			}
			ctx.ResultText(strings.Repeat(arg[0].Text(), int(n)))
		}),
		// REVERSE(s)：按字符反转
		reg("reverse", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			r := []rune(arg[0].Text())
			for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
				r[i], r[j] = r[j], r[i]
			}
			ctx.ResultText(string(r))
		}),
		// SPACE(n)
		reg("space", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			n := valueInt(arg[0])
			if n <= 0 {
				ctx.ResultText("")
				return
			}
			ctx.ResultText(strings.Repeat(" ", int(n)))
		}),
		// STRCMP(a, b)：-1/0/1（字节序）
		reg("strcmp", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			ctx.ResultInt64(int64(strings.Compare(arg[0].Text(), arg[1].Text())))
		}),
		// ASCII(s)：首字节；空串 → 0
		reg("ascii", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			s := arg[0].Text()
			if s == "" {
				ctx.ResultInt64(0)
				return
			}
			ctx.ResultInt64(int64(s[0]))
		}),
		// UNHEX(s)：hex → 原始字节（非法 → NULL）
		reg("unhex", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			h := arg[0].Text()
			if len(h)%2 == 1 {
				h = "0" + h
			}
			out := make([]byte, len(h)/2)
			for i := 0; i+1 < len(h); i += 2 {
				hi, ok1 := unhexByte(h[i])
				lo, ok2 := unhexByte(h[i+1])
				if !ok1 || !ok2 {
					ctx.ResultNull()
					return
				}
				out[i/2] = hi<<4 | lo
			}
			ctx.ResultBlob(out)
		}),
		// FORMAT(x, d)：千分位 + 固定小数位
		reg("format", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			d := valueInt(arg[1])
			if d < 0 {
				d = 0
			}
			if d > 30 {
				d = 30
			}
			ctx.ResultText(mysqlFormatNum(x, int(d)))
		}),
		// REGEXP(pattern, str)：REGEXP 运算符后端（MySQL POSIX 语义近似为 RE2）
		reg("regexp", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			re, err := regexp.Compile(arg[0].Text())
			if err != nil {
				ctx.ResultError(err)
				return
			}
			ctx.ResultInt64(b2i64(re.MatchString(arg[1].Text())))
		}),
		// mysql_trim(kind, rem, s)：TRIM(BOTH|LEADING|TRAILING rem FROM s)
		reg("mysql_trim", 3, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			kind, rem, s := strings.ToLower(arg[0].Text()), arg[1].Text(), arg[2].Text()
			switch kind {
			case "leading":
				ctx.ResultText(strings.TrimLeft(s, rem))
			case "trailing":
				ctx.ResultText(strings.TrimRight(s, rem))
			default:
				ctx.ResultText(strings.Trim(s, rem))
			}
		}),
		// ---- 数学 ----
		reg("mod", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			a, b := valueInt2(arg)
			if b == 0 {
				if arg[0].Type() == sqlite3.INTEGER && arg[1].Type() == sqlite3.INTEGER {
					ctx.ResultNull() // MySQL 整数除 0 → NULL
					return
				}
				ctx.ResultNull()
				return
			}
			ctx.ResultInt64(a % b)
		}),
		reg("floor", 1, mathFn(math.Floor)),
		reg("ceil", 1, mathFn(math.Ceil)),
		reg("ceiling", 1, mathFn(math.Ceil)),
		reg("sqrt", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			if x < 0 {
				ctx.ResultNull() // MySQL 负数开方 → NULL
				return
			}
			ctx.ResultFloat(math.Sqrt(x))
		}),
		reg("pow", 2, math2Fn(math.Pow)),
		reg("power", 2, math2Fn(math.Pow)),
		reg("exp", 1, mathFn(math.Exp)),
		reg("ln", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			if x <= 0 {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(math.Log(x))
		}),
		reg("log", -1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if len(arg) == 0 || anyNull(arg) {
				ctx.ResultNull()
				return
			}
			if len(arg) == 1 {
				x := valueFloat(arg[0])
				if x <= 0 {
					ctx.ResultNull()
					return
				}
				ctx.ResultFloat(math.Log(x))
				return
			}
			b, x := valueFloat(arg[0]), valueFloat(arg[1])
			if b <= 0 || b == 1 || x <= 0 {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(math.Log(x) / math.Log(b))
		}),
		reg("log2", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			if x <= 0 {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(math.Log2(x))
		}),
		reg("log10", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			if x <= 0 {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(math.Log10(x))
		}),
		reg("sin", 1, mathFn(math.Sin)),
		reg("cos", 1, mathFn(math.Cos)),
		reg("tan", 1, mathFn(math.Tan)),
		reg("asin", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			if x < -1 || x > 1 {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(math.Asin(x))
		}),
		reg("acos", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			if x < -1 || x > 1 {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(math.Acos(x))
		}),
		reg("atan", -1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if len(arg) == 0 || anyNull(arg) {
				ctx.ResultNull()
				return
			}
			if len(arg) == 1 {
				ctx.ResultFloat(math.Atan(valueFloat(arg[0])))
				return
			}
			ctx.ResultFloat(math.Atan2(valueFloat(arg[0]), valueFloat(arg[1])))
		}),
		reg("atan2", 2, math2Fn(math.Atan2)),
		reg("pi", 0, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			ctx.ResultFloat(math.Pi)
		}),
		reg("cot", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			t := math.Tan(valueFloat(arg[0]))
			if t == 0 {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(1 / t)
		}),
		reg("radians", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(valueFloat(arg[0]) * math.Pi / 180)
		}),
		reg("degrees", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(valueFloat(arg[0]) * 180 / math.Pi)
		}),
		reg("sign", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			switch {
			case x > 0:
				ctx.ResultInt64(1)
			case x < 0:
				ctx.ResultInt64(-1)
			default:
				ctx.ResultInt64(0)
			}
		}),
		// TRUNCATE(x, d)：向零截断到 d 位小数
		reg("truncate", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			x := valueFloat(arg[0])
			d := valueInt(arg[1])
			p := math.Pow(10, float64(d))
			ctx.ResultFloat(math.Trunc(x*p) / p)
		}),
	); err != nil {
		return err
	}

	// ---- 日期时间族 ----
	if err := must(
		reg("year", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Year()) })),
		reg("month", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Month()) })),
		reg("day", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Day()) })),
		reg("dayofmonth", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Day()) })),
		reg("dayofweek", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Weekday()) + 1 })), // 1=周日
		reg("weekday", 1, timeFn(func(t time.Time, _ string) any { return int64((int(t.Weekday())+6)%7) })), // 0=周一
		reg("dayofyear", 1, timeFn(func(t time.Time, _ string) any { return int64(t.YearDay()) })),
		reg("quarter", 1, timeFn(func(t time.Time, _ string) any { return int64((int(t.Month())-1)/3 + 1) })),
		reg("week", 1, timeFn(func(t time.Time, _ string) any { return int64(weekNum(t, false)) })), // mode 0
		reg("weekofyear", 1, timeFn(func(t time.Time, _ string) any { return int64(isoWeekOn(t)) })),
		reg("hour", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Hour()) })),
		reg("minute", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Minute()) })),
		reg("second", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Second()) })),
		reg("microsecond", 1, timeFn(func(t time.Time, _ string) any { return int64(t.Nanosecond() / 1000) })),
		reg("dayname", 1, timeFn(func(t time.Time, _ string) any { return weekLong[int(t.Weekday())] })),
		reg("monthname", 1, timeFn(func(t time.Time, _ string) any { return monLong[int(t.Month())-1] })),
		// LAST_DAY(d)：当月最后一天（DATE 形态）
		reg("last_day", 1, timeFn(func(t time.Time, _ string) any {
			return t.AddDate(0, 1, -t.Day()).Format("2006-01-02")
		})),
		// TO_DAYS(d)：自公元 0 年起的天数（MySQL 儒略历近似）
		reg("to_days", 1, timeFn(func(t time.Time, _ string) any {
			return int64(daysSinceYear0(t))
		})),
		// FROM_DAYS(n)
		reg("from_days", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			n := valueInt(arg[0])
			t := time.Date(0, 1, 1, 0, 0, 0, 0, time.Local).AddDate(0, 0, int(n-366))
			ctx.ResultText(t.Format("2006-01-02"))
		}),
		// SEC_TO_TIME(n) → 'HH:MM:SS'
		reg("sec_to_time", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			n := valueInt(arg[0])
			neg := n < 0
			if neg {
				n = -n
			}
			ctx.ResultText(fmt.Sprintf("%s%02d:%02d:%02d", map[bool]string{true: "-", false: ""}[neg],
				n/3600, (n%3600)/60, n%60))
		}),
		// TIME_TO_SEC('HH:MM:SS')
		reg("time_to_sec", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if arg[0].Type() == sqlite3.NULL {
				ctx.ResultNull()
				return
			}
			parts := strings.Split(arg[0].Text(), ":")
			if len(parts) < 2 {
				ctx.ResultNull()
				return
			}
			var h, m, s int64
			if _, err := fmt.Sscanf(strings.Join(parts, ":"), "%d:%d:%d", &h, &m, &s); err != nil {
				if _, err := fmt.Sscanf(arg[0].Text(), "%d:%d", &h, &m); err != nil {
					ctx.ResultNull()
					return
				}
			}
			sign := int64(1)
			if strings.HasPrefix(arg[0].Text(), "-") {
				sign = -1
			}
			ctx.ResultInt64(sign * (h*3600 + m*60 + s))
		}),
		// EXTRACT(unit FROM d) 后端：extract_unit(d, 'unit')
		reg("extract_unit", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			t, ok := parseSQLTime(arg[0].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			unit := strings.ToUpper(strings.TrimSpace(arg[1].Text()))
			switch unit {
			case "MICROSECOND":
				ctx.ResultInt64(int64(t.Nanosecond() / 1000))
			case "SECOND":
				ctx.ResultInt64(int64(t.Second()))
			case "MINUTE":
				ctx.ResultInt64(int64(t.Minute()))
			case "HOUR":
				ctx.ResultInt64(int64(t.Hour()))
			case "DAY":
				ctx.ResultInt64(int64(t.Day()))
			case "WEEK":
				ctx.ResultInt64(int64(weekNum(t, false)))
			case "MONTH":
				ctx.ResultInt64(int64(t.Month()))
			case "QUARTER":
				ctx.ResultInt64(int64((int(t.Month())-1)/3 + 1))
			case "YEAR":
				ctx.ResultInt64(int64(t.Year()))
			default:
				ctx.ResultNull()
			}
		}),
		// mysql_date_add(date, n, 'unit')：日历感知（MONTH/QUARTER/YEAR 用 AddDate）
		reg("mysql_date_add", 3, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			t, ok := parseSQLTime(arg[0].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			n := valueInt(arg[1])
			unit := strings.ToUpper(strings.TrimSpace(arg[2].Text()))
			var out time.Time
			switch unit {
			case "MICROSECOND":
				out = t.Add(time.Duration(n) * time.Microsecond)
			case "SECOND":
				out = t.Add(time.Duration(n) * time.Second)
			case "MINUTE":
				out = t.Add(time.Duration(n) * time.Minute)
			case "HOUR":
				out = t.Add(time.Duration(n) * time.Hour)
			case "DAY", "WEEK":
				if unit == "WEEK" {
					n *= 7
				}
				out = t.AddDate(0, 0, int(n))
			case "MONTH", "QUARTER", "YEAR":
				var months int64
				switch unit {
				case "MONTH":
					months = n
				case "QUARTER":
					months = n * 3
				default:
					months = n * 12
				}
				// MySQL 语义：月/年加法对目标月天数 clamp（1-31 + 1 MONTH = 2-28/29）
				total := int64(t.Year())*12 + int64(t.Month()) - 1 + months
				y, m := int(total/12), int(total%12)
				if m < 0 {
					m += 12
					y--
				}
				m++ // 1 起
				last := time.Date(y, time.Month(m+1), 0, 0, 0, 0, 0, t.Location()).Day()
				d := t.Day()
				if d > last {
					d = last
				}
				out = time.Date(y, time.Month(m), d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
			default:
				ctx.ResultNull()
				return
			}
			// 保持输入形态：纯日期 → 纯日期输出；带时间 → 全时间戳
			if len(arg[0].Text()) <= 10 {
				ctx.ResultText(out.Format("2006-01-02"))
			} else {
				ctx.ResultText(out.Format("2006-01-02 15:04:05"))
			}
		}),
	); err != nil {
		return err
	}
	return nil
}

func math2Fn(f func(a, b float64) float64) sqlite3.ScalarFunction {
	return func(ctx sqlite3.Context, arg ...sqlite3.Value) {
		if len(arg) < 2 || anyNull(arg[:2]) {
			ctx.ResultNull()
			return
		}
		ctx.ResultFloat(f(valueFloat(arg[0]), valueFloat(arg[1])))
	}
}

// valueInt2 取双整数参数。
func valueInt2(arg []sqlite3.Value) (int64, int64) {
	return valueInt(arg[0]), valueInt(arg[1])
}

func unhexByte(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, true
	}
	return 0, false
}

func valueFloat(v sqlite3.Value) float64 {
	switch v.Type() {
	case sqlite3.INTEGER:
		return float64(v.Int64())
	case sqlite3.FLOAT:
		return v.Float()
	default:
		f, _ := strconv.ParseFloat(strings.TrimSpace(v.Text()), 64)
		return f
	}
}

func b2i64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// daysSinceYear0 MySQL TO_DAYS 的固定天数差（以 1970-01-01 = 719528 为锚）。
func daysSinceYear0(t time.Time) int64 {
	const anchor = int64(719528)
	return t.Unix()/86400 + anchor
}

// mysqlFormatNum 千分位格式化（MySQL FORMAT 语义）。
func mysqlFormatNum(x float64, d int) string {
	neg := x < 0
	if neg {
		x = -x
	}
	s := strconv.FormatFloat(x, 'f', d, 64)
	intPart := s
	frac := ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, frac = s[:i], s[i:]
	}
	var sb strings.Builder
	pre := len(intPart) % 3
	if pre > 0 {
		sb.WriteString(intPart[:pre])
		if len(intPart) > pre {
			sb.WriteByte(',')
		}
	}
	for i := pre; i < len(intPart); i += 3 {
		sb.WriteString(intPart[i : i+3])
		if i+3 < len(intPart) {
			sb.WriteByte(',')
		}
	}
	out := sb.String() + frac
	if neg && strings.Trim(out, "0.") != "" {
		return "-" + out
	}
	return out
}
