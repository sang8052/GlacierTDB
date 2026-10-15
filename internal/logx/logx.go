// Package logx 提供极简分级日志（stderr，单行格式），无第三方依赖。
package logx

import (
	"strings"
	"fmt"
	"os"
	"sync"
	"time"
)

type Level int

const (
	Debug Level = iota
	Info
	Warn
	Error
)

var (
	mu    sync.Mutex
	level = Info
)

func ParseLevel(s string) Level {
	switch s {
	case "debug":
		return Debug
	case "warn", "warning":
		return Warn
	case "error":
		return Error
	default:
		return Info
	}
}

func SetLevel(l Level) {
	mu.Lock()
	level = l
	mu.Unlock()
}

func logf(l Level, tag, format string, args ...any) {
	mu.Lock()
	defer mu.Unlock()
	if l < level {
		return
	}
	fmt.Fprintf(os.Stderr, "%s [%s] %s\n", time.Now().Format("2006-01-02 15:04:05.000"), tag, fmt.Sprintf(format, args...))
}

func Debugf(format string, args ...any) { logf(Debug, "DBG", format, args...) }
func Infof(format string, args ...any)  { logf(Info, "INF", format, args...) }
func Warnf(format string, args ...any)  { logf(Warn, "WRN", format, args...) }
func Errorf(format string, args ...any) { logf(Error, "ERR", format, args...) }

// ---------------------------------------------------------------------------
// SQL 审计日志（独立文件，JSON 行格式，按大小轮转）
// ---------------------------------------------------------------------------

const sqlLogRotateBytes = 100 << 20 // 100MB

var (
	sqlLogMu   sync.Mutex
	sqlLogFile *os.File
	sqlLogSize int64
)

// InitSQLLog 打开 SQL 审计日志文件（空路径 = 关闭功能）。
func InitSQLLog(path string) error {
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	var size int64
	if st, err := f.Stat(); err == nil {
		size = st.Size()
	}
	sqlLogMu.Lock()
	sqlLogFile = f
	sqlLogSize = size
	sqlLogMu.Unlock()
	return nil
}

// CloseSQLLog 关闭审计日志文件。
func CloseSQLLog() {
	sqlLogMu.Lock()
	if sqlLogFile != nil {
		sqlLogFile.Close()
		sqlLogFile = nil
	}
	sqlLogMu.Unlock()
}

// SQLAudit 写一行 JSON 审计记录（语句超长截断）。
func SQLAudit(line string) {
	sqlLogMu.Lock()
	defer sqlLogMu.Unlock()
	if sqlLogFile == nil {
		return
	}
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	n, _ := sqlLogFile.WriteString(line)
	sqlLogSize += int64(n)
	if sqlLogSize >= sqlLogRotateBytes {
		path := sqlLogFile.Name()
		sqlLogFile.Close()
		_ = os.Rename(path, path+".old") // 单文件轮转
		if f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			sqlLogFile = f
			sqlLogSize = 0
		} else {
			sqlLogFile = nil
		}
	}
}

// SQLLogEnabled 返回 SQL 审计日志是否开启。
func SQLLogEnabled() bool {
	sqlLogMu.Lock()
	defer sqlLogMu.Unlock()
	return sqlLogFile != nil
}
