// 在线备份（v1.3.12 P0-1）：服务运行中对全部库逐个 VACUUM INTO 出
// 单文件快照（WAL 读快照，不阻塞业务写入），全量成功后原子落位。
// 备份产物保持普通 SQLite 单文件（加密部署下为同口令 adiantum 密文单文件），
// 保住「拷贝文件即迁移」的卖点——但运行中备份必须经过本工具，不能直接
// cp -r（拷贝期间 checkpoint 会得到主库与 -wal 不一致的组合）。
package sqlite

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3/driver"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
)

// BackupResult 单库备份结果。
type BackupResult struct {
	File    string
	Size    int64
	Cost    time.Duration
	Objects int // sqlite_master 对象数（前后一致性校验用）
}

// BackupAll 在线备份：遍历 db_dir 下全部库（sys/ + user/ + 根目录残留），
// 逐库 VACUUM INTO 临时目录，全部成功后原子替换目标目录。
// 单库失败立即中止（半成品留在 <target>.part，不会被误用）；
// verify=true 时对每个产物做 integrity_check + 对象数比对（多花几秒换确定性）。
func BackupAll(cfg *config.Config, target string, verify bool) ([]BackupResult, error) {
	if target == "" {
		return nil, fmt.Errorf("backup target directory is empty")
	}
	files, err := listBackupFiles(cfg.DBDir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no database files found under %s", cfg.DBDir)
	}

	// 加密参数与 NewBackend 一致（不抢目录锁：WAL 多进程读并发安全）
	encrypt := cfg.Security.Encrypt
	passphrase := cfg.Security.Passphrase
	if encrypt && passphrase == "" {
		return nil, fmt.Errorf("security.encrypt 开启时必须配置 security.passphrase")
	}

	part := target + ".part"
	_ = os.RemoveAll(part)
	if err := os.MkdirAll(part, 0o755); err != nil {
		return nil, fmt.Errorf("create %s: %w", part, err)
	}
	defer func() { _ = os.RemoveAll(part) }() // 正常路径 rename 后 part 已不存在

	var out []BackupResult
	for _, f := range files {
		start := time.Now()
		src, err := driver.Open(backupDSN(f, encrypt, passphrase, cfg))
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", f, err)
		}
		var objs int
		if err := src.QueryRow("SELECT count(*) FROM sqlite_master").Scan(&objs); err != nil {
			src.Close()
			return nil, fmt.Errorf("count %s: %w", f, err)
		}
		dst := filepath.Join(part, filepath.Base(f))
		// VACUUM INTO 读快照写新文件：与运行中服务的写入并发安全（WAL），
		// 产物为紧凑单文件（无 -wal/-shm 伴随文件）
		if _, err := src.Exec("VACUUM INTO ?", backupURI(dst, encrypt, passphrase)); err != nil {
			src.Close()
			return nil, fmt.Errorf("vacuum-into %s: %w", filepath.Base(f), err)
		}
		src.Close()

		if verify {
			vdb, err := driver.Open(backupDSN(dst, encrypt, passphrase, cfg))
			if err != nil {
				return nil, fmt.Errorf("verify open %s: %w", dst, err)
			}
			var ok string
			var objsAfter int
			verr := vdb.QueryRow("PRAGMA integrity_check").Scan(&ok)
			if verr == nil {
				verr = vdb.QueryRow("SELECT count(*) FROM sqlite_master").Scan(&objsAfter)
			}
			vdb.Close()
			if verr != nil || ok != "ok" || objsAfter != objs {
				return nil, fmt.Errorf("verify %s failed (integrity=%q objs %d/%d): %v",
					filepath.Base(f), ok, objsAfter, objs, verr)
			}
		}
		size := fileSize(dst)
		out = append(out, BackupResult{File: filepath.Base(f), Size: size,
			Cost: time.Since(start), Objects: objs})
		logx.Infof("backup: %s done (%d bytes, %s)", filepath.Base(f), size, time.Since(start).Round(time.Millisecond))
	}

	// 全部成功：目标已存在则先移走旧备份（保留一代），再原子 rename
	if _, err := os.Stat(target); err == nil {
		old := target + ".old"
		_ = os.RemoveAll(old)
		if err := os.Rename(target, old); err != nil {
			return nil, fmt.Errorf("rotate old backup %s: %w", target, err)
		}
		logx.Infof("backup: previous backup rotated to %s", old)
	}
	if err := os.Rename(part, target); err != nil {
		return nil, fmt.Errorf("finalize backup %s: %w", target, err)
	}
	return out, nil
}

// listBackupFiles 收集待备份库文件（sys/ + user/ + 根目录残留；跳过
// -wal/-shm/临时文件）。
func listBackupFiles(dbDir string) ([]string, error) {
	var out []string
	for _, sub := range []string{SysDir, UserDir, ""} {
		dir := dbDir
		if sub != "" {
			dir = filepath.Join(dbDir, sub)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".db") {
				continue
			}
			out = append(out, filepath.Join(dir, name))
		}
	}
	sort.Strings(out)
	return out, nil
}

// backupDSN 打开源/产物库（与 Backend.dsnFor 同语义：加密时 textkey pragma
// 必须先于其它 pragma；备份只读，不设置 journal_mode）。
func backupDSN(file string, encrypt bool, passphrase string, cfg *config.Config) string {
	p := strings.ReplaceAll(filepath.Clean(file), "\\", "/")
	var sb strings.Builder
	for _, r := range p {
		switch {
		case r == '/' || r == ':' || r == '-' || r == '_' || r == '.' || r == '~',
			r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			sb.WriteRune(r)
		default:
			for _, c := range []byte(string(r)) {
				fmt.Fprintf(&sb, "%%%02X", c)
			}
		}
	}
	if encrypt {
		kv := url.Values{}
		kv.Add("vfs", "adiantum")
		kv.Add("_pragma", fmt.Sprintf("textkey('%s')", passphrase))
		return "file:" + sb.String() + "?" + kv.Encode() +
			"&_pragma=busy_timeout(5000)&_pragma=synchronous(" + fmt.Sprint(cfg.SQLite.SynchronousLevel()) + ")"
	}
	return "file:" + sb.String() +
		"?_pragma=busy_timeout(5000)&_pragma=synchronous(" + fmt.Sprint(cfg.SQLite.SynchronousLevel()) + ")"
}

// backupURI VACUUM INTO 目标 URI（加密部署下产物同口令加密）。
func backupURI(file string, encrypt bool, passphrase string) string {
	if !encrypt {
		return "file:" + strings.ReplaceAll(filepath.Clean(file), "\\", "/")
	}
	return "file:" + strings.ReplaceAll(filepath.Clean(file), "\\", "/") +
		"?vfs=adiantum&textkey=" + url.QueryEscape(passphrase)
}
