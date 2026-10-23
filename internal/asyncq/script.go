package asyncq

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"glaciertdb/internal/logx"
)

// runScript 后处理管道（v1.1.1）：结果 JSONL（jobs/<id>.jsonl）经 stdin 流入
// SCRIPT 指定的脚本，stdout 产物落 jobs/<id>.<OUTPUT>（缺省 OUTPUT=out）。
//   - 运行时按扩展名选择：.js → async_query.nodejs_path，.py → async_query.python_path，
//     .php → async_query.php_path（v1.3.1；空 = 禁用该运行时）；
//   - 脚本必须位于 async_query.script_dir 白名单目录内（防任意路径执行）；
//   - 超时取任务 TIMEOUT；CANCEL 可中断；stderr 摘要进错误消息。
func (e *Engine) runScript(j *Job) error {
	if j.Script == "" {
		return nil
	}
	var runtime string
	switch strings.ToLower(filepath.Ext(j.Script)) {
	case ".js":
		runtime = e.cfg.AsyncQuery.NodejsPath
	case ".py":
		runtime = e.cfg.AsyncQuery.PythonPath
	case ".php":
		runtime = e.cfg.AsyncQuery.PhpPath
	default:
		return fmt.Errorf("unsupported script type %q (expect .js / .py / .php)", filepath.Ext(j.Script))
	}
	if runtime == "" {
		return fmt.Errorf("script runtime disabled (set async_query.nodejs_path / python_path / php_path)")
	}
	// 白名单目录校验（脚本必须位于 script_dir 之下）
	if e.cfg.AsyncQuery.ScriptDir == "" {
		return fmt.Errorf("script_dir not configured (async_query.script_dir)")
	}
	base, err := filepath.Abs(e.cfg.AsyncQuery.ScriptDir)
	if err != nil {
		return err
	}
	script := j.Script
	if !filepath.IsAbs(script) {
		script = filepath.Join(base, script)
	}
	script, err = filepath.Abs(script)
	if err != nil {
		return err
	}
	sep := string(os.PathSeparator)
	if script != base && !strings.HasPrefix(script, base+sep) {
		return fmt.Errorf("script %s outside script_dir %s", j.Script, e.cfg.AsyncQuery.ScriptDir)
	}
	if _, err := os.Stat(script); err != nil {
		return fmt.Errorf("script %s: %w", j.Script, err)
	}

	outName := j.ScriptOut
	if outName == "" {
		outName = "out"
	}
	outName = filepath.Base(outName) // 防路径穿越
	outPath := filepath.Join(e.jobsDir, j.ID+"."+outName)

	in, err := os.Open(e.resultPath(j.ID))
	if err != nil {
		return fmt.Errorf("open result: %w", err)
	}
	defer in.Close()
	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	timeout := time.Duration(j.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(e.cfg.AsyncQuery.DefaultTimeoutSec) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// CANCEL 中断脚本
	go func() {
		select {
		case <-j.CancelCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	cmd := exec.CommandContext(ctx, runtime, script)
	cmd.Stdin = io.Reader(in)
	cmd.Stdout = out
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	logx.Infof("job %s: script %s via %s (output %s)", j.ID, j.Script, runtime, outName)
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > 300 {
			msg = msg[:300] + "..."
		}
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("script timeout after %s: %s", timeout, msg)
		}
		if j.CancelCh != nil {
			select {
			case <-j.CancelCh:
				return fmt.Errorf("script cancelled")
			default:
			}
		}
		return fmt.Errorf("script exit: %v: %s", err, msg)
	}
	st, _ := out.Stat()
	if st != nil {
		j.Bytes += st.Size()
	}
	return nil
}
