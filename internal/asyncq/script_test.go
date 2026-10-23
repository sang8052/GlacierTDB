package asyncq

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"glaciertdb/internal/config"
)

// TestRunScriptPipeline 后处理管道：结果 JSONL → 脚本 stdin → 产物落 jobs/<id>.<OUTPUT>。
// 用 /bin/sh 假装 runtime（脚本体是 shell 管道），验证机制本身。
func TestRunScriptPipeline(t *testing.T) {
	dir := t.TempDir()
	scriptDir := filepath.Join(dir, "scripts")
	if err := os.MkdirAll(scriptDir, 0o755); err != nil {
		t.Fatal(err)
	}
	input := "{\"columns\":[\"a\"]}\n[1]\n[2]\n"
	if err := os.WriteFile(filepath.Join(dir, "t1.jsonl"), []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(scriptDir, "pipe.js")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nwc -c | tr -d ' '\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	e := &Engine{
		jobsDir: dir,
		cfg: &config.Config{AsyncQuery: config.AsyncOpts{
			NodejsPath:        "/bin/sh",
			ScriptDir:         scriptDir,
			DefaultTimeoutSec: 30,
		}},
	}
	j := &Job{ID: "t1", Script: "pipe.js", ScriptOut: "count"}
	if err := e.runScript(j); err != nil {
		t.Fatalf("runScript: %v", err)
	}
	out, err := os.ReadFile(filepath.Join(dir, "t1.count"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(out)), strconv.Itoa(len(input)); got != want {
		t.Fatalf("output = %q, want %q (byte count of stdin)", got, want)
	}
}

// TestRunScriptWhitelist 白名单目录校验：script_dir 之外的脚本拒绝执行。
func TestRunScriptWhitelist(t *testing.T) {
	dir := t.TempDir()
	scriptDir := filepath.Join(dir, "scripts")
	if err := os.MkdirAll(scriptDir, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(dir, "evil.js")
	if err := os.WriteFile(outside, []byte("#!/bin/sh\necho pwned\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	e := &Engine{
		jobsDir: dir,
		cfg: &config.Config{AsyncQuery: config.AsyncOpts{
			NodejsPath: "/bin/sh",
			ScriptDir:  scriptDir,
		}},
	}
	j := &Job{ID: "t2", Script: "../evil.js", ScriptOut: "out"}
	if err := e.runScript(j); err == nil {
		t.Fatal("expected whitelist rejection, got nil")
	}
}

// TestRunScriptNoRuntime 未配置 runtime 时明确报错。
func TestRunScriptNoRuntime(t *testing.T) {
	dir := t.TempDir()
	scriptDir := filepath.Join(dir, "scripts")
	_ = os.MkdirAll(scriptDir, 0o755)
	_ = os.WriteFile(filepath.Join(scriptDir, "x.js"), []byte("1"), 0o644)
	e := &Engine{
		jobsDir: dir,
		cfg:     &config.Config{AsyncQuery: config.AsyncOpts{ScriptDir: scriptDir}},
	}
	if err := e.runScript(&Job{ID: "t3", Script: "x.js"}); err == nil {
		t.Fatal("expected runtime-disabled error, got nil")
	}
}
