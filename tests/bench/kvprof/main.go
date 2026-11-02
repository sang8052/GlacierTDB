// kvprof — KV 热路径微基准（诊断用，不入库不部署）。
// 复刻 bench_all.py redis 场景的 SET/GET 单连接往返，输出 ops/s 与 CPU profile。
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"runtime/pprof"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/kv"
	"glaciertdb/internal/sqlite"
)

var crlf = string([]byte{13, 10})

func bulk(s string) string { return "$" + fmt.Sprint(len(s)) + crlf + s + crlf }

func arr(parts ...string) string {
	out := "*" + fmt.Sprint(len(parts)) + crlf
	for _, s := range parts {
		out += bulk(s)
	}
	return out
}

func main() {
	cfg := config.Default()
	cfg.Fill()
	cfg.DBDir = "/tmp/kvdata"
	if os.Getenv("PERSIST") == "async" {
		cfg.KV.PersistMode = "async"
		cfg.KV.AsyncFlushMs = 200
	}
	os.MkdirAll("/tmp/kvdata", 0o755)
	backend, err := sqlite.NewBackend(cfg)
	if err != nil {
		panic(err)
	}
	store, err := kv.Open(backend, cfg)
	if err != nil {
		panic(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:6399")
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go kv.ServeConn(c, store, nil, cfg)
		}
	}()
	time.Sleep(300 * time.Millisecond)

	c, _ := net.Dial("tcp", "127.0.0.1:6399")
	w := bufio.NewWriter(c)
	r := bufio.NewReader(c)
	send := func(cmd string) string {
		w.WriteString(cmd)
		w.Flush()
		line, _ := r.ReadString(10)
		return line
	}
	setCmd := arr("SET", "bench:k", "v")
	getCmd := arr("GET", "bench:k")
	lpushCmd := arr("LPUSH", "bench:q", "m")
	rpopCmd := arr("RPOP", "bench:q")
	send(setCmd)
	send(getCmd)
	send(lpushCmd)
	send(rpopCmd)

	if os.Getenv("PROF") == "queue" {
		f, _ := os.Create("/tmp/cpu.prof")
		pprof.StartCPUProfile(f)
		t0 := time.Now()
		n := 0
		for time.Since(t0) < 8*time.Second {
			send(lpushCmd)
			send(rpopCmd)
			n += 2
		}
		pprof.StopCPUProfile()
		f.Close()
		fmt.Println("queue ops/s:", float64(n)/8, "per-op-us:", float64(time.Since(t0).Microseconds())/float64(n))
		return
	}

	f, _ := os.Create("/tmp/cpu.prof")
	pprof.StartCPUProfile(f)
	t0 := time.Now()
	n := 0
	for time.Since(t0) < 8*time.Second {
		send(setCmd)
		send(getCmd)
		n += 2
	}
	pprof.StopCPUProfile()
	f.Close()
	fmt.Println("ops/s:", float64(n)/8, "per-op-us:", float64(time.Since(t0).Microseconds())/float64(n))
}
