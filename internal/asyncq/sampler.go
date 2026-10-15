package asyncq

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sampler 资源指标异步采样器（Linux /proc）。
// 指标：CPU（服务器 %）、MEM（服务器可用 %）、APP_CPU（本进程 %）、APP_MEM（本进程 RSS %）、IO_MBPS（本进程写 MB/s）。
// 采样周期默认 30s，可调，最低 10s（低于 10 拒绝，回退 10s 并告警）。
type Sampler struct {
	mu   sync.RWMutex
	m    map[string]float64
	iv   time.Duration
	stop chan struct{}
}

// NewSampler 启动采样循环。
func NewSampler(intervalSec int) *Sampler {
	if intervalSec < 10 {
		if intervalSec > 0 {
			intervalSec = 10
		} else {
			intervalSec = 30
		}
	}
	s := &Sampler{
		m:    map[string]float64{},
		iv:   time.Duration(intervalSec) * time.Second,
		stop: make(chan struct{}),
	}
	go s.loop()
	return s
}

func (s *Sampler) loop() {
	s.sample() // 启动即采一次
	t := time.NewTicker(s.iv)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.sample()
		case <-s.stop:
			return
		}
	}
}

func (s *Sampler) sample() {
	cpu := readGlobalCPUPct()
	mem := readGlobalMemPct()
	appCPU, appMem, ioMBps := readAppStats(s.iv)
	s.mu.Lock()
	s.m["CPU"] = cpu
	s.m["MEM"] = mem
	s.m["APP_CPU"] = appCPU
	s.m["APP_MEM"] = appMem
	s.m["IO_MBPS"] = ioMBps
	s.mu.Unlock()
}

// Satisfied 判定 WHEN 表达式（如 "CPU < 70 AND MEM < 80 AND APP_CPU < 50"）。
// 解析不了的条件视为不满足（保守：宁可等待也不超载执行）。
func (s *Sampler) Satisfied(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true
	}
	s.mu.RLock()
	m := make(map[string]float64, len(s.m))
	for k, v := range s.m {
		m[k] = v
	}
	s.mu.RUnlock()
	if len(m) == 0 { // 尚无样本
		return false
	}
	re := regexp.MustCompile(`(?i)([A-Z_]+)\s*(<|<=|>|>=)\s*([\d.]+)`)
	ok := true
	for _, cond := range re.FindAllStringSubmatch(expr, -1) {
		key := strings.ToUpper(cond[1])
		val, err := strconv.ParseFloat(cond[3], 64)
		if err != nil {
			return false
		}
		cur, has := m[key]
		if !has {
			return false // 未知指标：保守不满足
		}
		switch cond[2] {
		case "<":
			ok = ok && cur < val
		case "<=":
			ok = ok && cur <= val
		case ">":
			ok = ok && cur > val
		case ">=":
			ok = ok && cur >= val
		}
	}
	return ok
}

// ----- /proc 读取 -----

var (
	lastStat  [2]uint64 // /proc/stat 总 jiffies
	lastSelf  [2]uint64 // /proc/self utime+stime
	lastWrite uint64
)

func readGlobalCPUPct() float64 {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return -1
	}
	defer f.Close()
	var line string
	if _, err := fmt.Fscanln(f, &line); err != nil {
		return -1
	}
	var user, nice, sys, idle, iowait, irq, softirq, steal uint64
	_, _ = fmt.Sscanf(line, "cpu %d %d %d %d %d %d %d %d", &user, &nice, &sys, &idle, &iowait, &irq, &softirq, &steal)
	total := user + nice + sys + idle + iowait + irq + softirq + steal
	busy := total - idle - iowait
	if lastStat[1] == 0 {
		lastStat[0], lastStat[1] = busy, total
		return -1 // 首采无差值
	}
	db, dt := busy-lastStat[0], total-lastStat[1]
	lastStat[0], lastStat[1] = busy, total
	if dt == 0 {
		return 0
	}
	return float64(db) / float64(dt) * 100
}

func readGlobalMemPct() float64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return -1
	}
	var total, avail float64
	for _, ln := range strings.Split(string(b), "\n") {
		var kb float64
		if strings.HasPrefix(ln, "MemTotal:") {
			fmt.Sscanf(ln, "MemTotal: %f", &kb)
			total = kb
		} else if strings.HasPrefix(ln, "MemAvailable:") {
			fmt.Sscanf(ln, "MemAvailable: %f", &kb)
			avail = kb
		}
	}
	if total == 0 {
		return -1
	}
	return (1 - avail/total) * 100 // 已用 %
}

func readAppStats(iv time.Duration) (cpuPct, memPct, ioMBps float64) {
	// /proc/self/stat：utime stime（字段 14、15）
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return -1, -1, -1
	}
	fields := strings.Fields(string(b))
	if len(fields) < 15 {
		return -1, -1, -1
	}
	ut, _ := strconv.ParseUint(fields[13], 10, 64)
	st, _ := strconv.ParseUint(fields[14], 10, 64)
	self := ut + st
	if lastSelf[1] == 0 {
		lastSelf[0], lastSelf[1] = self, self
		cpuPct = -1
	} else {
		cpuPct = float64(self-lastSelf[0]) / iv.Seconds() / 100 * 100 // 100Hz jiffies → %
		lastSelf[0], lastSelf[1] = self, self
	}
	// RSS：/proc/self/status VmRSS（KB）
	if sb, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, ln := range strings.Split(string(sb), "\n") {
			var kb float64
			if strings.HasPrefix(ln, "VmRSS:") {
				fmt.Sscanf(ln, "VmRSS: %f", &kb)
				var mt float64
				if mb, err := os.ReadFile("/proc/meminfo"); err == nil {
					for _, l2 := range strings.Split(string(mb), "\n") {
						if strings.HasPrefix(l2, "MemTotal:") {
							fmt.Sscanf(l2, "MemTotal: %f", &mt)
						}
					}
				}
				if mt > 0 {
					memPct = kb / mt * 100
				}
				break
			}
		}
	}
	// 写入速率：/proc/self/io write_bytes
	if ib, err := os.ReadFile("/proc/self/io"); err == nil {
		for _, ln := range strings.Split(string(ib), "\n") {
			var wb uint64
			if strings.HasPrefix(ln, "write_bytes:") {
				fmt.Sscanf(ln, "write_bytes: %d", &wb)
				if lastWrite == 0 {
					lastWrite = wb
					ioMBps = -1
				} else {
					ioMBps = float64(wb-lastWrite) / 1024 / 1024 / iv.Seconds()
					lastWrite = wb
				}
				break
			}
		}
	}
	return
}
