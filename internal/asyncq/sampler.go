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
// 指标：CPU（服务器 %）、MEM（服务器已用 %）、DISK_MBPS（服务器磁盘写 MB/s，v1.0.9）、
// APP_CPU（本进程 %）、APP_MEM（本进程 RSS %）、IO_MBPS（本进程写 MB/s）。
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
	disk := readGlobalDiskWriteMBps(s.iv)
	appCPU, appMem, ioMBps := readAppStats(s.iv)
	s.mu.Lock()
	s.m["CPU"] = cpu
	s.m["MEM"] = mem
	s.m["DISK_MBPS"] = disk
	s.m["APP_CPU"] = appCPU
	s.m["APP_MEM"] = appMem
	s.m["IO_MBPS"] = ioMBps
	s.mu.Unlock()
}

// VarInfo 监测变量快照（SHOW QUERY VARS 展示）。
type VarInfo struct {
	Name  string
	Value float64 // <0 表示尚无有效样本
	Level string  // 服务器级 / 应用级
	Desc  string
}

// varMeta 监测变量元信息（顺序即展示顺序）。
var varMeta = []struct{ name, level, desc string }{
	{"CPU", "服务器级", "服务器整机 CPU 占用 %（/proc/stat）"},
	{"MEM", "服务器级", "服务器内存已用 %（/proc/meminfo）"},
	{"DISK_MBPS", "服务器级", "服务器磁盘写速率 MB/s（/proc/diskstats 物理盘合计，v1.0.9）"},
	{"APP_CPU", "应用级", "本网关进程 CPU %（/proc/self/stat）"},
	{"APP_MEM", "应用级", "本网关进程 RSS / 总内存 %（/proc/self/status）"},
	{"IO_MBPS", "应用级", "本网关进程写盘速率 MB/s（/proc/self/io）"},
}

// Snapshot 返回全部监测变量当前值（SHOW QUERY VARS 用）。
func (s *Sampler) Snapshot() []VarInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]VarInfo, 0, len(varMeta))
	for _, m := range varMeta {
		out = append(out, VarInfo{Name: m.name, Value: s.m[m.name], Level: m.level, Desc: m.desc})
	}
	return out
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
	lastDiskSectors uint64 // /proc/diskstats 物理盘 sectors written 合计
)

// rePhysDisk 物理盘命名（分区带数字后缀不匹配：sda1 / nvme0n1p2 排除）。
var rePhysDisk = regexp.MustCompile(`^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|hd[a-z]+|nvme\d+n\d+|mmcblk\d+)$`)

// readGlobalDiskWriteMBps 服务器整机磁盘写速率：/proc/diskstats 物理盘
// sectors written（字段 10，512B/扇区）采样差值。容器内 /proc/diskstats
// 仍是宿主机视图，反映的是整机而非容器namespace。
func readGlobalDiskWriteMBps(iv time.Duration) float64 {
	b, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return -1
	}
	var sectors uint64
	for _, ln := range strings.Split(string(b), "\n") {
		f := strings.Fields(ln)
		if len(f) < 10 || !rePhysDisk.MatchString(f[2]) {
			continue
		}
		if sw, err := strconv.ParseUint(f[9], 10, 64); err == nil {
			sectors += sw
		}
	}
	if lastDiskSectors == 0 {
		if sectors == 0 {
			return -1
		}
		lastDiskSectors = sectors
		return -1 // 首采只建基线
	}
	mbps := float64(sectors-lastDiskSectors) * 512 / 1024 / 1024 / iv.Seconds()
	lastDiskSectors = sectors
	if mbps < 0 { // 计数器回绕
		return 0
	}
	return mbps
}

func readGlobalCPUPct() float64 {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return -1
	}
	// 首行 "cpu  user nice sys idle iowait irq softirq steal ..."（多空格分隔）
	fields := strings.Fields(strings.SplitN(string(b), "\n", 2)[0])
	if len(fields) < 5 || fields[0] != "cpu" {
		return -1
	}
	// v1.0.9 修复：此前 Fscanln(&line) 按空白分词只读到 "cpu" 一个词，
	// 后续 Sscanf 全部解析为 0，CPU 恒为 -1（首采基线无法建立）
	var vals = make([]uint64, len(fields)-1)
	for i, f := range fields[1:] {
		v, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			return -1
		}
		vals[i] = v
	}
	pick := func(i int) uint64 { // 老内核可能缺 iowait/irq/softirq/steal 尾字段
		if i < len(vals) {
			return vals[i]
		}
		return 0
	}
	user, nice, sys, idle := pick(0), pick(1), pick(2), pick(3)
	iowait, irq, softirq, steal := pick(4), pick(5), pick(6), pick(7)
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
