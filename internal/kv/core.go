package kv

// ---------------------------------------------------------------------------
// 独立运算核（v1.4.0 任务 3，kv.dedicated_core）
//
// 模型：dedicated_core=true 时，RESP 非阻塞命令经 exec 投递到单一执行 goroutine
// 串行执行（Redis 单线程模型），并把该 goroutine 的 OS 线程绑到指定核
// （Linux，core_linux.go），隔离 proxy/asyncq/S3 等组件的调度噪声。
//
// 务实实现（轻方案）：只包 dispatch 入口一处（server.go runDispatch），应答写出
// 本就在 dispatch 内，天然被投递覆盖；执行为同步等待（连接内保序、会话状态
// 只被单连接串行触达、MULTI/EXEC 内层命令顺序执行）。阻塞类命令
// （BLPOP/BRPOP/SUBSCRIBE 族/AUTH/HELLO，server.go blockingCmd）留在连接
// goroutine 原地执行——避免挂死全局串行执行核。
//
// 语义说明：每命令多两次 goroutine 切换，单连接延迟略升，多连接吞吐集中于
// 单核——这正是「独立运算核」的预期形态。GOMAXPROCS 不动。
// ---------------------------------------------------------------------------

import (
	"strconv"

	"glaciertdb/internal/logx"
)

// execQueueLen 运算核投递队列长度（有界；打满即阻塞投递方 = 天然背压）。
const execQueueLen = 1024

// exec 命令执行调度：非专用模式原地执行；专用模式投递运算核并同步等待完成。
// 运算核内 panic 被捕获（保持核存活，该连接收不到应答由客户端超时处理）。
func (s *Store) exec(fn func()) {
	if s.execCh == nil {
		fn()
		return
	}
	done := make(chan struct{})
	s.execCh <- func() {
		defer func() {
			if r := recover(); r != nil {
				logx.Errorf("kv exec core panic: %v", r)
			}
			close(done)
		}()
		fn()
	}
	<-done
}

// startExecCore 启动运算核执行 goroutine（dedicated_core=true 时由 Open 调用；
// 须在开始服务连接之前完成——Open 返回后才进入监听）。
func (s *Store) startExecCore() {
	s.execCh = make(chan func(), execQueueLen)
	go func() {
		cpu, pinned := s.bindExecCore(s.coreCPU)
		s.corePinned.Store(int64(cpu))
		if pinned {
			logx.Infof("kv: dedicated exec core online (cpu=%d, single-threaded dispatch)", cpu)
		} else {
			logx.Infof("kv: dedicated exec core online (unpinned%s)", coreNote())
		}
		for fn := range s.execCh {
			fn()
		}
	}()
}

// CoreMode INFO 观测：shared | dedicated(cpu=N) | dedicated(unpinned)（v1.4.0）。
func (s *Store) CoreMode() string {
	if s.execCh == nil {
		return "shared"
	}
	if c := s.corePinned.Load(); c >= 0 {
		return "dedicated(cpu=" + strconv.FormatInt(c, 10) + ")"
	}
	return "dedicated(unpinned)"
}
