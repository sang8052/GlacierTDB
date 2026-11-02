//go:build !linux

package kv

// 独立运算核（v1.4.0）：非 Linux 平台不支持核绑定，dedicated_core 的单线程
// 串行执行语义仍然生效，启动日志注明。

// bindExecCore 非 Linux 平台：不绑核。返回 (-1, false)。
func (s *Store) bindExecCore(cpu int) (int, bool) { return -1, false }

// coreNote 非绑核场景的日志附注。
func coreNote() string { return " (core pinning is linux-only)" }
