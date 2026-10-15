package sqlite

import (
	"net/url"
	"testing"
)

// TestDSNFor 验证跨平台 DSN 生成（Windows 盘符/反斜杠/空格/中文）。
func TestDSNFor(t *testing.T) {
	b := &Backend{dsnOpts: "_pragma=busy_timeout%285000%29"}

	cases := []struct{ in, want string }{
		{"/data/backup.db", "file:/data/backup.db?_pragma=busy_timeout%285000%29"},
		{"data/backup.db", "file:data/backup.db?_pragma=busy_timeout%285000%29"},
		{`H:\glaciertdb\data\backup.db`, "file:H:/glaciertdb/data/backup.db?_pragma=busy_timeout%285000%29"},
		{`C:\Program Files\my db\x.db`, "file:C:/Program%20Files/my%20db/x.db?_pragma=busy_timeout%285000%29"},
		{`D:\数据\库.db`, "file:D:/%E6%95%B0%E6%8D%AE/%E5%BA%93.db?_pragma=busy_timeout%285000%29"},
	}
	for _, c := range cases {
		got := b.dsnFor(c.in)
		if got != c.want {
			t.Errorf("dsnFor(%q)\n got: %s\nwant: %s", c.in, got, c.want)
		}
	}
	// 所有输出必须能被 url.Parse 接受（ncruces 驱动对 file: 前缀先 url.Parse）
	for _, c := range cases {
		dsn := b.dsnFor(c.in)
		if _, err := url.Parse(dsn); err != nil {
			t.Errorf("dsnFor(%q) → %q url.Parse 失败: %v", c.in, dsn, err)
		}
	}
}
