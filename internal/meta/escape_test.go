package meta

import "testing"

func TestUnescapeLiterals(t *testing.T) {
	cases := []struct{ name, in, want string }{
		// BUG.txt 主场景：pymysql 参数化写入 JSON（驱动层转义后的 SQL 文本）
		{
			"double-quote escapes",
			`INSERT INTO t_bug VALUES (1, '{\"a\": \"b\"}')`,
			`INSERT INTO t_bug VALUES (1, '{"a": "b"}')`,
		},
		{
			"double-quote escapes in WHERE",
			`SELECT * FROM t WHERE cfg = '{\"a\": \"b\"}' AND id = 1`,
			`SELECT * FROM t WHERE cfg = '{"a": "b"}' AND id = 1`,
		},
		// 单引号转义：\' 解码为 ' 后按 SQLite 规则重编码为 ''
		{
			"escaped single quote",
			`SELECT 'it\'s'`,
			`SELECT 'it''s'`,
		},
		// 反斜杠转义：\\ → \
		{
			"escaped backslash",
			`INSERT INTO t VALUES ('C:\\path\\to\\file')`,
			`INSERT INTO t VALUES ('C:\path\to\file')`,
		},
		// \n → 换行
		{
			"newline escape",
			"SELECT 'line1\\nline2'",
			"SELECT 'line1\nline2'",
		},
		// LIKE 通配符字面量保留两字符
		{
			"LIKE wildcard literals preserved",
			`SELECT * FROM t WHERE name LIKE '100\%'`,
			`SELECT * FROM t WHERE name LIKE '100\%'`,
		},
		// 无反斜杠：原样
		{
			"no backslash untouched",
			`SELECT 'abc', 'it''s' FROM t WHERE x = 'd''e'`,
			`SELECT 'abc', 'it''s' FROM t WHERE x = 'd''e'`,
		},
		// 字面量外的反斜杠不动
		{
			"backslash outside literal untouched",
			`SELECT a\b FROM t`,
			`SELECT a\b FROM t`,
		},
		// 反引号/双引号段内的单引号不误伤
		{
			"apostrophe inside double-quoted ident",
			`SELECT "it's" FROM t WHERE c = 'x'`,
			`SELECT "it's" FROM t WHERE c = 'x'`,
		},
		{
			"backtick segment untouched",
			"SELECT `we'ird` FROM t WHERE c = 'a\\nb'",
			"SELECT `we'ird` FROM t WHERE c = 'a\nb'",
		},
		// 行注释中的引号不参与扫描
		{
			"hash comment with apostrophe",
			`SELECT 1 # don't care` + "\n" + `FROM t WHERE c = 'a\nb'`,
			`SELECT 1 # don't care` + "\n" + `FROM t WHERE c = 'a` + "\n" + `b'`,
		},
		{
			"double-dash comment with apostrophe",
			"SELECT 1 -- don't\nFROM t WHERE c = 'x'",
			"SELECT 1 -- don't\nFROM t WHERE c = 'x'",
		},
		// x--1 仍是表达式（不当作注释），且不影响后续字面量
		{
			"minus-minus expression not comment",
			"SELECT 5--3, 'a\\tb'",
			"SELECT 5--3, 'a\tb'",
		},
		// 未知转义：丢反斜杠
		{
			"unknown escape drops backslash",
			`SELECT 'a\qb'`,
			`SELECT 'aqb'`,
		},
		// 未闭合字面量：整段原样
		{
			"unterminated literal untouched",
			`SELECT 'abc`,
			`SELECT 'abc`,
		},
		// 混合：'' 转义引号 + \" + \\ 共存
		{
			"mixed escapes",
			`UPDATE t SET cfg = '{"name": "it''s \"x\"", "path": "C:\\dir"}' WHERE id = 1`,
			`UPDATE t SET cfg = '{"name": "it''s "x"", "path": "C:\dir"}' WHERE id = 1`,
		},
	}
	for _, c := range cases {
		if got := UnescapeLiterals(c.in); got != c.want {
			t.Errorf("%s:\n  in   = %q\n  want = %q\n  got  = %q", c.name, c.in, c.want, got)
		}
	}
}
