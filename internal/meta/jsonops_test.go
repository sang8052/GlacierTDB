package meta

import "testing"

func TestRewriteJSONOps(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{
			"literal operand ->>",
			`SELECT '{"a":"x"}' ->> '$.a'`,
			`SELECT json_extract('{"a":"x"}','$.a')`,
		},
		{
			"literal operand ->",
			`SELECT '{"a":"x"}' -> '$.a'`,
			`SELECT (CASE WHEN json_type('{"a":"x"}','$.a')='text' THEN json_quote(json_extract('{"a":"x"}','$.a')) ELSE json_extract('{"a":"x"}','$.a') END)`,
		},
		{
			"column operand",
			"SELECT t.cfg->>'$.deploy_mode' FROM t",
			"SELECT json_extract(t.cfg,'$.deploy_mode') FROM t",
		},
		{
			"chained",
			"SELECT t.cfg->'$.a'->>'$.b' FROM t",
			"SELECT json_extract((CASE WHEN json_type(t.cfg,'$.a')='text' THEN json_quote(json_extract(t.cfg,'$.a')) ELSE json_extract(t.cfg,'$.a') END),'$.b') FROM t",
		},
		{
			"paren expr operand",
			"SELECT (a||b)->>'$.x' FROM t",
			"SELECT json_extract((a||b),'$.x') FROM t",
		},
		{
			"no arrow",
			"SELECT 'a->b' FROM t",
			"SELECT 'a->b' FROM t",
		},
		{
			"arrow without path literal passes through",
			"SELECT 1 -> 2",
			"SELECT 1 -> 2",
		},
	}
	for _, c := range cases {
		if got := RewriteJSONOps(c.in); got != c.want {
			t.Errorf("%s:\n  in   = %q\n  want = %q\n  got  = %q", c.name, c.in, c.want, got)
		}
	}
}
