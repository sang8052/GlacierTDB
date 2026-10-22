// Package sqlite — MySQL JSON 兼容函数的 Go 实现（v1.0.9）。
// 与 funcs.go 相同经 ncruces driver.Open 的连接初始化钩子注册到每个后端物理连接。
// SQLite 原生已有 json_extract/json_array/json_object/json_valid/json_set/
// json_replace/json_remove/json_type/json_array_length/json_quote（且与 MySQL
// 大小写不敏感同名），这里只补语义不同的五个：json_length / json_contains /
// json_keys / json_search / json_merge_patch。
package sqlite

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

// jObj 保留键插入序的 JSON 对象（encoding/json 的 map 会丢顺序）。
type jObj struct {
	keys []string
	vals map[string]any
}

func (o *jObj) get(k string) (any, bool) {
	v, ok := o.vals[k]
	return v, ok
}

func (o *jObj) set(k string, v any) {
	if _, ok := o.vals[k]; !ok {
		o.keys = append(o.keys, k)
	}
	o.vals[k] = v
}

func (o *jObj) del(k string) {
	if _, ok := o.vals[k]; !ok {
		return
	}
	delete(o.vals, k)
	for i, k2 := range o.keys {
		if k2 == k {
			o.keys = append(o.keys[:i], o.keys[i+1:]...)
			break
		}
	}
}

func (o *jObj) count() int { return len(o.keys) }

// jsonSearchRoot json_search 的搜索起点（子树值 + 规整后的路径前缀）。
type jsonSearchRoot struct {
	v    any
	path string
}

// registerJSONFuncs 在新物理连接上注册 MySQL JSON 兼容函数。
func registerJSONFuncs(c *sqlite3.Conn) error {
	reg := func(name string, nArg int, fn sqlite3.ScalarFunction) error {
		return c.CreateFunction(name, nArg, sqlite3.DETERMINISTIC, fn)
	}
	must := func(errs ...error) error {
		for _, e := range errs {
			if e != nil {
				return e
			}
		}
		return nil
	}
	return must(
		// json_length(json[, path])：数组元素数 / 对象键数 / 标量 1；
		// 非法 JSON 或路径未命中 → NULL
		reg("json_length", -1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if len(arg) < 1 || len(arg) > 2 || anyNull(arg) {
				ctx.ResultNull()
				return
			}
			v, ok := jsonParseText(arg[0].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			if len(arg) == 2 {
				v, ok = jsonPathGet(v, arg[1].Text())
				if !ok {
					ctx.ResultNull()
					return
				}
			}
			switch t := v.(type) {
			case *jObj:
				ctx.ResultInt64(int64(t.count()))
			case []any:
				ctx.ResultInt64(int64(len(t)))
			default:
				ctx.ResultInt64(1)
			}
		}),
		// json_contains(target, candidate[, path])：MySQL 子集包含语义；
		// 返回 1/0，路径未命中 → 0，参数非法 → NULL
		reg("json_contains", -1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if len(arg) < 2 || len(arg) > 3 || anyNull(arg) {
				ctx.ResultNull()
				return
			}
			t, ok := jsonParseText(arg[0].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			if len(arg) == 3 {
				tv, ok := jsonPathGet(t, arg[2].Text())
				if !ok {
					ctx.ResultInt64(0) // 路径未命中 → 0
					return
				}
				t = tv
			}
			cd, ok := jsonParseText(arg[1].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			if jsonContainsVal(t, cd) {
				ctx.ResultInt64(1)
			} else {
				ctx.ResultInt64(0)
			}
		}),
		// json_keys(json[, path])：对象键名 JSON 数组（按插入序）；
		// 非对象 / 路径未命中 / 非法 JSON → NULL
		reg("json_keys", -1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if len(arg) < 1 || len(arg) > 2 || anyNull(arg) {
				ctx.ResultNull()
				return
			}
			v, ok := jsonParseText(arg[0].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			if len(arg) == 2 {
				v, ok = jsonPathGet(v, arg[1].Text())
				if !ok {
					ctx.ResultNull()
					return
				}
			}
			o, ok := v.(*jObj)
			if !ok {
				ctx.ResultNull()
				return
			}
			keys := o.keys
			if keys == nil {
				keys = []string{}
			}
			b, err := json.Marshal(keys)
			if err != nil {
				ctx.ResultNull()
				return
			}
			ctx.ResultText(string(b))
		}),
		// json_search(json, one_or_all, search_str[, escape_char[, path] ...])：
		// 在 JSON 字符串值里搜子串（大小写不敏感），支持 % / _ 通配符，
		// escape_char 缺省反斜杠、显式 NULL/空串表示不转义；
		// one → 首个路径字符串或 NULL，all → 路径 JSON 数组或 NULL
		reg("json_search", -1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if len(arg) < 3 {
				ctx.ResultNull()
				return
			}
			for i := 0; i < 3; i++ { // 必需参数 NULL → NULL
				if arg[i].Type() == sqlite3.NULL {
					ctx.ResultNull()
					return
				}
			}
			doc, ok := jsonParseText(arg[0].Text())
			if !ok {
				ctx.ResultNull()
				return
			}
			mode := strings.ToLower(strings.TrimSpace(arg[1].Text()))
			if mode != "one" && mode != "all" {
				ctx.ResultNull()
				return
			}
			esc, hasEsc := rune('\\'), true // 缺省转义符反斜杠
			if len(arg) > 3 {
				if arg[3].Type() == sqlite3.NULL {
					hasEsc = false
				} else if s := arg[3].Text(); s == "" {
					hasEsc = false
				} else {
					esc, hasEsc = []rune(s)[0], true
				}
			}
			var roots []jsonSearchRoot
			if len(arg) > 4 {
				for i := 4; i < len(arg); i++ {
					if arg[i].Type() == sqlite3.NULL {
						ctx.ResultNull()
						return
					}
					p := arg[i].Text()
					v, ok := jsonPathGet(doc, p)
					if !ok {
						continue // 路径未命中：跳过该路径
					}
					roots = append(roots, jsonSearchRoot{v, jsonPathNorm(p)})
				}
			} else {
				roots = append(roots, jsonSearchRoot{doc, "$"})
			}
			pat := arg[2].Text()
			var paths []string
			stop := false
			var walk func(v any, path string)
			walk = func(v any, path string) {
				if stop {
					return
				}
				switch t := v.(type) {
				case *jObj: // 只搜字符串值，不搜键名
					for _, k := range t.keys {
						walk(t.vals[k], path+"."+jsonQuoteKey(k))
					}
				case []any:
					for i, e := range t {
						walk(e, path+"["+strconv.Itoa(i)+"]")
					}
				case string:
					if jsonWildMatchSub(t, pat, esc, hasEsc) {
						paths = append(paths, path)
						if mode == "one" {
							stop = true
						}
					}
				}
			}
			for _, r := range roots {
				walk(r.v, r.path)
				if stop {
					break
				}
			}
			if len(paths) == 0 {
				ctx.ResultNull()
				return
			}
			if mode == "one" {
				ctx.ResultText(paths[0])
				return
			}
			b, err := json.Marshal(paths)
			if err != nil {
				ctx.ResultNull()
				return
			}
			ctx.ResultText(string(b))
		}),
		// json_merge_patch(a, b[, c ...])：RFC 7386 递归合并（patch 为 null 删键），
		// 两个及以上参数依次合并；非法 JSON 按原文本标量处理
		reg("json_merge_patch", -1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if len(arg) < 2 || anyNull(arg) {
				ctx.ResultNull()
				return
			}
			v := jsonParseOrScalar(arg[0].Text())
			for i := 1; i < len(arg); i++ {
				v = jsonMergePatch(v, jsonParseOrScalar(arg[i].Text()))
			}
			var sb strings.Builder
			jsonWriteValue(&sb, v)
			ctx.ResultText(sb.String())
		}),
	)
}

// jsonParseText 严格解析 JSON 文本（保留数字原文与键序），尾随垃圾视为非法。
func jsonParseText(s string) (any, bool) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	v, err := jsonParseValue(dec)
	if err != nil {
		return nil, false
	}
	if dec.More() {
		return nil, false
	}
	return v, true
}

// jsonParseValue 递归解析一个 JSON 值（对象→*jObj，数组→[]any，数字→json.Number）。
func jsonParseValue(dec *json.Decoder) (any, error) {
	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, nil // JSON null
	}
	switch tt := t.(type) {
	case json.Delim:
		switch tt {
		case '{':
			o := &jObj{vals: map[string]any{}}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, ok := kt.(string)
				if !ok {
					return nil, fmt.Errorf("JSON 对象键非法")
				}
				v, err := jsonParseValue(dec)
				if err != nil {
					return nil, err
				}
				o.set(key, v)
			}
			if _, err := dec.Token(); err != nil { // 消费 '}'
				return nil, err
			}
			return o, nil
		case '[':
			arr := []any{}
			for dec.More() {
				v, err := jsonParseValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			if _, err := dec.Token(); err != nil { // 消费 ']'
				return nil, err
			}
			return arr, nil
		default:
			return nil, fmt.Errorf("JSON 定界符非法")
		}
	case json.Number:
		return tt, nil
	case string:
		return tt, nil
	case bool:
		return tt, nil
	default:
		return nil, fmt.Errorf("JSON token 非法")
	}
}

// jsonParseOrScalar 解析 JSON；失败按原文本标量（JSON 字符串）处理，供 merge_patch 用。
func jsonParseOrScalar(s string) any {
	if v, ok := jsonParseText(s); ok {
		return v
	}
	return s
}

// jsonWriteValue 输出 JSON 文本（保持对象键序与数字原文）。
func jsonWriteValue(sb *strings.Builder, v any) {
	switch t := v.(type) {
	case *jObj:
		sb.WriteByte('{')
		for i, k := range t.keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(jsonQuoteKey(k))
			sb.WriteByte(':')
			jsonWriteValue(sb, t.vals[k])
		}
		sb.WriteByte('}')
	case []any:
		sb.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				sb.WriteByte(',')
			}
			jsonWriteValue(sb, e)
		}
		sb.WriteByte(']')
	case string:
		sb.WriteString(jsonQuoteKey(t))
	case json.Number:
		sb.WriteString(t.String())
	case bool:
		if t {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	default:
		sb.WriteString("null")
	}
}

// jsonQuoteKey 字符串 → JSON 字符串字面量（含引号转义），键名/路径片段共用。
func jsonQuoteKey(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// jsonMergePatch 按 RFC 7386 合并：patch 非对象则整体替换，对象则递归合并、null 删键。
func jsonMergePatch(target, patch any) any {
	po, ok := patch.(*jObj)
	if !ok {
		return patch
	}
	out := &jObj{vals: map[string]any{}}
	if to, ok := target.(*jObj); ok {
		out.keys = append(out.keys, to.keys...)
		for k, v := range to.vals {
			out.vals[k] = v
		}
	}
	for _, k := range po.keys {
		pv := po.vals[k]
		if pv == nil {
			out.del(k) // patch 值为 null → 删除键
			continue
		}
		tv, _ := out.get(k)
		out.set(k, jsonMergePatch(tv, pv))
	}
	return out
}

// jsonContainsVal 递归判断 target 是否包含 candidate（MySQL JSON_CONTAINS 语义）。
func jsonContainsVal(target, cand any) bool {
	switch c := cand.(type) {
	case *jObj:
		t, ok := target.(*jObj)
		if !ok {
			return false
		}
		for _, k := range c.keys {
			tv, ok := t.get(k)
			if !ok {
				return false
			}
			if !jsonContainsVal(tv, c.vals[k]) {
				return false
			}
		}
		return true
	case []any:
		t, ok := target.([]any)
		if !ok {
			return false
		}
		for _, ce := range c {
			found := false
			for _, te := range t {
				if jsonContainsVal(te, ce) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default: // 标量 candidate（含 null）
		if t, ok := target.([]any); ok {
			for _, te := range t {
				if jsonContainsVal(te, cand) {
					return true
				}
			}
			return false
		}
		return jsonScalarEqual(target, cand)
	}
}

// jsonScalarEqual 标量相等（字符串比较大小写不敏感，数字按数值比较）。
func jsonScalarEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && strings.EqualFold(av, bv)
	case json.Number:
		bv, ok := b.(json.Number)
		if !ok {
			return false
		}
		if ai, err := strconv.ParseInt(av.String(), 10, 64); err == nil {
			if bi, err2 := strconv.ParseInt(bv.String(), 10, 64); err2 == nil {
				return ai == bi
			}
		}
		fa, ea := av.Float64()
		fb, eb := bv.Float64()
		return ea == nil && eb == nil && fa == fb
	default:
		return false
	}
}

// jsonPathStep 单步路径：对象键或数组下标。
type jsonPathStep struct {
	key  string
	idx  int
	isIx bool
}

// jsonPathSteps 解析 $、$.a.b、$.a[0]、$."a b"[1]、$["a"] 形式路径。
func jsonPathSteps(p string) ([]jsonPathStep, bool) {
	p = strings.TrimSpace(p)
	if p == "" {
		p = "$"
	}
	if p[0] != '$' {
		return nil, false
	}
	var steps []jsonPathStep
	i := 1
	for i < len(p) {
		switch p[i] {
		case '.':
			i++
			var name string
			if i < len(p) && p[i] == '"' {
				j := strings.IndexByte(p[i+1:], '"')
				if j < 0 {
					return nil, false
				}
				name = p[i+1 : i+1+j]
				i += 2 + j
			} else {
				j := i
				for j < len(p) && p[j] != '.' && p[j] != '[' {
					j++
				}
				name = p[i:j]
				i = j
			}
			if name == "" {
				return nil, false
			}
			steps = append(steps, jsonPathStep{key: name})
		case '[':
			i++
			if i < len(p) && p[i] == '"' {
				j := strings.IndexByte(p[i+1:], '"')
				if j < 0 {
					return nil, false
				}
				name := p[i+1 : i+1+j]
				i += 2 + j
				if i >= len(p) || p[i] != ']' {
					return nil, false
				}
				i++
				steps = append(steps, jsonPathStep{key: name})
			} else {
				j := i
				for j < len(p) && p[j] != ']' {
					j++
				}
				if j >= len(p) {
					return nil, false
				}
				n, err := strconv.Atoi(strings.TrimSpace(p[i:j]))
				if err != nil {
					return nil, false
				}
				i = j + 1
				steps = append(steps, jsonPathStep{idx: n, isIx: true})
			}
		default:
			return nil, false
		}
	}
	return steps, true
}

// jsonPathGet 按简单路径取值；路径非法或未命中返回 false。
func jsonPathGet(root any, p string) (any, bool) {
	steps, ok := jsonPathSteps(p)
	if !ok {
		return nil, false
	}
	cur := root
	for _, st := range steps {
		if st.isIx {
			arr, ok := cur.([]any)
			if !ok || st.idx < 0 || st.idx >= len(arr) {
				return nil, false
			}
			cur = arr[st.idx]
		} else {
			o, ok := cur.(*jObj)
			if !ok {
				return nil, false
			}
			v, ok := o.get(st.key)
			if !ok {
				return nil, false
			}
			cur = v
		}
	}
	return cur, true
}

// jsonPathNorm 把路径规整为规范形式（键名统一加双引号）。
func jsonPathNorm(p string) string {
	steps, ok := jsonPathSteps(p)
	if !ok {
		return p
	}
	var sb strings.Builder
	sb.WriteByte('$')
	for _, st := range steps {
		if st.isIx {
			sb.WriteByte('[')
			sb.WriteString(strconv.Itoa(st.idx))
			sb.WriteByte(']')
		} else {
			sb.WriteByte('.')
			sb.WriteString(jsonQuoteKey(st.key))
		}
	}
	return sb.String()
}

// jsonWildMatchSub 通配符子串匹配：% 任意串、_ 单字符，esc 转义下个字符，
// 大小写不敏感；模式只需匹配字符串值的一段连续子串。
func jsonWildMatchSub(s, pattern string, esc rune, hasEsc bool) bool {
	sr := []rune(strings.ToLower(s))
	pr := []rune(strings.ToLower(pattern))
	var lesc rune
	if hasEsc {
		if r := []rune(strings.ToLower(string(esc))); len(r) > 0 {
			lesc = r[0]
		}
	}
	// 词法：0=字面 1=% 2=_
	type wtok struct {
		kind int
		r    rune
	}
	var toks []wtok
	for i := 0; i < len(pr); i++ {
		c := pr[i]
		if hasEsc && c == lesc {
			i++
			if i < len(pr) {
				toks = append(toks, wtok{0, pr[i]})
			} else {
				toks = append(toks, wtok{0, c}) // 尾部孤立转义按字面
			}
			continue
		}
		switch c {
		case '%':
			toks = append(toks, wtok{1, 0})
		case '_':
			toks = append(toks, wtok{2, 0})
		default:
			toks = append(toks, wtok{0, c})
		}
	}
	var m func(si, ti int) bool
	m = func(si, ti int) bool {
		if ti == len(toks) {
			return true // 模式整体匹配完即命中（子串语义）
		}
		t := toks[ti]
		switch t.kind {
		case 0:
			return si < len(sr) && sr[si] == t.r && m(si+1, ti+1)
		case 2: // _
			return si < len(sr) && m(si+1, ti+1)
		default: // %
			for k := si; k <= len(sr); k++ {
				if m(k, ti+1) {
					return true
				}
			}
			return false
		}
	}
	for si := 0; si <= len(sr); si++ {
		if m(si, 0) {
			return true
		}
	}
	return false
}
