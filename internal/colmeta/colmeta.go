// Package colmeta 列类型映射（v1.3.3）：系统库 glacier_meta 记录每库每表
// 「列 → MySQL 原始类型」，支撑底层 SQLite 与 MySQL 类型体系的差异抹平——
//
//   存储：DOUBLE/FLOAT/DECIMAL 一律映射为 SQLite TEXT 落盘（防 15 位浮点
//         格式化截断；入库时数字字面量自动加引号，原样保存客户端送来的每一位）；
//   读取：按映射把 TEXT 还原为 float64（DOUBLE/FLOAT，协议类型码 MYSQL_TYPE_DOUBLE）
//         或 NEWDECIMAL 文本，结果集列类型按建表时的 MySQL 类型上报；
//   反射：SHOW COLUMNS / SHOW CREATE TABLE / information_schema.COLUMNS
//         优先回放映射里的原始类型。
package colmeta

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// ColDef 一列的 MySQL 类型描述。
type ColDef struct {
	Ordinal int    // 建表顺序（1 起）
	Name    string // 列名（去引号）
	Type    string // MySQL 原始类型文本（大写规范化，如 DOUBLE PRECISION / DECIMAL(10,2) / ENUM('a','b')）
	Code    byte   // MySQL 协议类型码（MYSQL_TYPE_*）
	Nullable bool
}

// Store 列类型映射存储（glacier_meta.db）。
type Store struct {
	mu    sync.Mutex
	db    *sql.DB
	cache map[string]map[string]ColDef // db.tbl → 列映射（写路径失效）
}

func cacheKey(db, tbl string) string { return db + "\x00" + tbl }

// Open 打开系统库并确保 schema 就位。
func Open(db *sql.DB) (*Store, error) {
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS col_types (
  db TEXT NOT NULL, tbl TEXT NOT NULL, col TEXT NOT NULL,
  seq INTEGER NOT NULL, mysql_type TEXT NOT NULL, type_code INTEGER NOT NULL,
  nullable INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (db, tbl, col)
);
CREATE INDEX IF NOT EXISTS col_types_tbl ON col_types(db, tbl);`); err != nil {
		return nil, fmt.Errorf("init colmeta schema: %w", err)
	}
	return &Store{db: db, cache: map[string]map[string]ColDef{}}, nil
}

// RecordCreateTable 建表全量登记（覆盖旧记录）。
func (s *Store) RecordCreateTable(db, tbl string, cols []ColDef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.invalidate(db, tbl)
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM col_types WHERE db=? AND tbl=?`, db, tbl); err != nil {
		return err
	}
	for _, c := range cols {
		if _, err := tx.Exec(`INSERT INTO col_types(db,tbl,col,seq,mysql_type,type_code,nullable) VALUES(?,?,?,?,?,?,?)`,
			db, tbl, c.Name, c.Ordinal, c.Type, int(c.Code), b2i(c.Nullable)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RecordAddColumn ALTER TABLE ADD COLUMN 单列登记。
func (s *Store) RecordAddColumn(db, tbl string, c ColDef) error {
	if c.Name == "" || c.Type == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.invalidate(db, tbl)
	_, err := s.db.Exec(`INSERT OR REPLACE INTO col_types(db,tbl,col,seq,mysql_type,type_code,nullable) VALUES(?,?,?,?,?,?,?)`,
		db, tbl, c.Name, c.Ordinal, c.Type, int(c.Code), b2i(c.Nullable))
	return err
}

// RecordDropColumn 删列映射。
func (s *Store) RecordDropColumn(db, tbl, col string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.invalidate(db, tbl)
	_, err := s.db.Exec(`DELETE FROM col_types WHERE db=? AND tbl=? AND col=?`, db, tbl, col)
	return err
}

// RecordRenameColumn 改列名（类型不变）。
func (s *Store) RecordRenameColumn(db, tbl, old, new string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.invalidate(db, tbl)
	_, err := s.db.Exec(`UPDATE col_types SET col=? WHERE db=? AND tbl=? AND col=?`, new, db, tbl, old)
	return err
}

// RecordChangeColumn CHANGE COLUMN：改名 + 换类型。
func (s *Store) RecordChangeColumn(db, tbl, old string, c ColDef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.invalidate(db, tbl)
	_, err := s.db.Exec(`INSERT OR REPLACE INTO col_types(db,tbl,col,seq,mysql_type,type_code,nullable) VALUES(?,?,?,?,?,?,?)`,
		db, tbl, c.Name, c.Ordinal, c.Type, int(c.Code), b2i(c.Nullable))
	if err != nil {
		return err
	}
	if old != c.Name {
		_, err = s.db.Exec(`DELETE FROM col_types WHERE db=? AND tbl=? AND col=?`, db, tbl, old)
	}
	return err
}

// RecordRenameTable 表改名（含 RENAME TABLE a TO b）。
func (s *Store) RecordRenameTable(db, old, new string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.invalidate(db, "")
	_, err := s.db.Exec(`UPDATE col_types SET tbl=? WHERE db=? AND tbl=?`, new, db, old)
	return err
}

// RecordDropTable 删表映射（可多张）。
func (s *Store) RecordDropTable(db string, tbls ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.invalidate(db, "")
	for _, t := range tbls {
		if _, err := s.db.Exec(`DELETE FROM col_types WHERE db=? AND tbl=?`, db, t); err != nil {
			return err
		}
	}
	return nil
}

// CopyTableLike CREATE TABLE ... LIKE src：复制源表映射。
func (s *Store) CopyTableLike(db, src, dst string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.invalidate(db, "")
	_, err := s.db.Exec(`INSERT OR REPLACE INTO col_types(db,tbl,col,seq,mysql_type,type_code,nullable)
		SELECT db, ?, col, seq, mysql_type, type_code, nullable FROM col_types WHERE db=? AND tbl=?`, dst, db, src)
	return err
}

// TableMap 取一张表的列映射（带缓存）；表无映射时返回 nil。
func (s *Store) TableMap(db, tbl string) map[string]ColDef {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.cache[cacheKey(db, tbl)]; ok {
		return m
	}
	m := s.tableMapLocked(db, tbl)
	s.cache[cacheKey(db, tbl)] = m
	return m
}

func (s *Store) tableMapLocked(db, tbl string) map[string]ColDef {
	rows, err := s.db.Query(`SELECT col,seq,mysql_type,type_code,nullable FROM col_types WHERE db=? AND tbl=?`, db, tbl)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := map[string]ColDef{}
	n := 0
	for rows.Next() {
		var c ColDef
		var code, nullable int
		if rows.Scan(&c.Name, &c.Ordinal, &c.Type, &code, &nullable) != nil {
			continue
		}
		c.Code = byte(code)
		c.Nullable = nullable != 0
		out[strings.ToLower(c.Name)] = c
		n++
	}
	if n == 0 {
		return nil
	}
	return out
}

// invalidate 失效缓存（写路径调用；tbl 空 = 整库失效）。
func (s *Store) invalidate(db, tbl string) {
	if tbl == "" {
		for k := range s.cache {
			if strings.HasPrefix(k, db+"\x00") {
				delete(s.cache, k)
			}
		}
		return
	}
	delete(s.cache, cacheKey(db, tbl))
}

// Close 关闭底层连接池。
func (s *Store) Close() { _ = s.db.Close() }

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// MySQL 类型码 / 类别
// ---------------------------------------------------------------------------

// MySQL 协议类型码（mysql_const 摘录）。
const (
	CodeTiny       = byte(1)
	CodeShort      = byte(2)
	CodeLong       = byte(3)
	CodeFloat      = byte(4)
	CodeDouble     = byte(5)
	CodeTimestamp  = byte(7)
	CodeLongLong   = byte(8)
	CodeInt24      = byte(9)
	CodeDate       = byte(10)
	CodeTime       = byte(11)
	CodeDatetime   = byte(12)
	CodeYear       = byte(13)
	CodeVarchar    = byte(253)
	CodeBit        = byte(16)
	CodeJSON       = byte(245)
	CodeNewDecimal = byte(246)
	CodeEnum       = byte(247)
	CodeSet        = byte(248)
	CodeBlob       = byte(252)
	CodeString     = byte(254)
)

// ParseTypeCode 由 MySQL 类型词根解析协议类型码；未知返回 253（VAR_STRING）。
func ParseTypeCode(typ string) byte {
	w := TypeRoot(typ)
	switch w {
	case "TINYINT", "BOOLEAN", "BOOL":
		return CodeTiny
	case "SMALLINT":
		return CodeShort
	case "MEDIUMINT":
		return CodeInt24
	case "INT", "INTEGER":
		return CodeLong
	case "BIGINT", "SERIAL":
		return CodeLongLong
	case "FLOAT":
		return CodeFloat
	case "DOUBLE", "REAL":
		return CodeDouble
	case "DECIMAL", "NUMERIC", "DEC", "FIXED":
		return CodeNewDecimal
	case "CHAR", "BINARY":
		return CodeString
	case "VARCHAR", "VARBINARY":
		return CodeVarchar
	case "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
		"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return CodeBlob
	case "DATE":
		return CodeDate
	case "TIME":
		return CodeTime
	case "DATETIME":
		return CodeDatetime
	case "TIMESTAMP":
		return CodeTimestamp
	case "YEAR":
		return CodeYear
	case "JSON":
		return CodeJSON
	case "ENUM":
		return CodeEnum
	case "SET":
		return CodeSet
	case "BIT":
		return CodeBit
	}
	return byte(253)
}

// TypeRoot 取类型词根（去长度括号、去 UNSIGNED/ZEROFILL/PRECISION 修饰）。
// "DOUBLE PRECISION(10,2) UNSIGNED" → "DOUBLE"；"ENUM('a','b')" → "ENUM"。
func TypeRoot(typ string) string {
	s := strings.ToUpper(strings.TrimSpace(typ))
	if i := strings.IndexByte(s, '('); i >= 0 {
		s = s[:i]
	}
	// 两词类型：DOUBLE PRECISION / CHARACTER VARYING / LONG VARCHAR 等 → 取首词归一
	s = strings.TrimSpace(s)
	for _, two := range []string{"DOUBLE PRECISION", "CHARACTER VARYING", "CHAR VARYING",
		"LONG VARCHAR", "LONG VARBINARY", "NATIONAL VARCHAR", "NATIONAL CHAR", "NCHAR VARCHAR"} {
		if strings.HasPrefix(s, two) {
			s = strings.TrimSpace(strings.TrimPrefix(s, two))
			switch two {
			case "DOUBLE PRECISION":
				return "DOUBLE"
			case "LONG VARBINARY":
				return "BLOB"
			default:
				return "VARCHAR"
			}
		}
	}
	if f := strings.Fields(s); len(f) > 0 {
		s = f[0]
	}
	return s
}

// StoreText 该类型是否以 TEXT 落盘（防浮点格式化截断；查出时还原）。
func StoreText(typ string) bool {
	switch TypeRoot(typ) {
	case "DOUBLE", "REAL", "FLOAT":
		return true
	case "DECIMAL", "NUMERIC", "DEC", "FIXED":
		return true
	}
	return false
}

// RestoreFloat 该类型查出时是否需要 TEXT → float64 还原。
func RestoreFloat(typ string) bool {
	switch TypeRoot(typ) {
	case "DOUBLE", "REAL", "FLOAT":
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// DDL 解析（CREATE TABLE 列定义 / ALTER 变更）
// ---------------------------------------------------------------------------

var (
	reCreateTable = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`)
	reIdentTok    = regexp.MustCompile("(?i)^(?:`([^`]+)`|\"([^\"]+)\"|\\[([^\\]]+)\\]|([A-Za-z_\\x{4e00}-\\x{9fa5}][A-Za-z0-9_$\\x{4e00}-\\x{9fa5}]*))")
	// 列定义中会终止「类型段」的关键字
	reColStop = regexp.MustCompile(`(?i)^(NOT|NULL|DEFAULT|AUTO_INCREMENT|UNIQUE|PRIMARY|KEY|COMMENT|COLLATE|CHARACTER|REFERENCES|ON|CHECK|GENERATED|VIRTUAL|STORED|ENFORCED|INVISIBLE|SRID|WITH)\b`)
	// 表级约束起始关键字（不属于列定义）
	reTableConstraint = regexp.MustCompile(`(?i)^(PRIMARY|UNIQUE|KEY|INDEX|CONSTRAINT|FOREIGN|FULLTEXT|SPATIAL|CHECK|EXCLUDE)\b`)
)

// Ident 取语句开头的标识符，返回（去引号名, 剩余语句）。解析失败 name 为空。
func Ident(s string) (string, string) {
	s = strings.TrimLeft(s, " \t\r\n")
	m := reIdentTok.FindStringSubmatch(s)
	if m == nil {
		return "", s
	}
	for _, g := range m[1:] {
		if g != "" {
			return g, s[len(m[0]):]
		}
	}
	return "", s
}

// ParseCreateTable 解析 CREATE TABLE 的表名与列定义（MySQL 原始类型）。
// like 为 CREATE TABLE ... LIKE src 的源表（列定义复制场景）。
func ParseCreateTable(q string) (tbl string, cols []ColDef, likeSrc string, ok bool) {
	rest := reCreateTable.FindString(q)
	if rest == "" {
		return "", nil, "", false
	}
	rest = q[len(rest):]
	name, after := Ident(rest)
	if name == "" {
		return "", nil, "", false
	}
	tbl = name
	after = strings.TrimLeft(after, " \t\r\n")
	if strings.EqualFold(strings.TrimSpace(after), "") {
		return "", nil, "", false
	}
	// CREATE TABLE t LIKE src（无列体）
	if m := regexp.MustCompile(`(?is)^LIKE\s+`).FindString(after); m != "" {
		src, _ := Ident(after[len(m):])
		if src != "" {
			return tbl, nil, src, true
		}
		return "", nil, "", false
	}
	// 找到列体起 '('
	op := strings.IndexByte(after, '(')
	if op < 0 {
		return "", nil, "", false
	}
	body, _, closed := scanBalanced(after, op)
	if !closed {
		return "", nil, "", false
	}
	for i, item := range splitTopLevelComma(body) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		col, isCol := parseColumnDef(item)
		if !isCol {
			continue
		}
		col.Ordinal = i + 1
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return "", nil, "", false
	}
	return tbl, cols, "", true
}

// joinTypeParts 类型词拼接：括号段直接吸附前词（DECIMAL(8,2) 而非 DECIMAL (8,2)）。
func joinTypeParts(parts []string) string {
	var sb strings.Builder
	for i, p := range parts {
		if i > 0 && strings.HasPrefix(p, "(") {
			sb.WriteString(p)
			continue
		}
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(p)
	}
	return sb.String()
}

// parseColumnDef 解析单个列定义项；表级约束返回 false。
func parseColumnDef(item string) (ColDef, bool) {
	head := item
	if strings.HasPrefix(head, "`") || strings.HasPrefix(head, "\"") || strings.HasPrefix(head, "[") {
		// 引号包裹名：配对闭引号从第 2 个字符起找（开引号自身不算）
		q := head[0]
		closeCh := byte(']')
		if q != '[' {
			closeCh = q
		}
		rel := strings.IndexByte(head[1:], closeCh)
		if rel < 0 {
			return ColDef{}, false
		}
		end := rel + 1 // 闭引号绝对下标
		name := head[1:end]
		rest := head[end+1:]
		return buildColDef(name, rest)
	}
	// 裸名：取词
	m := reIdentTok.FindStringSubmatch(head)
	if m == nil {
		return ColDef{}, false
	}
	name := ""
	for _, g := range m[1:] {
		if g != "" {
			name = g
			break
		}
	}
	if name == "" || reTableConstraint.MatchString(strings.ToUpper(name)) {
		return ColDef{}, false
	}
	return buildColDef(name, head[len(m[0]):])
}

func buildColDef(name, rest string) (ColDef, bool) {
	rest = strings.TrimLeft(rest, " \t\r\n")
	if rest == "" {
		return ColDef{}, false
	}
	var typParts []string
	rem := rest
	depth := 0
	for {
		// 逐 token 收集类型段：直到遇到列约束关键字（顶层）
		tok := nextToken(rem)
		if tok == "" {
			break
		}
		up := strings.ToUpper(tok)
		if depth == 0 && reColStop.MatchString(up) {
			break
		}
		typParts = append(typParts, tok)
		rem = rem[len(tok):]
		// 括号段整体并入（ENUM('a','b') / DECIMAL(10,2)）
		rem = strings.TrimLeft(rem, " \t\r\n")
		if strings.HasPrefix(rem, "(") {
			par, afterPar, _ := scanBalanced(rem, 0)
			typParts = append(typParts, "("+par+")")
			rem = strings.TrimLeft(afterPar, " \t\r\n")
			depth = 0
			continue
		}
	}
	typ := joinTypeParts(typParts)
	if typ == "" {
		return ColDef{}, false
	}
	// 保留原始大小写（ENUM 值不变形；词根解析内部自行 ToUpper）
	cd := ColDef{Name: name, Type: typ, Code: ParseTypeCode(typ)}
	// NULL 性：缺省可空；显式 NOT NULL → false
	if regexp.MustCompile(`(?i)\bNOT\s+NULL\b`).MatchString(rest) || regexp.MustCompile(`(?i)\bPRIMARY\s+KEY\b`).MatchString(rest) {
		cd.Nullable = false
	} else {
		cd.Nullable = true
	}
	return cd, true
}

// nextToken 取下一个词（标识符/数字/点号）；括号前的空白已跳过。
func nextToken(s string) string {
	s = strings.TrimLeft(s, " \t\r\n")
	if s == "" {
		return ""
	}
	c := s[0]
	if c == '(' || c == ')' || c == ',' {
		return string(c)
	}
	i := 0
	for i < len(s) {
		ch := s[i]
		if ch == '_' || ch == '$' || ch == '.' ||
			(ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
			ch >= 0x80 {
			i++
			continue
		}
		break
	}
	if i == 0 {
		return ""
	}
	return s[:i]
}

// scanBalanced 从 s[op]=='(' 起扫描配对括号（引号内不计），返回（内部内容, 右括号后剩余, 是否闭合）。
func scanBalanced(s string, op int) (string, string, bool) {
	depth := 0
	var quote byte
	for i := op; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' && (quote == '\'' || quote == '"') {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[op+1 : i], s[i+1:], true
			}
		}
	}
	return s[op+1:], "", false
}

// splitTopLevelComma 顶层逗号切分（括号/引号内不切）。
func splitTopLevelComma(s string) []string {
	var out []string
	depth := 0
	var quote byte
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' && (quote == '\'' || quote == '"') {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	out = append(out, s[start:])
	return out
}
