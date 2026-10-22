// Package sqlite — MySQL/PostGIS 风格空间函数的 Go 实现（v1.0.9）。
// 与 funcs.go 相同经 ncruces driver.Open 的连接初始化钩子注册到每个后端物理连接。
// 本项目 GEOMETRY 列存规范 WKT TEXT；输入支持 WKT 文本 / GeoJSON 文本 / WKB 二进制
// 三种格式自动识别，解析失败一律返回 NULL（不报错）。
package sqlite

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"strings"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

// 几何类型。
const (
	geoPointType = 1 + iota // 点
	geoLineType             // 线（顶点串）
	geoPolyType             // 面（多环，首环外环）
)

// geoPoint 平面点（X=经度/横轴，Y=纬度/纵轴）。
type geoPoint struct {
	X, Y float64
}

// geoGeom 小型几何模型：点 / 线 / 面。
type geoGeom struct {
	kind  int
	pt    geoPoint
	line  []geoPoint
	rings [][]geoPoint
}

// geoBBox 包围盒。
type geoBBox struct {
	minX, minY, maxX, maxY float64
}

// registerGeoFuncs 在新物理连接上注册空间函数。
func registerGeoFuncs(c *sqlite3.Conn) error {
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
		// st_astext(geom) → 规范 WKT 文本
		reg("st_astext", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			g, ok := parseGeomValue(arg[0])
			if !ok {
				ctx.ResultNull()
				return
			}
			ctx.ResultText(g.wkt())
		}),
		// st_geomfromtext(wkt) → 规范 WKT 文本（本项目 GEOMETRY 存 WKT）
		reg("st_geomfromtext", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			g, ok := parseWKT(strings.TrimSpace(arg[0].Text()))
			if !ok {
				ctx.ResultNull()
				return
			}
			ctx.ResultText(g.wkt())
		}),
		// st_asgeojson(geom) → GeoJSON 文本
		reg("st_asgeojson", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			g, ok := parseGeomValue(arg[0])
			if !ok {
				ctx.ResultNull()
				return
			}
			ctx.ResultText(g.geojson())
		}),
		// st_geomfromgeojson(gj) → 规范 WKT 文本
		reg("st_geomfromgeojson", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			g, ok := parseGeoJSON(strings.TrimSpace(arg[0].Text()))
			if !ok {
				ctx.ResultNull()
				return
			}
			ctx.ResultText(g.wkt())
		}),
		// st_makepoint(x, y) → POINT(x y)，数值去尾零
		reg("st_makepoint", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			ctx.ResultText("POINT(" + fmtFloat(arg[0].Float()) + " " + fmtFloat(arg[1].Float()) + ")")
		}),
		// st_x / st_longitude → X 坐标；点取自身、线取首点、面取首环首点
		reg("st_x", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			geoCoordResult(ctx, arg, true)
		}),
		reg("st_longitude", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			geoCoordResult(ctx, arg, true)
		}),
		// st_y / st_latitude → Y 坐标
		reg("st_y", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			geoCoordResult(ctx, arg, false)
		}),
		reg("st_latitude", 1, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			geoCoordResult(ctx, arg, false)
		}),
		// st_distance(a, b) → 平面欧氏距离（点-点 / 点-线 / 点-面），其他组合 NULL
		reg("st_distance", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			a, oka := parseGeomValue(arg[0])
			b, okb := parseGeomValue(arg[1])
			if !oka || !okb {
				ctx.ResultNull()
				return
			}
			d, ok := geoDistance(a, b)
			if !ok {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(d)
		}),
		// st_distance_sphere(a, b) → 球面距离（米，haversine，R=6371000）
		reg("st_distance_sphere", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			a, oka := parseGeomValue(arg[0])
			b, okb := parseGeomValue(arg[1])
			if !oka || !okb {
				ctx.ResultNull()
				return
			}
			d, ok := geoDistanceSphere(a, b)
			if !ok {
				ctx.ResultNull()
				return
			}
			ctx.ResultFloat(d)
		}),
		// st_within(a, b) → a 落在 b 上/内为 1，其他组合 0
		reg("st_within", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			geoWithinResult(ctx, arg)
		}),
		// st_contains(a, b) = st_within(b, a)
		reg("st_contains", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			a, oka := parseGeomValue(arg[0])
			b, okb := parseGeomValue(arg[1])
			if !oka || !okb {
				ctx.ResultNull()
				return
			}
			ctx.ResultInt64(b2i(geoWithin(b, a)))
		}),
		// st_intersects(a, b) → 包围盒相交 + 具体判定
		reg("st_intersects", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			a, oka := parseGeomValue(arg[0])
			b, okb := parseGeomValue(arg[1])
			if !oka || !okb {
				ctx.ResultNull()
				return
			}
			ctx.ResultInt64(b2i(geoIntersects(a, b)))
		}),
		// st_equals(a, b) → 规范 WKT 文本相等为 1
		reg("st_equals", 2, func(ctx sqlite3.Context, arg ...sqlite3.Value) {
			if anyNull(arg) {
				ctx.ResultNull()
				return
			}
			a, oka := parseGeomValue(arg[0])
			b, okb := parseGeomValue(arg[1])
			if !oka || !okb {
				ctx.ResultNull()
				return
			}
			ctx.ResultInt64(b2i(a.wkt() == b.wkt()))
		}),
	)
}

// b2i bool → 0/1。
func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// geoCoordResult st_x/st_y/st_longitude/st_latitude 统一入口。
func geoCoordResult(ctx sqlite3.Context, arg []sqlite3.Value, wantX bool) {
	if anyNull(arg) {
		ctx.ResultNull()
		return
	}
	g, ok := parseGeomValue(arg[0])
	if !ok {
		ctx.ResultNull()
		return
	}
	p, ok := g.firstPoint()
	if !ok {
		ctx.ResultNull()
		return
	}
	if wantX {
		ctx.ResultFloat(p.X)
	} else {
		ctx.ResultFloat(p.Y)
	}
}

// geoWithinResult st_within 统一入口。
func geoWithinResult(ctx sqlite3.Context, arg []sqlite3.Value) {
	if anyNull(arg) {
		ctx.ResultNull()
		return
	}
	a, oka := parseGeomValue(arg[0])
	b, okb := parseGeomValue(arg[1])
	if !oka || !okb {
		ctx.ResultNull()
		return
	}
	ctx.ResultInt64(b2i(geoWithin(a, b)))
}

// parseGeomValue 三种输入自动识别：BLOB → WKB；文本 → GeoJSON（以 { 开头）或 WKT。
func parseGeomValue(v sqlite3.Value) (*geoGeom, bool) {
	if v.Type() == sqlite3.BLOB {
		return parseWKB(v.RawBlob())
	}
	s := strings.TrimSpace(v.Text())
	if s == "" {
		return nil, false
	}
	if s[0] == '{' {
		return parseGeoJSON(s)
	}
	if g, ok := parseWKT(s); ok {
		return g, true
	}
	// 兜底：文本里藏 WKB 的罕见情况
	return parseWKB(v.RawBlob())
}

// fmtFloat 数值格式化（去尾零、非科学计数）。
func fmtFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// wkt 规范 WKT 文本。
func (g *geoGeom) wkt() string {
	var sb strings.Builder
	switch g.kind {
	case geoPointType:
		sb.WriteString("POINT(")
		writeWktPoint(&sb, g.pt)
		sb.WriteByte(')')
	case geoLineType:
		sb.WriteString("LINESTRING(")
		writeWktPoints(&sb, g.line)
		sb.WriteByte(')')
	case geoPolyType:
		sb.WriteString("POLYGON(")
		for i, ring := range g.rings {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteByte('(')
			writeWktPoints(&sb, ring)
			sb.WriteByte(')')
		}
		sb.WriteByte(')')
	}
	return sb.String()
}

// geojson 规范 GeoJSON 文本。
func (g *geoGeom) geojson() string {
	var sb strings.Builder
	switch g.kind {
	case geoPointType:
		sb.WriteString(`{"type":"Point","coordinates":`)
		writeGjCoord(&sb, g.pt)
		sb.WriteByte('}')
	case geoLineType:
		sb.WriteString(`{"type":"LineString","coordinates":[`)
		for i, p := range g.line {
			if i > 0 {
				sb.WriteByte(',')
			}
			writeGjCoord(&sb, p)
		}
		sb.WriteString("]}")
	case geoPolyType:
		sb.WriteString(`{"type":"Polygon","coordinates":[`)
		for i, ring := range g.rings {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('[')
			for j, p := range ring {
				if j > 0 {
					sb.WriteByte(',')
				}
				writeGjCoord(&sb, p)
			}
			sb.WriteByte(']')
		}
		sb.WriteString("]}")
	}
	return sb.String()
}

func writeWktPoint(sb *strings.Builder, p geoPoint) {
	sb.WriteString(fmtFloat(p.X))
	sb.WriteByte(' ')
	sb.WriteString(fmtFloat(p.Y))
}

func writeWktPoints(sb *strings.Builder, pts []geoPoint) {
	for i, p := range pts {
		if i > 0 {
			sb.WriteString(", ")
		}
		writeWktPoint(sb, p)
	}
}

func writeGjCoord(sb *strings.Builder, p geoPoint) {
	sb.WriteByte('[')
	sb.WriteString(fmtFloat(p.X))
	sb.WriteByte(',')
	sb.WriteString(fmtFloat(p.Y))
	sb.WriteByte(']')
}

// firstPoint 代表点：点取自身、线取首点、面取首环首点。
func (g *geoGeom) firstPoint() (geoPoint, bool) {
	switch g.kind {
	case geoPointType:
		return g.pt, true
	case geoLineType:
		if len(g.line) > 0 {
			return g.line[0], true
		}
	case geoPolyType:
		if len(g.rings) > 0 && len(g.rings[0]) > 0 {
			return g.rings[0][0], true
		}
	}
	return geoPoint{}, false
}

// vertices 顶点集（球面距离近似用）。
func (g *geoGeom) vertices() []geoPoint {
	switch g.kind {
	case geoPointType:
		return []geoPoint{g.pt}
	case geoLineType:
		return g.line
	case geoPolyType:
		var pts []geoPoint
		for _, ring := range g.rings {
			pts = append(pts, ring...)
		}
		return pts
	}
	return nil
}

// bbox 包围盒。
func (g *geoGeom) bbox() geoBBox {
	bb := geoBBox{minX: math.Inf(1), minY: math.Inf(1), maxX: math.Inf(-1), maxY: math.Inf(-1)}
	for _, p := range g.vertices() {
		bb.minX = math.Min(bb.minX, p.X)
		bb.minY = math.Min(bb.minY, p.Y)
		bb.maxX = math.Max(bb.maxX, p.X)
		bb.maxY = math.Max(bb.maxY, p.Y)
	}
	return bb
}

// wktScanner WKT 词法扫描器。
type wktScanner struct {
	s string
	i int
}

func (w *wktScanner) skipSpace() {
	for w.i < len(w.s) && (w.s[w.i] == ' ' || w.s[w.i] == '\t' || w.s[w.i] == '\n' || w.s[w.i] == '\r') {
		w.i++
	}
}

// take 跳过空白后消费指定字符。
func (w *wktScanner) take(c byte) bool {
	w.skipSpace()
	if w.i < len(w.s) && w.s[w.i] == c {
		w.i++
		return true
	}
	return false
}

// num 读取一个浮点数（支持负号 / 小数 / 科学计数）。
func (w *wktScanner) num() (float64, bool) {
	w.skipSpace()
	start := w.i
	for w.i < len(w.s) {
		c := w.s[w.i]
		if (c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' {
			w.i++
		} else {
			break
		}
	}
	if w.i == start {
		return 0, false
	}
	v, err := strconv.ParseFloat(w.s[start:w.i], 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// point 读取 "x y" 点。
func (w *wktScanner) point() (geoPoint, bool) {
	x, ok := w.num()
	if !ok {
		return geoPoint{}, false
	}
	y, ok := w.num()
	if !ok {
		return geoPoint{}, false
	}
	return geoPoint{X: x, Y: y}, true
}

// points 读取逗号分隔的点序列（括号外）。
func (w *wktScanner) points() ([]geoPoint, bool) {
	var pts []geoPoint
	for {
		p, ok := w.point()
		if !ok {
			return nil, false
		}
		pts = append(pts, p)
		if !w.take(',') {
			return pts, true
		}
	}
}

// parseWKT 解析 POINT / LINESTRING / POLYGON WKT（大小写不敏感、允许多余空白）。
func parseWKT(s string) (*geoGeom, bool) {
	w := &wktScanner{s: s}
	w.skipSpace()
	start := w.i
	for w.i < len(w.s) && ((w.s[w.i] >= 'A' && w.s[w.i] <= 'Z') || (w.s[w.i] >= 'a' && w.s[w.i] <= 'z')) {
		w.i++
	}
	name := strings.ToUpper(s[start:w.i])
	if name == "" {
		return nil, false
	}
	switch name {
	case "POINT":
		if !w.take('(') {
			return nil, false
		}
		p, ok := w.point()
		if !ok {
			return nil, false
		}
		if !w.take(')') {
			return nil, false
		}
		w.skipSpace()
		if w.i != len(s) {
			return nil, false
		}
		return &geoGeom{kind: geoPointType, pt: p}, true
	case "LINESTRING":
		if !w.take('(') {
			return nil, false
		}
		pts, ok := w.points()
		if !ok {
			return nil, false
		}
		if !w.take(')') {
			return nil, false
		}
		w.skipSpace()
		if w.i != len(s) {
			return nil, false
		}
		return &geoGeom{kind: geoLineType, line: pts}, true
	case "POLYGON":
		if !w.take('(') {
			return nil, false
		}
		var rings [][]geoPoint
		for {
			if !w.take('(') {
				return nil, false
			}
			pts, ok := w.points()
			if !ok {
				return nil, false
			}
			if !w.take(')') {
				return nil, false
			}
			rings = append(rings, pts)
			if !w.take(',') {
				break
			}
		}
		if !w.take(')') {
			return nil, false
		}
		w.skipSpace()
		if w.i != len(s) {
			return nil, false
		}
		return &geoGeom{kind: geoPolyType, rings: rings}, true
	default:
		return nil, false
	}
}

// parseGeoJSON 解析 Point / LineString / Polygon GeoJSON。
func parseGeoJSON(s string) (*geoGeom, bool) {
	var head struct {
		Type        string          `json:"type"`
		Coordinates json.RawMessage `json:"coordinates"`
	}
	if err := json.Unmarshal([]byte(s), &head); err != nil {
		return nil, false
	}
	switch strings.ToLower(head.Type) {
	case "point":
		var c []float64
		if err := json.Unmarshal(head.Coordinates, &c); err != nil || len(c) < 2 {
			return nil, false
		}
		return &geoGeom{kind: geoPointType, pt: geoPoint{X: c[0], Y: c[1]}}, true
	case "linestring":
		var c [][]float64
		if err := json.Unmarshal(head.Coordinates, &c); err != nil || len(c) == 0 {
			return nil, false
		}
		pts, ok := gjPoints(c)
		if !ok {
			return nil, false
		}
		return &geoGeom{kind: geoLineType, line: pts}, true
	case "polygon":
		var c [][][]float64
		if err := json.Unmarshal(head.Coordinates, &c); err != nil || len(c) == 0 {
			return nil, false
		}
		var rings [][]geoPoint
		for _, r := range c {
			pts, ok := gjPoints(r)
			if !ok {
				return nil, false
			}
			rings = append(rings, pts)
		}
		return &geoGeom{kind: geoPolyType, rings: rings}, true
	default:
		return nil, false
	}
}

// gjPoints GeoJSON 坐标对数组 → 点串。
func gjPoints(c [][]float64) ([]geoPoint, bool) {
	pts := make([]geoPoint, 0, len(c))
	for _, xy := range c {
		if len(xy) < 2 {
			return nil, false
		}
		pts = append(pts, geoPoint{X: xy[0], Y: xy[1]})
	}
	return pts, true
}

// wkbReader 顺序读取 WKB 字节。
type wkbReader struct {
	b      []byte
	i      int
	little bool
}

// byteOrder 读取字节序头（0 大端 / 1 小端）。
func (r *wkbReader) byteOrder() bool {
	if len(r.b) < 1 {
		return false
	}
	switch r.b[0] {
	case 0:
		r.little = false
	case 1:
		r.little = true
	default:
		return false
	}
	r.i = 1
	return true
}

// u32 读取 4 字节无符号整数。
func (r *wkbReader) u32() (uint32, bool) {
	if r.i+4 > len(r.b) {
		return 0, false
	}
	var v uint32
	if r.little {
		v = binary.LittleEndian.Uint32(r.b[r.i:])
	} else {
		v = binary.BigEndian.Uint32(r.b[r.i:])
	}
	r.i += 4
	return v, true
}

// f64 读取 8 字节浮点数。
func (r *wkbReader) f64() (float64, bool) {
	if r.i+8 > len(r.b) {
		return 0, false
	}
	var v uint64
	if r.little {
		v = binary.LittleEndian.Uint64(r.b[r.i:])
	} else {
		v = binary.BigEndian.Uint64(r.b[r.i:])
	}
	r.i += 8
	return math.Float64frombits(v), true
}

// point 读取一对 double。
func (r *wkbReader) point() (geoPoint, bool) {
	x, ok := r.f64()
	if !ok {
		return geoPoint{}, false
	}
	y, ok := r.f64()
	if !ok {
		return geoPoint{}, false
	}
	return geoPoint{X: x, Y: y}, true
}

// points 读取带数量前缀的点串。
func (r *wkbReader) points() ([]geoPoint, bool) {
	n, ok := r.u32()
	if !ok {
		return nil, false
	}
	if uint64(n)*16 > uint64(len(r.b)-r.i) { // 防御非法超大计数
		return nil, false
	}
	pts := make([]geoPoint, 0, int(n))
	for i := uint32(0); i < n; i++ {
		p, ok := r.point()
		if !ok {
			return nil, false
		}
		pts = append(pts, p)
	}
	return pts, true
}

// parseWKB 解析标准 WKB（小端 / 大端，type 1/2/3）。
func parseWKB(b []byte) (*geoGeom, bool) {
	r := &wkbReader{b: b}
	if !r.byteOrder() {
		return nil, false
	}
	typ, ok := r.u32()
	if !ok {
		return nil, false
	}
	switch typ {
	case 1: // Point
		p, ok := r.point()
		if !ok {
			return nil, false
		}
		return &geoGeom{kind: geoPointType, pt: p}, true
	case 2: // LineString
		pts, ok := r.points()
		if !ok {
			return nil, false
		}
		return &geoGeom{kind: geoLineType, line: pts}, true
	case 3: // Polygon
		n, ok := r.u32()
		if !ok {
			return nil, false
		}
		var rings [][]geoPoint
		for i := uint32(0); i < n; i++ {
			pts, ok := r.points()
			if !ok {
				return nil, false
			}
			rings = append(rings, pts)
		}
		return &geoGeom{kind: geoPolyType, rings: rings}, true
	default:
		return nil, false
	}
}

// pointSegDist 点到线段最短距离。
func pointSegDist(p, a, b geoPoint) float64 {
	dx, dy := b.X-a.X, b.Y-a.Y
	if dx == 0 && dy == 0 {
		return math.Hypot(p.X-a.X, p.Y-a.Y)
	}
	t := ((p.X-a.X)*dx + (p.Y-a.Y)*dy) / (dx*dx + dy*dy)
	if t < 0 {
		t = 0
	} else if t > 1 {
		t = 1
	}
	return math.Hypot(p.X-(a.X+t*dx), p.Y-(a.Y+t*dy))
}

// pointOnSeg 点是否在线段上（容差 1e-12）。
func pointOnSeg(p, a, b geoPoint) bool {
	return pointSegDist(p, a, b) <= 1e-12
}

// pointOnLine 点是否在折线上（容差 1e-12）。
func pointOnLine(p geoPoint, line []geoPoint) bool {
	if len(line) == 1 {
		return math.Hypot(p.X-line[0].X, p.Y-line[0].Y) <= 1e-12
	}
	for i := 0; i+1 < len(line); i++ {
		if pointOnSeg(p, line[i], line[i+1]) {
			return true
		}
	}
	return false
}

// pointInPoly 射线法判点在面内（含边界算在内；多环按奇偶规则扣洞）。
func pointInPoly(p geoPoint, g *geoGeom) bool {
	inside := false
	for _, ring := range g.rings {
		n := len(ring)
		if n == 0 {
			continue
		}
		for i := 0; i < n; i++ {
			a := ring[i]
			b := ring[(i+1)%n]
			if pointOnSeg(p, a, b) {
				return true // 边界上算在内
			}
			if (a.Y > p.Y) != (b.Y > p.Y) { // 竖直射线计交点
				x := a.X + (p.Y-a.Y)*(b.X-a.X)/(b.Y-a.Y)
				if x > p.X {
					inside = !inside
				}
			}
		}
	}
	return inside
}

// pointLineDist 点到折线最短距离。
func pointLineDist(p geoPoint, line []geoPoint) float64 {
	if len(line) == 1 {
		return math.Hypot(p.X-line[0].X, p.Y-line[0].Y)
	}
	d := math.Inf(1)
	for i := 0; i+1 < len(line); i++ {
		if dd := pointSegDist(p, line[i], line[i+1]); dd < d {
			d = dd
		}
	}
	return d
}

// pointPolyBoundDist 点到面边界（全部环）最短距离。
func pointPolyBoundDist(p geoPoint, g *geoGeom) float64 {
	d := math.Inf(1)
	for _, ring := range g.rings {
		n := len(ring)
		if n == 0 {
			continue
		}
		for i := 0; i < n; i++ {
			if dd := pointSegDist(p, ring[i], ring[(i+1)%n]); dd < d {
				d = dd
			}
		}
	}
	return d
}

// geoWithin within(a,b)：a 为点且落在 b（点/线/面）上或内；其他组合恒 0。
func geoWithin(a, b *geoGeom) bool {
	if a.kind != geoPointType {
		return false
	}
	switch b.kind {
	case geoPointType:
		return math.Hypot(a.pt.X-b.pt.X, a.pt.Y-b.pt.Y) <= 1e-12
	case geoLineType:
		return pointOnLine(a.pt, b.line)
	case geoPolyType:
		return pointInPoly(a.pt, b)
	}
	return false
}

// geoDistance 平面欧氏距离：点-点 / 点-线 / 点-面（面内为 0）；其他组合不支持。
func geoDistance(a, b *geoGeom) (float64, bool) {
	if a.kind == geoPointType && b.kind == geoPointType {
		return math.Hypot(a.pt.X-b.pt.X, a.pt.Y-b.pt.Y), true
	}
	if a.kind != geoPointType && b.kind == geoPointType {
		a, b = b, a // 归一为 a=点
	}
	if a.kind == geoPointType {
		switch b.kind {
		case geoLineType:
			return pointLineDist(a.pt, b.line), true
		case geoPolyType:
			if pointInPoly(a.pt, b) {
				return 0, true
			}
			return pointPolyBoundDist(a.pt, b), true
		}
	}
	return 0, false
}

// geoDistanceSphere 球面距离（米，haversine，地球半径 6371000）：
// 点-点精确，其他组合用顶点间最短距离近似。
func geoDistanceSphere(a, b *geoGeom) (float64, bool) {
	va, vb := a.vertices(), b.vertices()
	if len(va) == 0 || len(vb) == 0 {
		return 0, false
	}
	d := math.Inf(1)
	for _, pa := range va {
		for _, pb := range vb {
			if dd := haversine(pa, pb); dd < d {
				d = dd
			}
		}
	}
	return d, true
}

// haversine 两点球面距离（米）。
func haversine(a, b geoPoint) float64 {
	const earthR = 6371000.0
	lat1 := a.Y * math.Pi / 180
	lat2 := b.Y * math.Pi / 180
	dLat := lat2 - lat1
	dLon := (b.X - a.X) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	if h > 1 {
		h = 1
	}
	return 2 * earthR * math.Asin(math.Sqrt(h))
}

// cross 叉积 (b-a)×(c-a)。
func cross(a, b, c geoPoint) float64 {
	return (b.X-a.X)*(c.Y-a.Y) - (b.Y-a.Y)*(c.X-a.X)
}

// onSeg 共线点 c 是否落在线段 ab 包围盒内。
func onSeg(a, b, c geoPoint) bool {
	return c.X <= math.Max(a.X, b.X) && c.X >= math.Min(a.X, b.X) &&
		c.Y <= math.Max(a.Y, b.Y) && c.Y >= math.Min(a.Y, b.Y)
}

// segIntersect 两线段是否相交（含端点接触 / 共线重叠）。
func segIntersect(p1, p2, p3, p4 geoPoint) bool {
	d1 := cross(p3, p4, p1)
	d2 := cross(p3, p4, p2)
	d3 := cross(p1, p2, p3)
	d4 := cross(p1, p2, p4)
	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}
	if d1 == 0 && onSeg(p3, p4, p1) {
		return true
	}
	if d2 == 0 && onSeg(p3, p4, p2) {
		return true
	}
	if d3 == 0 && onSeg(p1, p2, p3) {
		return true
	}
	if d4 == 0 && onSeg(p1, p2, p4) {
		return true
	}
	return false
}

// bboxHit 包围盒是否相交（含接触）。
func bboxHit(a, b *geoGeom) bool {
	ba, bb := a.bbox(), b.bbox()
	return ba.minX <= bb.maxX && ba.maxX >= bb.minX &&
		ba.minY <= bb.maxY && ba.maxY >= bb.minY
}

// geoIntersects 判定两几何是否相交：包围盒预筛 + 具体判定。
func geoIntersects(a, b *geoGeom) bool {
	if !bboxHit(a, b) {
		return false
	}
	switch {
	case a.kind == geoPointType && b.kind == geoPointType:
		return math.Hypot(a.pt.X-b.pt.X, a.pt.Y-b.pt.Y) <= 1e-12
	case a.kind == geoPointType && b.kind == geoLineType:
		return pointOnLine(a.pt, b.line)
	case a.kind == geoLineType && b.kind == geoPointType:
		return pointOnLine(b.pt, a.line)
	case a.kind == geoPointType && b.kind == geoPolyType:
		return pointInPoly(a.pt, b)
	case a.kind == geoPolyType && b.kind == geoPointType:
		return pointInPoly(b.pt, a)
	case a.kind == geoLineType && b.kind == geoLineType:
		return lineLineHit(a.line, b.line)
	case a.kind == geoLineType && b.kind == geoPolyType:
		return linePolyHit(a.line, b)
	case a.kind == geoPolyType && b.kind == geoLineType:
		return linePolyHit(b.line, a)
	case a.kind == geoPolyType && b.kind == geoPolyType:
		return polyPolyHit(a, b)
	}
	return false
}

// lineLineHit 两折线是否有线段相交（含退化单点线）。
func lineLineHit(la, lb []geoPoint) bool {
	if len(la) == 1 {
		return pointOnLine(la[0], lb)
	}
	if len(lb) == 1 {
		return pointOnLine(lb[0], la)
	}
	for i := 0; i+1 < len(la); i++ {
		for j := 0; j+1 < len(lb); j++ {
			if segIntersect(la[i], la[i+1], lb[j], lb[j+1]) {
				return true
			}
		}
	}
	return false
}

// linePolyHit 线与面：线顶点在面内，或线段与面边界相交。
func linePolyHit(line []geoPoint, g *geoGeom) bool {
	for _, p := range line {
		if pointInPoly(p, g) {
			return true
		}
	}
	for _, ring := range g.rings {
		n := len(ring)
		if n == 0 {
			continue
		}
		for i := 0; i < n; i++ {
			for j := 0; j+1 < len(line); j++ {
				if segIntersect(line[j], line[j+1], ring[i], ring[(i+1)%n]) {
					return true
				}
			}
		}
	}
	return false
}

// polyPolyHit 面-面：顶点互含或边界相交。
func polyPolyHit(a, b *geoGeom) bool {
	for _, p := range a.vertices() {
		if pointInPoly(p, b) {
			return true
		}
	}
	for _, p := range b.vertices() {
		if pointInPoly(p, a) {
			return true
		}
	}
	for _, ra := range a.rings {
		for _, rb := range b.rings {
			if ringRingHit(ra, rb) {
				return true
			}
		}
	}
	return false
}

// ringRingHit 两环边界是否相交。
func ringRingHit(ra, rb []geoPoint) bool {
	na, nb := len(ra), len(rb)
	if na == 0 || nb == 0 {
		return false
	}
	for i := 0; i < na; i++ {
		for j := 0; j < nb; j++ {
			if segIntersect(ra[i], ra[(i+1)%na], rb[j], rb[(j+1)%nb]) {
				return true
			}
		}
	}
	return false
}
