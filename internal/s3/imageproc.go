// Package s3 — OpenCV 风格图片预处理管道（v1.3.7）。
//
// 操作表达式 x-gimg-ops：'|' 分隔的管道，每步 name(arg,...)，命名对齐 OpenCV：
//
//	rectangle(x,y,w,h,R,G,B[,thickness])   cv2.rectangle（thickness=-1 填充）
//	circle(cx,cy,r,R,G,B[,thickness])      cv2.circle
//	line(x1,y1,x2,y2,R,G,B[,thickness])    cv2.line
//	text('label',x,y,R,G,B[,scale])        cv2.putText（引号内文本可含逗号；中文需配置字体）
//	grayscale                              cv2.cvtColor(BGR2GRAY)
//	blur(ksize)                            cv2.blur 均值滤波
//	rotate(90|180|270)                     cv2.rotate
//	flip(0|1|-1)                           cv2.flip（0 垂直 / 1 水平 / -1 双向）
//
// 执行顺序：crop → ops → resize → format（绘制坐标基于裁剪后画布）。
// 文本渲染：默认内置 bitmap 字体（ASCII）；配置 s3.image_font 指向 ttf 时
// 经 golang.org/x/image/font/opentype 加载，支持中文等 TrueType 字形。
package s3

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/math/fixed"

	"glaciertdb/internal/logx"
)

// ---------------------------------------------------------------------------
// 操作解析
// ---------------------------------------------------------------------------

type imgOp struct {
	name string
	args []string
}

// parseOps 解析操作管道（'|' 分隔；text 的文本参数用单引号包裹可含逗号）。
func parseOps(s string) ([]imgOp, error) {
	var ops []imgOp
	for _, seg := range strings.Split(s, "|") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		open := strings.IndexByte(seg, '(')
		if open < 0 || !strings.HasSuffix(seg, ")") {
			return nil, fmt.Errorf("bad op %q (want name(args))", seg)
		}
		name := strings.ToLower(strings.TrimSpace(seg[:open]))
		argstr := seg[open+1 : len(seg)-1]
		if _, known := opFuncs[name]; !known {
			return nil, fmt.Errorf("unknown image op %q", name)
		}
		ops = append(ops, imgOp{name: name, args: splitOpArgs(argstr)})
	}
	return ops, nil
}

// splitOpArgs 顶层逗号切分；单引号段内不切。
func splitOpArgs(s string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\'':
			inQuote = !inQuote
		case c == ',' && !inQuote:
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	if strings.TrimSpace(cur.String()) != "" || len(out) > 0 {
		out = append(out, strings.TrimSpace(cur.String()))
	}
	return out
}

func atoiArg(args []string, i int, def int) int {
	if i >= len(args) {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(args[i]))
	if err != nil {
		return def
	}
	return n
}

// ---------------------------------------------------------------------------
// 操作实现（opFuncs 注册表）
// ---------------------------------------------------------------------------

var opFuncs = map[string]func(img image.Image, args []string) (image.Image, error){}

func init() {
	opFuncs["rectangle"] = opRectangle
	opFuncs["circle"] = opCircle
	opFuncs["line"] = opLine
	opFuncs["text"] = opText
	opFuncs["grayscale"] = opGrayscale
	opFuncs["blur"] = opBlur
	opFuncs["rotate"] = opRotate
	opFuncs["flip"] = opFlip
}

// toRGBA 通用画布转换（绘制操作前置）。
func toRGBA(src image.Image) *image.RGBA {
	if r, ok := src.(*image.RGBA); ok {
		return r
	}
	out := image.NewRGBA(src.Bounds())
	draw.Draw(out, out.Bounds(), src, src.Bounds().Min, draw.Src)
	return out
}

func colorArg(args []string, i int) (color.RGBA, error) {
	if i+2 >= len(args) {
		return color.RGBA{}, fmt.Errorf("color needs R,G,B args")
	}
	var c color.RGBA
	c.A = 255
	fmt.Sscanf(strings.TrimSpace(args[i]), "%d", &c.R)
	fmt.Sscanf(strings.TrimSpace(args[i+1]), "%d", &c.G)
	fmt.Sscanf(strings.TrimSpace(args[i+2]), "%d", &c.B)
	return c, nil
}

func opRectangle(src image.Image, args []string) (image.Image, error) {
	if len(args) < 4 {
		return nil, fmt.Errorf("rectangle needs x,y,w,h[,R,G,B[,thickness]]")
	}
	x, y := atoiArg(args, 0, 0), atoiArg(args, 1, 0)
	w, h := atoiArg(args, 2, 0), atoiArg(args, 3, 0)
	if w <= 0 || h <= 0 {
		return nil, fmt.Errorf("rectangle w/h must be > 0")
	}
	c := color.RGBA{255, 255, 0, 255} // OpenCV 默认黄框对齐
	if len(args) >= 7 {
		var err error
		if c, err = colorArg(args, 4); err != nil {
			return nil, err
		}
	}
	thick := 2
	if len(args) >= 8 {
		thick = atoiArg(args, 7, 2)
	}
	img := toRGBA(src)
	b := img.Bounds()
	x0, y0 := b.Min.X+x, b.Min.Y+y
	x1, y1 := x0+w, y0+h
	if thick < 0 { // FILLED
		for yy := y0; yy < y1; yy++ {
			for xx := x0; xx < x1; xx++ {
				img.Set(xx, yy, c)
			}
		}
		return img, nil
	}
	// 四边描线（对齐 cv2.rectangle 线宽语义）
	for t := 0; t < thick; t++ {
		for xx := x0; xx <= x1; xx++ {
			img.Set(xx, y0+t, c)
			img.Set(xx, y1-t, c)
		}
		for yy := y0; yy <= y1; yy++ {
			img.Set(x0+t, yy, c)
			img.Set(x1-t, yy, c)
		}
	}
	return img, nil
}

func opCircle(src image.Image, args []string) (image.Image, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("circle needs cx,cy,r[,R,G,B[,thickness]]")
	}
	cx, cy, r := atoiArg(args, 0, 0), atoiArg(args, 1, 0), atoiArg(args, 2, 0)
	if r <= 0 {
		return nil, fmt.Errorf("circle r must be > 0")
	}
	c := color.RGBA{255, 0, 0, 255}
	if len(args) >= 6 {
		var err error
		if c, err = colorArg(args, 3); err != nil {
			return nil, err
		}
	}
	thick := 2
	if len(args) >= 7 {
		thick = atoiArg(args, 6, 2)
	}
	img := toRGBA(src)
	fill := thick < 0
	rr := float64(r)
	if fill {
		for yy := -r; yy <= r; yy++ {
			for xx := -r; xx <= r; xx++ {
				if float64(xx*xx+yy*yy) <= rr*rr {
					img.Set(cx+xx, cy+yy, c)
				}
			}
		}
		return img, nil
	}
	// 中点圆算法描边（按线宽向内外各扩）
	for t := 0; t < thick; t++ {
		rr := float64(r) - float64(t)/2
		rrOut := rr + float64(t)/2
		prev := math.NaN()
		for deg := 0.0; deg <= 360.0; deg += 0.25 {
			rad := deg * math.Pi / 180
			for ri := rr; ri <= rrOut; ri += 0.5 {
				img.Set(cx+int(math.Round(ri*math.Cos(rad))), cy+int(math.Round(ri*math.Sin(rad))), c)
			}
			_ = prev
		}
	}
	return img, nil
}

func opLine(src image.Image, args []string) (image.Image, error) {
	if len(args) < 4 {
		return nil, fmt.Errorf("line needs x1,y1,x2,y2[,R,G,B[,thickness]]")
	}
	x1, y1 := atoiArg(args, 0, 0), atoiArg(args, 1, 0)
	x2, y2 := atoiArg(args, 2, 0), atoiArg(args, 3, 0)
	c := color.RGBA{0, 0, 255, 255}
	if len(args) >= 7 {
		var err error
		if c, err = colorArg(args, 4); err != nil {
			return nil, err
		}
	}
	thick := 2
	if len(args) >= 8 {
		thick = atoiArg(args, 7, 2)
	}
	img := toRGBA(src)
	// Bresenham 直线（线宽垂直扩展）
	dx, dy := abs(x2-x1), abs(y2-y1)
	sx, sy := 1, 1
	if x1 > x2 {
		sx = -1
	}
	if y1 > y2 {
		sy = -1
	}
	err := dx - dy
	expand := thick / 2
	for {
		for ex := -expand; ex <= expand; ex++ {
			for ey := -expand; ey <= expand; ey++ {
				img.Set(x1+ex, y1+ey, c)
			}
		}
		if x1 == x2 && y1 == y2 {
			break
		}
		e2 := 2 * err
		if e2 > -dy {
			err -= dy
			x1 += sx
		}
		if e2 < dx {
			err += dx
			y1 += sy
		}
	}
	return img, nil
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// ---------------------------------------------------------------------------
// 文本渲染（cv2.putText 等价）：默认 bitmap ASCII；配置 ttf 后支持中文
// ---------------------------------------------------------------------------

var (
	fontMu    sync.Mutex
	ttfFont   font.Face // s3.image_font 指定的 TrueType 字体（惰性加载单例）
	ttfLoaded bool
	ttfFailed string
)

// loadImageFont 加载 ttf 字体（size 16 基准，scale 参数按倍率重建）。
func loadImageFont(path string, size float64) (font.Face, error) {
	if path == "" {
		return font.Face(basicFontFace()), nil
	}
	fontMu.Lock()
	defer fontMu.Unlock()
	if ttfFailed == path {
		return nil, fmt.Errorf("font %s failed to load before", path)
	}
	if !ttfLoaded {
		data, err := readFileAll(path)
		if err != nil {
			ttfFailed = path
			return nil, fmt.Errorf("load s3.image_font %s: %w", path, err)
		}
		f, err := opentype.Parse(data)
		if err != nil {
			ttfFailed = path
			return nil, fmt.Errorf("parse s3.image_font %s: %w", path, err)
		}
		ttfBase = f
		ttfLoaded = true
		logx.Infof("s3: image font loaded: %s", path)
	}
	if ttfBase == nil {
		return font.Face(basicFontFace()), nil
	}
	return opentype.NewFace(ttfBase, &opentype.FaceOptions{
		Size: size * 16, DPI: 72, Hinting: font.HintingFull,
	})
}

var ttfBase *opentype.Font

func opText(src image.Image, args []string) (image.Image, error) {
	if len(args) < 4 {
		return nil, fmt.Errorf("text needs 'str',x,y[,R,G,B[,scale]]")
	}
	txt := strings.Trim(args[0], "'")
	x, y := atoiArg(args, 1, 0), atoiArg(args, 2, 0)
	c := color.RGBA{255, 255, 255, 255}
	if len(args) >= 7 {
		var err error
		if c, err = colorArg(args, 3); err != nil {
			return nil, err
		}
	}
	scale := 1.0
	if len(args) >= 8 {
		if v, err := strconv.ParseFloat(strings.TrimSpace(args[7]), 64); err == nil && v > 0 {
			scale = v
		}
	}
	img := toRGBA(src)
	face, err := loadImageFont(currentImageFont(), scale)
	if err != nil {
		return nil, err
	}
	d := &font.Drawer{
		Dst: img,
		Src: image.NewUniform(c),
		Face: face,
		Dot: fixed.P(x, y),
	}
	d.DrawString(txt)
	return img, nil
}

// ---------------------------------------------------------------------------
// 滤镜与几何
// ---------------------------------------------------------------------------

func opGrayscale(src image.Image, _ []string) (image.Image, error) {
	b := src.Bounds()
	img := toRGBA(src)
	out := image.NewRGBA(b)
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			r, g, bl, a := img.At(x, y).RGBA()
			gray := uint32(0.299*float64(r>>8) + 0.587*float64(g>>8) + 0.114*float64(bl>>8))
			if gray > 255 {
				gray = 255
			}
			out.SetRGBA(x, y, color.RGBA{uint8(gray), uint8(gray), uint8(gray), uint8(a >> 8)})
		}
	}
	return out, nil
}

func opBlur(src image.Image, args []string) (image.Image, error) {
	k := atoiArg(args, 0, 3)
	if k < 1 || k > 51 || k%2 == 0 {
		return nil, fmt.Errorf("blur ksize must be odd 1..51")
	}
	b := src.Bounds()
	img := toRGBA(src)
	out := image.NewRGBA(b)
	half := k / 2
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			var sr, sg, sb, sa, n uint64
			for dy := -half; dy <= half; dy++ {
				for dx := -half; dx <= half; dx++ {
					xx, yy := x+dx, y+dy
					if xx < b.Min.X || xx >= b.Max.X || yy < b.Min.Y || yy >= b.Max.Y {
						continue
					}
					r, g, bl, a := img.At(xx, yy).RGBA()
					sr += uint64(r >> 8)
					sg += uint64(g >> 8)
					sb += uint64(bl >> 8)
					sa += uint64(a >> 8)
					n++
				}
			}
			if n == 0 {
				n = 1
			}
			out.SetRGBA(x, y, color.RGBA{uint8(sr / n), uint8(sg / n), uint8(sb / n), uint8(sa / n)})
		}
	}
	return out, nil
}

func opRotate(src image.Image, args []string) (image.Image, error) {
	deg := atoiArg(args, 0, 90)
	switch ((deg % 360) + 360) % 360 {
	case 90:
		b := src.Bounds()
		w, h := b.Dx(), b.Dy()
		out := image.NewRGBA(image.Rect(0, 0, h, w))
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				out.Set(h-1-y, x, src.At(b.Min.X+x, b.Min.Y+y))
			}
		}
		return out, nil
	case 180:
		b := src.Bounds()
		w, h := b.Dx(), b.Dy()
		out := image.NewRGBA(image.Rect(0, 0, w, h))
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				out.Set(w-1-x, h-1-y, src.At(b.Min.X+x, b.Min.Y+y))
			}
		}
		return out, nil
	case 270:
		b := src.Bounds()
		w, h := b.Dx(), b.Dy()
		out := image.NewRGBA(image.Rect(0, 0, h, w))
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				out.Set(y, w-1-x, src.At(b.Min.X+x, b.Min.Y+y))
			}
		}
		return out, nil
	case 0:
		return src, nil
	}
	return nil, fmt.Errorf("rotate supports 90/180/270 only")
}

func opFlip(src image.Image, args []string) (image.Image, error) {
	code := atoiArg(args, 0, 1)
	b := src.Bounds()
	w, h := b.Dx(), b.Dy()
	out := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			sx, sy := x, y
			switch code {
			case 0: // 垂直（上下翻转）
				sy = h - 1 - y
			case 1: // 水平（左右翻转）
				sx = w - 1 - x
			case -1: // 双向
				sx, sy = w-1-x, h-1-y
			default:
				return nil, fmt.Errorf("flip code must be 0|1|-1")
			}
			out.Set(x, y, src.At(b.Min.X+sx, b.Min.Y+sy))
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 管道执行（image.go processImage 调用）
// ---------------------------------------------------------------------------

// applyOps 依序执行操作管道。
func applyOps(src image.Image, ops []imgOp) (image.Image, error) {
	var err error
	for _, op := range ops {
		fn := opFuncs[op.name]
		if fn == nil {
			return nil, fmt.Errorf("unknown image op %q", op.name)
		}
		if src, err = fn(src, op.args); err != nil {
			return nil, err
		}
	}
	return src, nil
}

// encodeImage 按格式编码（jpeg 默认 quality 85，由 imgParams.Quality 控制）。
func encodeImage(img image.Image, format string, quality int) ([]byte, string, error) {
	var buf bytes.Buffer
	switch format {
	case "jpeg", "jpg":
		if quality <= 0 || quality > 100 {
			quality = 85
		}
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
			return nil, "", err
		}
		return buf.Bytes(), "image/jpeg", nil
	default: // png
		if err := png.Encode(&buf, img); err != nil {
			return nil, "", err
		}
		return buf.Bytes(), "image/png", nil
	}
}

// ---------------------------------------------------------------------------
// 字体与配置接线
// ---------------------------------------------------------------------------

// imageFontPath s3.image_font 配置（Server 启动时注入；空 = 内置 ASCII 字体）。
var imageFontPath string

// SetImageFont 设置 ttf 字体路径（Server 启动时调用）。
func SetImageFont(path string) { imageFontPath = path }

// currentImageFont 当前生效字体路径。
func currentImageFont() string { return imageFontPath }

// basicFontFace 内置 7x13 bitmap 字体（ASCII，无外部依赖）。
func basicFontFace() font.Face { return basicfont.Face7x13 }

// readFileAll 读文件（字体加载用）。
func readFileAll(path string) ([]byte, error) { return os.ReadFile(path) }
