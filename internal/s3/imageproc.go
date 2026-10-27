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
//	blend('key'[,x,y[,alpha[,cx,cy,cw,ch]]])  裁剪融合（v1.3.11）：取同桶对象 key
//	                                       的图像，可选裁剪 (cx,cy,cw,ch) 后以权重
//	                                       alpha 贴入当前位置（≈cv2.addWeighted）
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
	"runtime"
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
// 裸名（如 grayscale，无括号）视为零参调用 —— 与文档写法兼容，不再静默忽略。
func parseOps(s string) ([]imgOp, error) {
	var ops []imgOp
	for _, seg := range strings.Split(s, "|") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		open := strings.IndexByte(seg, '(')
		name, argstr := "", ""
		if open < 0 {
			name = strings.ToLower(strings.TrimSpace(seg)) // 裸名 ≡ name()
		} else {
			if !strings.HasSuffix(seg, ")") {
				return nil, fmt.Errorf("bad op %q (want name(args))", seg)
			}
			name = strings.ToLower(strings.TrimSpace(seg[:open]))
			argstr = seg[open+1 : len(seg)-1]
		}
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

// imgResolver 同桶图像解析器：blend 操作按对象 key 取第二张图（v1.3.11）。
// 只暴露 key（无 bucket 维度）——融合源恒限定在当前请求的来源桶内。
type imgResolver func(key string) (image.Image, error)

var opFuncs = map[string]func(img image.Image, args []string, res imgResolver) (image.Image, error){}

func init() {
	opFuncs["rectangle"] = opRectangle
	opFuncs["circle"] = opCircle
	opFuncs["line"] = opLine
	opFuncs["text"] = opText
	opFuncs["grayscale"] = opGrayscale
	opFuncs["blur"] = opBlur
	opFuncs["rotate"] = opRotate
	opFuncs["flip"] = opFlip
	opFuncs["blend"] = opBlend
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

// parallelBands 把 [0,total) 切成至多 8 个带并行执行（小任务直接单线程）。
// 图像像素级处理的公共并行骨架（v1.3.10 提速：多核摊平逐像素循环）。
func parallelBands(total int, fn func(lo, hi int)) {
	if total <= 0 {
		return
	}
	procs := runtime.GOMAXPROCS(0)
	if procs > 8 {
		procs = 8
	}
	if procs <= 1 || total < 1<<20 { // <1M 像素：并行开销不划算
		fn(0, total)
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < procs; i++ {
		lo := total * i / procs
		hi := total * (i + 1) / procs
		if lo >= hi {
			continue
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			fn(lo, hi)
		}(lo, hi)
	}
	wg.Wait()
}

// setRGBAFast 越界保护的 RGBA 像素直写（替代 image.Set 接口调用）。
func setRGBAFast(img *image.RGBA, x, y int, c color.RGBA) {
	b := img.Bounds()
	if x < b.Min.X || x >= b.Max.X || y < b.Min.Y || y >= b.Max.Y {
		return
	}
	off := img.PixOffset(x, y)
	img.Pix[off] = c.R
	img.Pix[off+1] = c.G
	img.Pix[off+2] = c.B
	img.Pix[off+3] = c.A
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

func opRectangle(src image.Image, args []string, _ imgResolver) (image.Image, error) {
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
		for yy := maxInt(y0, b.Min.Y); yy < minInt(y1, b.Max.Y); yy++ {
			for xx := maxInt(x0, b.Min.X); xx < minInt(x1, b.Max.X); xx++ {
				setRGBAFast(img, xx, yy, c)
			}
		}
		return img, nil
	}
	// 四边描线（对齐 cv2.rectangle 线宽语义）
	for t := 0; t < thick; t++ {
		for xx := x0; xx <= x1; xx++ {
			setRGBAFast(img, xx, y0+t, c)
			setRGBAFast(img, xx, y1-t, c)
		}
		for yy := y0; yy <= y1; yy++ {
			setRGBAFast(img, x0+t, yy, c)
			setRGBAFast(img, x1-t, yy, c)
		}
	}
	return img, nil
}

func opCircle(src image.Image, args []string, _ imgResolver) (image.Image, error) {
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
					setRGBAFast(img, cx+xx, cy+yy, c)
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
				setRGBAFast(img, cx+int(math.Round(ri*math.Cos(rad))), cy+int(math.Round(ri*math.Sin(rad))), c)
			}
		}
		_ = prev
	}
	return img, nil
}

func opLine(src image.Image, args []string, _ imgResolver) (image.Image, error) {
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
				setRGBAFast(img, x1+ex, y1+ey, c)
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
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

func opText(src image.Image, args []string, _ imgResolver) (image.Image, error) {
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

func opGrayscale(src image.Image, _ []string, _ imgResolver) (image.Image, error) {
	b := src.Bounds()
	img := toRGBA(src)
	out := image.NewRGBA(b)
	w := b.Dx()
	// v1.3.10 提速：Pix 直写 + 行带并行（原逐像素 At/Set 接口调用，大图慢一个数量级）
	parallelBands(b.Dy(), func(lo, hi int) {
		for row := lo; row < hi; row++ {
			so := img.PixOffset(b.Min.X, b.Min.Y+row)
			do := out.PixOffset(b.Min.X, b.Min.Y+row)
			for col := 0; col < w; col++ {
				r := float64(img.Pix[so])
				g := float64(img.Pix[so+1])
				bl := float64(img.Pix[so+2])
				gray := uint32(0.299*r + 0.587*g + 0.114*bl)
				if gray > 255 {
					gray = 255
				}
				out.Pix[do] = uint8(gray)
				out.Pix[do+1] = uint8(gray)
				out.Pix[do+2] = uint8(gray)
				out.Pix[do+3] = img.Pix[so+3]
				so += 4
				do += 4
			}
		}
	})
	return out, nil
}

// opBlur 均值滤波（cv2.blur）：可分离两遍滑动窗口（水平 → 垂直），每像素 O(1)
// 而非 O(k²)；边界窗口仅计有效像素（与原逐点实现语义一致）。中间结果用
// 16.16 定点避免两遍整除的舍入漂移（与精确二维均值误差 ≤ 1 LSB）。
func opBlur(src image.Image, args []string, _ imgResolver) (image.Image, error) {
	k := atoiArg(args, 0, 3)
	if k < 1 || k > 51 || k%2 == 0 {
		return nil, fmt.Errorf("blur ksize must be odd 1..51")
	}
	b := src.Bounds()
	w, h := b.Dx(), b.Dy()
	img := toRGBA(src)
	half := k / 2

	// 第一遍（水平）：tmp[通道] = round(sum * 65536 / nx)（255*51*65536 ≈ 8.5e8，uint32 可容）
	tmp := make([]uint32, w*h*4)
	parallelBands(h, func(lo, hi int) {
		for row := lo; row < hi; row++ {
			base := img.PixOffset(b.Min.X, b.Min.Y+row)
			trow := tmp[row*w*4:]
			var sr, sg, sb, sa uint64
			l, r := 0, minInt(half, w-1) // x=0 的窗口 [0, half]
			for x := 0; x <= r; x++ {
				sr += uint64(img.Pix[base+x*4])
				sg += uint64(img.Pix[base+x*4+1])
				sb += uint64(img.Pix[base+x*4+2])
				sa += uint64(img.Pix[base+x*4+3])
			}
			for x := 0; x < w; x++ {
				nx := uint64(r - l + 1)
				o := x * 4
				trow[o] = uint32((sr*65536 + nx/2) / nx)
				trow[o+1] = uint32((sg*65536 + nx/2) / nx)
				trow[o+2] = uint32((sb*65536 + nx/2) / nx)
				trow[o+3] = uint32((sa*65536 + nx/2) / nx)
				if r+1 < w { // 右端纳入新列
					r++
					sr += uint64(img.Pix[base+r*4])
					sg += uint64(img.Pix[base+r*4+1])
					sb += uint64(img.Pix[base+r*4+2])
					sa += uint64(img.Pix[base+r*4+3])
				}
				if x >= half && l < x-half+1 { // 左端移出旧列
					sr -= uint64(img.Pix[base+l*4])
					sg -= uint64(img.Pix[base+l*4+1])
					sb -= uint64(img.Pix[base+l*4+2])
					sa -= uint64(img.Pix[base+l*4+3])
					l++
				}
			}
		}
	})

	// 第二遍（垂直）：列带并行，行窗口滑动累加，round(sum/ny) 后右移 16 位
	out := image.NewRGBA(image.Rect(0, 0, w, h))
	rowBytes := w * 4
	parallelBands(w, func(clo, chi int) {
		nb := (chi - clo) * 4
		acc := make([]uint64, nb)
		t, bt := 0, minInt(half, h-1) // y=0 的窗口 [0, half]
		for i := 0; i <= bt; i++ {
			trow := tmp[i*rowBytes:]
			for j := 0; j < nb; j++ {
				acc[j] += uint64(trow[clo*4+j])
			}
		}
		for y := 0; y < h; y++ {
			ny := uint64(bt - t + 1)
			drow := out.Pix[y*rowBytes:]
			for j := 0; j < nb; j++ {
				drow[clo*4+j] = uint8(((acc[j]+ny/2)/ny + 0x8000) >> 16)
			}
			if bt+1 < h {
				bt++
				trow := tmp[bt*rowBytes:]
				for j := 0; j < nb; j++ {
					acc[j] += uint64(trow[clo*4+j])
				}
			}
			if y >= half && t < y-half+1 {
				trow := tmp[t*rowBytes:]
				for j := 0; j < nb; j++ {
					acc[j] -= uint64(trow[clo*4+j])
				}
				t++
			}
		}
	})
	return out, nil
}

func opRotate(src image.Image, args []string, _ imgResolver) (image.Image, error) {
	deg := atoiArg(args, 0, 90)
	switch ((deg % 360) + 360) % 360 {
	case 90, 180, 270:
	case 0:
		return src, nil
	default:
		return nil, fmt.Errorf("rotate supports 90/180/270 only")
	}
	b := src.Bounds()
	w, h := b.Dx(), b.Dy()
	// RGBA 快速路径：按字节直拷（v1.3.10 提速，大图约一个数量级）
	if s, ok := src.(*image.RGBA); ok {
		ow, oh := w, h
		if deg != 180 {
			ow, oh = h, w
		}
		out := image.NewRGBA(image.Rect(0, 0, ow, oh))
		ss := s.Stride
		ds := out.Stride
		switch deg {
		case 90: // out(h-1-y, x) = src(x, y)：按输出行顺序直写
			parallelBands(ow, func(lo, hi int) { // 输出行 = 源列 x
				for x := lo; x < hi; x++ {
					so := s.PixOffset(b.Min.X+x, b.Min.Y)
					for y := 0; y < h; y++ {
						si := so + y*ss
						di := x*ds + (h-1-y)*4
						out.Pix[di] = s.Pix[si]
						out.Pix[di+1] = s.Pix[si+1]
						out.Pix[di+2] = s.Pix[si+2]
						out.Pix[di+3] = s.Pix[si+3]
					}
				}
			})
		case 180: // 行倒序 + 行内逆序
			parallelBands(oh, func(lo, hi int) {
				for y := lo; y < hi; y++ {
					so := s.PixOffset(b.Min.X, b.Min.Y+h-1-y)
					do := y * ds
					for x := 0; x < w; x++ {
						si := so + (w-1-x)*4
						out.Pix[do] = s.Pix[si]
						out.Pix[do+1] = s.Pix[si+1]
						out.Pix[do+2] = s.Pix[si+2]
						out.Pix[do+3] = s.Pix[si+3]
						do += 4
					}
				}
			})
		default: // 270：out(y, w-1-x) = src(x, y)
			parallelBands(ow, func(lo, hi int) { // 输出行 = w-1-x → x = ow-1-行
				for xr := lo; xr < hi; xr++ {
					x := ow - 1 - xr
					so := s.PixOffset(b.Min.X+x, b.Min.Y)
					for y := 0; y < h; y++ {
						si := so + y*ss
						di := xr*ds + y*4
						out.Pix[di] = s.Pix[si]
						out.Pix[di+1] = s.Pix[si+1]
						out.Pix[di+2] = s.Pix[si+2]
						out.Pix[di+3] = s.Pix[si+3]
					}
				}
			})
		}
		return out, nil
	}
	switch ((deg % 360) + 360) % 360 {
	case 90:
		out := image.NewRGBA(image.Rect(0, 0, h, w))
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				out.Set(h-1-y, x, src.At(b.Min.X+x, b.Min.Y+y))
			}
		}
		return out, nil
	case 180:
		out := image.NewRGBA(image.Rect(0, 0, w, h))
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				out.Set(w-1-x, h-1-y, src.At(b.Min.X+x, b.Min.Y+y))
			}
		}
		return out, nil
	default:
		out := image.NewRGBA(image.Rect(0, 0, h, w))
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				out.Set(y, w-1-x, src.At(b.Min.X+x, b.Min.Y+y))
			}
		}
		return out, nil
	}
}

func opFlip(src image.Image, args []string, _ imgResolver) (image.Image, error) {
	code := atoiArg(args, 0, 1)
	switch code {
	case 0, 1, -1:
	default:
		return nil, fmt.Errorf("flip code must be 0|1|-1")
	}
	b := src.Bounds()
	w, h := b.Dx(), b.Dy()
	out := image.NewRGBA(image.Rect(0, 0, w, h))
	if s, ok := src.(*image.RGBA); ok { // 快速路径：字节直拷
		ds := out.Stride
		parallelBands(h, func(lo, hi int) {
			for y := lo; y < hi; y++ {
				sy := y // 源行
				if code == 0 || code == -1 {
					sy = h - 1 - y
				}
				so := s.PixOffset(b.Min.X, b.Min.Y+sy)
				do := y * ds
				switch code {
				case 1, -1: // 水平 / 双向：行内逆序（双向的行倒序已在 sy 中处理）
					for x := 0; x < w; x++ {
						si := so + (w-1-x)*4
						out.Pix[do] = s.Pix[si]
						out.Pix[do+1] = s.Pix[si+1]
						out.Pix[do+2] = s.Pix[si+2]
						out.Pix[do+3] = s.Pix[si+3]
						do += 4
					}
				default: // 0 垂直：仅行倒序（上面 sy 已处理），行内正序
					copy(out.Pix[do:do+w*4], s.Pix[so:so+w*4])
				}
			}
		})
		return out, nil
	}
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
			}
			out.Set(x, y, src.At(b.Min.X+sx, b.Min.Y+sy))
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 裁剪融合（v1.3.11）
// ---------------------------------------------------------------------------

// opBlend 裁剪融合：blend('key'[,x,y[,alpha[,cx,cy,cw,ch]]])。
//   key    同桶对象 key（单引号包裹，可含逗号 / 斜杠）
//   x,y    贴入到当前画布的左上角坐标（默认 0,0）
//   alpha  融合权重 0..1（默认 1 = 完全覆盖；0.5 ≈ cv2.addWeighted(a,0.5,b,0.5,0)）
//   cx,cy,cw,ch  可选：先对 key 的图像裁剪该矩形再融合（裁剪融合）
// 源图经 imgResolver 加载——resolver 仅按 key 取当前来源桶内对象，
// 不提供跨桶能力（同桶融合，v1.3.11 权限边界）。
func opBlend(src image.Image, args []string, res imgResolver) (image.Image, error) {
	if res == nil {
		return nil, fmt.Errorf("blend requires a same-bucket source image (unavailable in this context)")
	}
	if len(args) < 1 || strings.Trim(args[0], "'") == "" {
		return nil, fmt.Errorf("blend needs 'key'[,x,y[,alpha[,cx,cy,cw,ch]]]")
	}
	key := strings.Trim(args[0], "'")
	overlay, err := res(key)
	if err != nil {
		return nil, fmt.Errorf("blend source %q: %w", key, err)
	}
	x, y := atoiArg(args, 1, 0), atoiArg(args, 2, 0)
	alpha := 1.0
	if len(args) >= 4 {
		if v, perr := strconv.ParseFloat(strings.TrimSpace(args[3]), 64); perr == nil && v >= 0 && v <= 1 {
			alpha = v
		} else {
			return nil, fmt.Errorf("blend alpha must be within 0..1")
		}
	}
	if len(args) >= 8 {
		cx, cy := atoiArg(args, 4, 0), atoiArg(args, 5, 0)
		cw, ch := atoiArg(args, 6, 0), atoiArg(args, 7, 0)
		if cw <= 0 || ch <= 0 {
			return nil, fmt.Errorf("blend crop w/h must be > 0")
		}
		r := image.Rect(cx, cy, cx+cw, cy+ch).Intersect(overlay.Bounds())
		if r.Empty() {
			return nil, fmt.Errorf("blend crop region outside source image bounds %v", overlay.Bounds())
		}
		overlay = subImage(overlay, r)
	}
	base := toRGBA(src)
	ov := toRGBA(overlay)
	ob := ov.Bounds()
	bb := base.Bounds()
	// 逐行并行：out = base*(1-w) + overlay*w，w = alpha × 源像素不透明度
	parallelBands(ob.Dy(), func(lo, hi int) {
		for sy := lo; sy < hi; sy++ {
			dy := y + sy
			if dy < bb.Min.Y || dy >= bb.Max.Y {
				continue
			}
			so := ov.PixOffset(ob.Min.X, ob.Min.Y+sy)
			for sx := 0; sx < ob.Dx(); sx++ {
				dx := x + sx
				if dx < bb.Min.X || dx >= bb.Max.X {
					so += 4
					continue
				}
				w := alpha * float64(ov.Pix[so+3]) / 255.0
				do := base.PixOffset(dx, dy)
				for i := 0; i < 3; i++ {
					base.Pix[do+i] = uint8(clamp255(float64(base.Pix[do+i])*(1-w) + float64(ov.Pix[so+i])*w))
				}
				base.Pix[do+3] = uint8(clamp255(float64(base.Pix[do+3])*(1-w) + 255*w))
				so += 4
			}
		}
	})
	return base, nil
}

// ---------------------------------------------------------------------------
// 管道执行（image.go processImage 调用）
// ---------------------------------------------------------------------------

// applyOps 依序执行操作管道（res = 同桶图像解析器，blend 用）。
func applyOps(src image.Image, ops []imgOp, res imgResolver) (image.Image, error) {
	var err error
	for _, op := range ops {
		fn := opFuncs[op.name]
		if fn == nil {
			return nil, fmt.Errorf("unknown image op %q", op.name)
		}
		if src, err = fn(src, op.args, res); err != nil {
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
