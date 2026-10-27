package s3

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	_ "image/gif" // 注册 GIF 解码器
	"image/jpeg"
	"image/png"
	"net/http"
	"strconv"
	"strings"
)

// imgParams 图片预处理参数（GET 读取链路加工 / PUT 上传链路解码）。
type imgParams struct {
	Crop    image.Rectangle // 非空 = 裁剪（left,top,width,height）
	ResizeW int
	ResizeH int
	Format  string // jpeg | png（空 = 保持原格式）
	Quality int    // jpeg 质量（1-100，默认 85）
	Base64  bool   // GET: 转 base64 输出；PUT: body 为 base64（解码后二进制落盘）
	DataURL bool   // base64 输出时带 data:image/...;base64, 前缀

	Ops    []imgOp // OpenCV 风格操作管道（v1.3.7）：rectangle/text/circle/line/grayscale/blur/rotate/flip
	SaveAs string  // POST 处理另存目标（bucket/key，v1.3.7；空 = 不另存）
}

// parseImgParams 从查询参数解析（x-gimg-*）。
//   x-gimg-crop=left,top,width,height
//   x-gimg-resize=width,height（0 = 按比例）
//   x-gimg-format=jpeg|png
//   x-gimg-quality=1..100
//   x-gimg=base64 | x-gimg=base64dataurl（GET 输出 / PUT 输入）
func parseImgParams(q map[string][]string) (imgParams, bool) {
	var p imgParams
	seen := false
	if v := first(q, "x-gimg"); v != "" {
		seen = true
		switch strings.ToLower(v) {
		case "base64":
			p.Base64 = true
		case "base64dataurl", "dataurl":
			p.Base64 = true
			p.DataURL = true
		}
	}
	if v := first(q, "x-gimg-crop"); v != "" {
		seen = true
		parts := strings.Split(v, ",")
		if len(parts) == 4 {
			var l, t, w, h int
			fmt.Sscanf(parts[0], "%d", &l)
			fmt.Sscanf(parts[1], "%d", &t)
			fmt.Sscanf(parts[2], "%d", &w)
			fmt.Sscanf(parts[3], "%d", &h)
			if w > 0 && h > 0 {
				p.Crop = image.Rect(l, t, l+w, t+h)
			}
		}
	}
	if v := first(q, "x-gimg-resize"); v != "" {
		seen = true
		parts := strings.Split(v, ",")
		if len(parts) >= 1 {
			fmt.Sscanf(parts[0], "%d", &p.ResizeW)
		}
		if len(parts) >= 2 {
			fmt.Sscanf(parts[1], "%d", &p.ResizeH)
		}
	}
	if v := first(q, "x-gimg-format"); v != "" {
		seen = true
		p.Format = strings.ToLower(v)
		if p.Format == "jpg" {
			p.Format = "jpeg"
		}
	}
	if v := first(q, "x-gimg-ops"); v != "" {
		seen = true
		ops, perr := parseOps(v)
		if perr != nil {
			return p, true // 语法错按无参数透传，具体报错由 processImage 给出
		}
		p.Ops = ops
	}
	if v := first(q, "x-gimg-save-as"); v != "" {
		seen = true
		p.SaveAs = v
	}
	if v := first(q, "x-gimg-quality"); v != "" {
		seen = true
		p.Quality, _ = strconv.Atoi(v)
	}
	if p.Quality <= 0 || p.Quality > 100 {
		p.Quality = 85
	}
	return p, seen
}

func first(q map[string][]string, k string) string {
	if vs, ok := q[k]; ok && len(vs) > 0 {
		return vs[0]
	}
	return ""
}

// decodeBase64Body 解码 base64 上传体（兼容 data URL 前缀与空白换行）。
func decodeBase64Body(data []byte) ([]byte, error) {
	s := strings.TrimSpace(string(data))
	if i := strings.Index(s, "base64,"); i >= 0 {
		s = strings.TrimSpace(s[i+len("base64,"):])
	}
	s = strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\r', '\t', ' ':
			return -1
		}
		return r
	}, s)
	out, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		// 宽容：尝试 RawStd（无填充）
		out, err = base64.RawStdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 payload: %w", err)
		}
	}
	return out, nil
}

// processImage 图片加工：解码 → 裁剪 → 缩放 → 编码（格式转换）→（可选）base64。
// 非图片输入且无 base64 参数时报错；已是目标格式且无加工时原样返回。
func processImage(data []byte, p imgParams) ([]byte, string, error) {
	needTransform := !p.Crop.Empty() || p.ResizeW > 0 || p.ResizeH > 0 || p.Format != "" || len(p.Ops) > 0
	if !needTransform {
		if p.Base64 {
			return encodeBase64Out(data, "", p.DataURL), "text/plain; charset=utf-8", nil
		}
		return data, "", nil
	}
	src, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", fmt.Errorf("decode image: %w", err)
	}
	// 裁剪
	if !p.Crop.Empty() {
		b := src.Bounds()
		r := p.Crop.Intersect(b)
		if r.Empty() {
			return nil, "", fmt.Errorf("crop region %v outside image bounds %v", p.Crop, b)
		}
		src = subImage(src, r)
	}
	// OpenCV 风格操作管道（v1.3.7）：绘制/滤镜/几何（绘制坐标基于裁剪后画布）
	if len(p.Ops) > 0 {
		src, err = applyOps(src, p.Ops)
		if err != nil {
			return nil, "", err
		}
	}
	// 缩放（双线性）
	if p.ResizeW > 0 || p.ResizeH > 0 {
		src = resizeBilinear(src, p.ResizeW, p.ResizeH)
	}
	// 编码
	outFmt := p.Format
	if outFmt == "" {
		outFmt = format
	}
	var buf bytes.Buffer
	switch outFmt {
	case "jpeg":
		if err := jpeg.Encode(&buf, src, &jpeg.Options{Quality: p.Quality}); err != nil {
			return nil, "", err
		}
	case "png":
		if err := png.Encode(&buf, src); err != nil {
			return nil, "", err
		}
	default:
		return nil, "", fmt.Errorf("unsupported output format %q (jpeg|png)", outFmt)
	}
	ct := "image/jpeg"
	if outFmt == "png" {
		ct = "image/png"
	}
	if p.Base64 {
		return encodeBase64Out(buf.Bytes(), ct, p.DataURL), "text/plain; charset=utf-8", nil
	}
	return buf.Bytes(), ct, nil
}

// encodeBase64Out base64 输出（DataURL 带 mime 前缀）。
func encodeBase64Out(data []byte, mime string, dataURL bool) []byte {
	enc := base64.StdEncoding.EncodeToString(data)
	if dataURL {
		if mime == "" {
			mime = http.DetectContentType(data)
		}
		return []byte("data:" + mime + ";base64," + enc)
	}
	return []byte(enc)
}

// subImage 取子图（任意实现，统一拷为 RGBA 保证后续几何变换可用）。
func subImage(src image.Image, r image.Rectangle) image.Image {
	dst := image.NewRGBA(image.Rect(0, 0, r.Dx(), r.Dy()))
	draw.Draw(dst, dst.Bounds(), src, r.Min, draw.Src)
	return dst
}

// resizeBilinear 双线性缩放（w/h 为 0 时按另一维等比；均 >0 时拉伸）。
// 纯手写实现，避免引入外部依赖。
func resizeBilinear(src image.Image, w, h int) image.Image {
	b := src.Bounds()
	sw, sh := b.Dx(), b.Dy()
	if w <= 0 && h <= 0 {
		return src
	}
	if w <= 0 {
		w = int(float64(sw) * float64(h) / float64(sh))
	}
	if h <= 0 {
		h = int(float64(sh) * float64(w) / float64(sw))
	}
	if w < 1 {
		w = 1
	}
	if h < 1 {
		h = 1
	}
	if w == sw && h == sh {
		return src
	}
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	xr := float64(sw) / float64(w)
	yr := float64(sh) / float64(h)
	for y := 0; y < h; y++ {
		sy := (float64(y)+0.5)*yr - 0.5
		if sy < 0 {
			sy = 0
		}
		y0 := int(sy)
		y1 := y0 + 1
		if y1 >= sh {
			y1 = sh - 1
		}
		fy := sy - float64(y0)
		for x := 0; x < w; x++ {
			sx := (float64(x)+0.5)*xr - 0.5
			if sx < 0 {
				sx = 0
			}
			x0 := int(sx)
			x1 := x0 + 1
			if x1 >= sw {
				x1 = sw - 1
			}
			fx := sx - float64(x0)
			c00 := rgbaAt(src, b.Min.X+x0, b.Min.Y+y0)
			c10 := rgbaAt(src, b.Min.X+x1, b.Min.Y+y0)
			c01 := rgbaAt(src, b.Min.X+x0, b.Min.Y+y1)
			c11 := rgbaAt(src, b.Min.X+x1, b.Min.Y+y1)
			var out [4]float64
			for i := 0; i < 4; i++ {
				top := c00[i]*(1-fx) + c10[i]*fx
				bot := c01[i]*(1-fx) + c11[i]*fx
				out[i] = top*(1-fy) + bot*fy
			}
			dst.SetRGBA(x, y, color.RGBA{
				R: uint8(clamp255(out[0])), G: uint8(clamp255(out[1])),
				B: uint8(clamp255(out[2])), A: uint8(clamp255(out[3])),
			})
		}
	}
	return dst
}

func rgbaAt(src image.Image, x, y int) [4]float64 {
	r, g, b, a := src.At(x, y).RGBA()
	return [4]float64{float64(r >> 8), float64(g >> 8), float64(b >> 8), float64(a >> 8)}
}

func clamp255(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return v
}
