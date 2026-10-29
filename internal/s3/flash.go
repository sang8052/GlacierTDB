package s3

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"glaciertdb/internal/logx"
)

// ---------------------------------------------------------------------------
// 文件闪传（秒传，v1.3.13 §1.2）
//
// 两种形态共用同一底层操作与去重范围（s3.dedup_scope）：
//   A. 单请求降级：PUT 带头 x-glacier-flash: sha256:<hex>（可选 x-glacier-flash-size
//      校验长度）——命中直接建对象引用既有 chunk 返回 etag；未命中请求体照常写入；
//   B. 两段式探测：POST /{bucket}/{key}?quick_transfer=<sha256hex>（无请求体）——
//      命中直接建对象；未命中返回明确错误码，客户端再全量 PUT。
//
// 索引（s3_flash）：登记「分块单请求 PUT」对象的 整文件 sha256 → (size, etag, 池键
// 序列)。multipart 合并对象与 ≤内联阈值的小对象不登记（前者不在写路径流式无法
// 免费算全文件 sha256，后者无共享收益）；未命中一律幂等降级为普通上传。
// 头/参数不参与 SigV4 签名（自定义头惯例，预签名 URL 可用）；不复用
// x-amz-content-sha256 做闪传判定（aws-chunked 流式时它是签名占位值）。
//
// 计费口径：命中计入请求数（PUT 计数），不计 bytes_in（内容未经过网络）。
// ---------------------------------------------------------------------------

// registerFlashIndex 登记闪传索引（对象写事务提交后调用；失败仅告警——闪传
// 不命中只是退化为普通上传，不影响存储正确性）。
func (s *Store) registerFlashIndex(scopeID, fileSHA string, size int64, etag string, hashes []string) {
	if fileSHA == "" || len(hashes) == 0 {
		return
	}
	blob := flashChunkBlob{H: hashes, S: chunkSizeSeq(size, s.chunkSize, len(hashes))}
	chunks, _ := json.Marshal(blob)
	if _, err := s.db.Exec(`INSERT INTO s3_flash(fkey,size,etag,chunks,created_ts) VALUES(?,?,?,?,?)
		ON CONFLICT(fkey) DO UPDATE SET size=excluded.size, etag=excluded.etag, chunks=excluded.chunks`,
		poolKey(scopeID, fileSHA), size, etag, string(chunks), nowTS()); err != nil {
		logx.Warnf("s3: flash index register failed: %v", err)
	}
}

// flashIndex 查闪传索引行。
type flashIndex struct {
	Size   int64
	ETag   string
	Chunks []string
	Sizes  []int64 // 各块实际字节数（与 Chunks 等长；Range 定位用）
}

// flashChunkBlob s3_flash.chunks 列的 JSON 形态：池键 + 各块实际字节数。
type flashChunkBlob struct {
	H []string `json:"h"`
	S []int64  `json:"s"`
}

func (s *Store) lookupFlashIndex(scopeID, fileSHA string) (*flashIndex, bool) {
	var size int64
	var etag, chunksJSON string
	err := s.db.QueryRow(`SELECT size,etag,chunks FROM s3_flash WHERE fkey=?`,
		poolKey(scopeID, fileSHA)).Scan(&size, &etag, &chunksJSON)
	if err != nil {
		return nil, false
	}
	var blob flashChunkBlob
	if json.Unmarshal([]byte(chunksJSON), &blob) != nil || len(blob.H) == 0 {
		return nil, false
	}
	sizes := blob.S
	if len(sizes) != len(blob.H) { // 兼容旧形态（仅开发期）：按均匀 cs 合成
		sizes = make([]int64, len(blob.H))
		for i := range sizes {
			sizes[i] = s.chunkSize
		}
	}
	return &flashIndex{Size: size, ETag: etag, Chunks: blob.H, Sizes: sizes}, true
}

// FlashCreate 闪传建对象：索引命中且全部池 chunk 存活 → 单事务建对象（引用既有
// chunk）；任一环节失败（索引未命中 / chunk 被 GC）返回 hit=false，客户端按普通
// 上传幂等降级。sizeHint ≥ 0 时校验长度一致（不一致按未命中处理）。
func (s *Store) FlashCreate(bucket, key, fileSHA string, sizeHint int64, attrs ObjectAttrs) (ObjectMeta, bool, error) {
	var out ObjectMeta
	if key == "" || len(key) > 1024 {
		return out, false, &s3Error{Code: "InvalidArgument", Status: 400, Msg: "Invalid object key"}
	}
	sha := strings.ToLower(strings.TrimPrefix(fileSHA, "sha256:"))
	if !reFlashSHA.MatchString(sha) {
		return out, false, &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: "invalid x-glacier-flash / quick_transfer value (expect sha256:<64 hex chars>)"}
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return out, false, err
	}
	scopeID := s.scopeFor(b)
	idx, ok := s.lookupFlashIndex(scopeID, sha)
	if !ok {
		return out, false, nil
	}
	if sizeHint >= 0 && sizeHint != idx.Size {
		return out, false, nil
	}
	// WORM / 版本链保护（与 PutObject 一致）
	if !b.Versioning {
		if cur, err := s.HeadObject(bucket, key, ""); err == nil {
			if err := cur.lockViolation(false); err != nil {
				return out, false, err
			}
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return out, false, err
	}
	defer tx.Rollback()

	oldStorage, oldCount := s.keyAggLocked(tx, b, key)
	versionID := ""
	if b.Versioning {
		versionID = genVersionID()
		if _, err := tx.Exec(fmt.Sprintf(`UPDATE %s SET is_latest=0 WHERE key=? AND is_latest=1`, b.TableName), key); err != nil {
			return out, false, nil
		}
	} else {
		if err := s.deleteObjectRowsTx(tx, b, key, ""); err != nil {
			return out, false, nil
		}
	}
	// 引用全部池 chunk（任一被 GC → 回退未命中；writeMu 串行下与 GC 无竞态）
	tbl := chunkTableFor(b.TableName)
	for seq, pk := range idx.Chunks {
		ok, err := poolRefExisting(tx, pk)
		if err != nil {
			return out, false, nil
		}
		if !ok {
			return out, false, nil
		}
		if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,seq,hash,data) VALUES(?,?,?,?,x'')`, tbl),
			key, versionID, seq, pk); err != nil {
			return out, false, nil
		}
	}
	manifest := MarshalPolicy(chunkManifestV2{V: 2, CS: s.chunkSize, HS: idx.Chunks, SS: idx.Sizes})
	metaJSON := "{}"
	if len(attrs.Metadata) > 0 {
		metaJSON = MarshalPolicy(attrs.Metadata)
	}
	if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,is_latest,delete_marker,size,etag,
		content_type,content_encoding,cache_control,content_disposition,metadata,content,storage,manifest,
		legal_hold,retention_mode,retain_until,created_ts)
		VALUES(?,?,1,0,?,?,?,?,?,?,?,?,?,?,0,'','',?)`, b.TableName),
		key, versionID, idx.Size, idx.ETag,
		attrs.ContentType, attrs.ContentEncoding, attrs.CacheControl, attrs.ContentDisposition,
		metaJSON, nil, "chunk", manifest, nowTS()); err != nil {
		return out, false, nil
	}
	if b.Versioning {
		s.adjustAggLocked(tx, b, key, idx.Size, 1)
	} else {
		s.adjustAggLocked(tx, b, key, idx.Size-oldStorage, 1-oldCount)
	}
	if err := tx.Commit(); err != nil {
		return out, false, nil
	}
	s.AddUsage(b.Owner, bucket, "PUT", 0, 0) // 闪传命中：计请求数不计 bytes_in

	out = ObjectMeta{
		Key: key, VersionID: versionID, IsLatest: true, Size: idx.Size, ETag: idx.ETag,
		ContentType: attrs.ContentType, ContentEncoding: attrs.ContentEncoding,
		CacheControl: attrs.CacheControl, ContentDisposition: attrs.ContentDisposition,
		Metadata: attrs.Metadata, Storage: "chunk", CreatedTS: nowTS(),
	}
	return out, true, nil
}

// reFlashSHA sha256 hex 校验（64 位十六进制）。
var reFlashSHA = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

// FlashMissErr 闪传未命中的显式错误码（B 形态：客户端据此全量 PUT）。
func FlashMissErr() *s3Error {
	return &s3Error{Code: "QuickTransferMiss", Status: 404,
		Msg: "quick_transfer hash not found on this instance: retry with a full PUT request"}
}
