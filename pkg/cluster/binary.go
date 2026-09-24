package cluster

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
)

const (
	protocolMagic byte = 0x4A // 'J' for JayDB
	protocolVer   byte = 1
)

var (
	ErrInvalidMagic = errors.New("jaydb cluster: invalid protocol magic")
	ErrInvalidVer   = errors.New("jaydb cluster: unsupported protocol version")

	binaryBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, 65536)
			return &b
		},
	}
)

// writeBinaryReq serializes an InterQueryReq into w using binary framing.
func writeBinaryReq(w io.Writer, req InterQueryReq) error {
	authB := []byte(req.Auth)
	nsB := []byte(req.Namespace)
	keyB := []byte(req.Key)
	etagB := []byte(req.ExpectedETag)
	valB := req.Value

	totalLen := 1 + 1 + 1 + 8 +
		2 + len(authB) +
		2 + len(nsB) +
		2 + len(keyB) +
		2 + len(etagB) +
		4 + len(valB)

	var buf []byte
	var poolBuf *[]byte
	if totalLen <= 65536 {
		poolBuf = binaryBufPool.Get().(*[]byte)
		buf = (*poolBuf)[:totalLen]
		defer binaryBufPool.Put(poolBuf)
	} else {
		buf = make([]byte, totalLen)
	}
	buf[0] = protocolMagic
	buf[1] = protocolVer
	buf[2] = byte(req.Op)
	binary.BigEndian.PutUint64(buf[3:11], uint64(req.TS))

	off := 11
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(authB)))
	off += 2
	copy(buf[off:], authB)
	off += len(authB)

	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(nsB)))
	off += 2
	copy(buf[off:], nsB)
	off += len(nsB)

	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(keyB)))
	off += 2
	copy(buf[off:], keyB)
	off += len(keyB)

	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(etagB)))
	off += 2
	copy(buf[off:], etagB)
	off += len(etagB)

	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(valB)))
	off += 4
	copy(buf[off:], valB)

	_, err := w.Write(buf)
	return err
}

// readBinaryReq deserializes an InterQueryReq from r using binary framing.
func readBinaryReq(r io.Reader) (InterQueryReq, error) {
	var header [11]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return InterQueryReq{}, err
	}
	if header[0] != protocolMagic {
		return InterQueryReq{}, ErrInvalidMagic
	}
	if header[1] != protocolVer {
		return InterQueryReq{}, ErrInvalidVer
	}
	op := OpType(header[2])
	ts := int64(binary.BigEndian.Uint64(header[3:11]))

	var lBuf [2]byte
	// Read auth
	if _, err := io.ReadFull(r, lBuf[:]); err != nil {
		return InterQueryReq{}, err
	}
	authLen := int(binary.BigEndian.Uint16(lBuf[:]))
	var authStr string
	if authLen > 0 {
		authBytes := make([]byte, authLen)
		if _, err := io.ReadFull(r, authBytes); err != nil {
			return InterQueryReq{}, err
		}
		authStr = string(authBytes)
	}

	// Read namespace
	if _, err := io.ReadFull(r, lBuf[:]); err != nil {
		return InterQueryReq{}, err
	}
	nsLen := int(binary.BigEndian.Uint16(lBuf[:]))
	var nsStr string
	if nsLen > 0 {
		nsBytes := make([]byte, nsLen)
		if _, err := io.ReadFull(r, nsBytes); err != nil {
			return InterQueryReq{}, err
		}
		nsStr = string(nsBytes)
	}

	// Read key
	if _, err := io.ReadFull(r, lBuf[:]); err != nil {
		return InterQueryReq{}, err
	}
	keyLen := int(binary.BigEndian.Uint16(lBuf[:]))
	var keyStr string
	if keyLen > 0 {
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return InterQueryReq{}, err
		}
		keyStr = string(keyBytes)
	}

	// Read expected ETag
	if _, err := io.ReadFull(r, lBuf[:]); err != nil {
		return InterQueryReq{}, err
	}
	etagLen := int(binary.BigEndian.Uint16(lBuf[:]))
	var etagStr string
	if etagLen > 0 {
		etagBytes := make([]byte, etagLen)
		if _, err := io.ReadFull(r, etagBytes); err != nil {
			return InterQueryReq{}, err
		}
		etagStr = string(etagBytes)
	}

	// Read value
	var vLenBuf [4]byte
	if _, err := io.ReadFull(r, vLenBuf[:]); err != nil {
		return InterQueryReq{}, err
	}
	valLen := int(binary.BigEndian.Uint32(vLenBuf[:]))
	var valBytes []byte
	if valLen > 0 {
		valBytes = make([]byte, valLen)
		if _, err := io.ReadFull(r, valBytes); err != nil {
			return InterQueryReq{}, err
		}
	}

	return InterQueryReq{
		Op:           op,
		TS:           ts,
		Auth:         authStr,
		Namespace:    nsStr,
		Key:          keyStr,
		ExpectedETag: etagStr,
		Value:        valBytes,
	}, nil
}

// writeBinaryResp serializes an InterQueryResp into w using binary framing.
func writeBinaryResp(w io.Writer, resp InterQueryResp) error {
	if resp.Err != "" {
		errB := []byte(resp.Err)
		buf := make([]byte, 3+2+len(errB))
		buf[0] = protocolMagic
		buf[1] = protocolVer
		buf[2] = 1 // error status
		binary.BigEndian.PutUint16(buf[3:5], uint16(len(errB)))
		copy(buf[5:], errB)
		_, err := w.Write(buf)
		return err
	}

	if resp.Object == nil {
		buf := []byte{protocolMagic, protocolVer, 0, 0} // status 0, hasObj 0
		_, err := w.Write(buf)
		return err
	}

	obj := resp.Object
	keyB := []byte(obj.Key)
	etagB := []byte(obj.ETag)
	valB := obj.Value
	modNano := obj.ModTime.UnixNano()

	totalLen := 3 + 1 + 2 + len(keyB) + 2 + len(etagB) + 8 + 4 + len(valB)
	var buf []byte
	var poolBuf *[]byte
	if totalLen <= 65536 {
		poolBuf = binaryBufPool.Get().(*[]byte)
		buf = (*poolBuf)[:totalLen]
		defer binaryBufPool.Put(poolBuf)
	} else {
		buf = make([]byte, totalLen)
	}
	buf[0] = protocolMagic
	buf[1] = protocolVer
	buf[2] = 0 // OK status
	buf[3] = 1 // hasObj = true

	off := 4
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(keyB)))
	off += 2
	copy(buf[off:], keyB)
	off += len(keyB)

	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(etagB)))
	off += 2
	copy(buf[off:], etagB)
	off += len(etagB)

	binary.BigEndian.PutUint64(buf[off:off+8], uint64(modNano))
	off += 8

	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(valB)))
	off += 4
	copy(buf[off:], valB)

	_, err := w.Write(buf)
	return err
}

// readBinaryResp deserializes an InterQueryResp from r using binary framing.
func readBinaryResp(r io.Reader) (InterQueryResp, error) {
	var header [3]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return InterQueryResp{}, err
	}
	if header[0] != protocolMagic {
		return InterQueryResp{}, ErrInvalidMagic
	}
	if header[1] != protocolVer {
		return InterQueryResp{}, ErrInvalidVer
	}
	status := header[2]
	if status == 1 {
		var lBuf [2]byte
		if _, err := io.ReadFull(r, lBuf[:]); err != nil {
			return InterQueryResp{}, err
		}
		errLen := int(binary.BigEndian.Uint16(lBuf[:]))
		errBytes := make([]byte, errLen)
		if _, err := io.ReadFull(r, errBytes); err != nil {
			return InterQueryResp{}, err
		}
		return InterQueryResp{Err: string(errBytes)}, nil
	}

	var hasObjBuf [1]byte
	if _, err := io.ReadFull(r, hasObjBuf[:]); err != nil {
		return InterQueryResp{}, err
	}
	if hasObjBuf[0] == 0 {
		return InterQueryResp{}, nil
	}

	var lBuf [2]byte
	// Read key
	if _, err := io.ReadFull(r, lBuf[:]); err != nil {
		return InterQueryResp{}, err
	}
	keyLen := int(binary.BigEndian.Uint16(lBuf[:]))
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBytes); err != nil {
		return InterQueryResp{}, err
	}

	// Read ETag
	if _, err := io.ReadFull(r, lBuf[:]); err != nil {
		return InterQueryResp{}, err
	}
	etagLen := int(binary.BigEndian.Uint16(lBuf[:]))
	etagBytes := make([]byte, etagLen)
	if _, err := io.ReadFull(r, etagBytes); err != nil {
		return InterQueryResp{}, err
	}

	// Read ModTime
	var nanoBuf [8]byte
	if _, err := io.ReadFull(r, nanoBuf[:]); err != nil {
		return InterQueryResp{}, err
	}
	modNano := int64(binary.BigEndian.Uint64(nanoBuf[:]))

	// Read value
	var vLenBuf [4]byte
	if _, err := io.ReadFull(r, vLenBuf[:]); err != nil {
		return InterQueryResp{}, err
	}
	valLen := int(binary.BigEndian.Uint32(vLenBuf[:]))
	var valBytes []byte
	if valLen > 0 {
		valBytes = make([]byte, valLen)
		if _, err := io.ReadFull(r, valBytes); err != nil {
			return InterQueryResp{}, err
		}
	}

	return InterQueryResp{
		Object: &storage.Object{
			Key:     string(keyBytes),
			ETag:    string(etagBytes),
			ModTime: time.Unix(0, modNano),
			Value:   valBytes,
		},
	}, nil
}
