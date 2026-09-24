package cluster

import (
	"bytes"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
)

func TestBinaryCodec_Request(t *testing.T) {
	req := InterQueryReq{
		Op:           OpPut,
		TS:           1234567890,
		Auth:         "auth-signature-hash",
		Namespace:    "tenant-alpha:default",
		Key:          "users/42/settings",
		ExpectedETag: `"etag-1234"`,
		Value:        []byte(`{"theme":"dark","notifications":true}`),
	}

	var buf bytes.Buffer
	if err := writeBinaryReq(&buf, req); err != nil {
		t.Fatalf("writeBinaryReq error: %v", err)
	}

	decoded, err := readBinaryReq(&buf)
	if err != nil {
		t.Fatalf("readBinaryReq error: %v", err)
	}

	if decoded.Op != req.Op {
		t.Errorf("Op mismatch: got %v, want %v", decoded.Op, req.Op)
	}
	if decoded.TS != req.TS {
		t.Errorf("TS mismatch: got %v, want %v", decoded.TS, req.TS)
	}
	if decoded.Auth != req.Auth {
		t.Errorf("Auth mismatch: got %q, want %q", decoded.Auth, req.Auth)
	}
	if decoded.Namespace != req.Namespace {
		t.Errorf("Namespace mismatch: got %q, want %q", decoded.Namespace, req.Namespace)
	}
	if decoded.Key != req.Key {
		t.Errorf("Key mismatch: got %q, want %q", decoded.Key, req.Key)
	}
	if decoded.ExpectedETag != req.ExpectedETag {
		t.Errorf("ExpectedETag mismatch: got %q, want %q", decoded.ExpectedETag, req.ExpectedETag)
	}
	if !bytes.Equal(decoded.Value, req.Value) {
		t.Errorf("Value mismatch: got %q, want %q", string(decoded.Value), string(req.Value))
	}
}

func TestBinaryCodec_ResponseSuccess(t *testing.T) {
	now := time.Now().Truncate(time.Nanosecond)
	resp := InterQueryResp{
		Object: &storage.Object{
			Key:     "users/42/settings",
			ETag:    `"etag-abc"`,
			ModTime: now,
			Value:   []byte(`{"theme":"light"}`),
		},
	}

	var buf bytes.Buffer
	if err := writeBinaryResp(&buf, resp); err != nil {
		t.Fatalf("writeBinaryResp error: %v", err)
	}

	decoded, err := readBinaryResp(&buf)
	if err != nil {
		t.Fatalf("readBinaryResp error: %v", err)
	}

	if decoded.Err != "" {
		t.Errorf("Unexpected Err: %s", decoded.Err)
	}
	if decoded.Object == nil {
		t.Fatal("Expected Object, got nil")
	}
	if decoded.Object.Key != resp.Object.Key {
		t.Errorf("Key mismatch: got %q, want %q", decoded.Object.Key, resp.Object.Key)
	}
	if decoded.Object.ETag != resp.Object.ETag {
		t.Errorf("ETag mismatch: got %q, want %q", decoded.Object.ETag, resp.Object.ETag)
	}
	if !decoded.Object.ModTime.Equal(resp.Object.ModTime) {
		t.Errorf("ModTime mismatch: got %v, want %v", decoded.Object.ModTime, resp.Object.ModTime)
	}
	if !bytes.Equal(decoded.Object.Value, resp.Object.Value) {
		t.Errorf("Value mismatch: got %q, want %q", string(decoded.Object.Value), string(resp.Object.Value))
	}
}

func TestBinaryCodec_ResponseError(t *testing.T) {
	resp := InterQueryResp{
		Err: "storage: object not found",
	}

	var buf bytes.Buffer
	if err := writeBinaryResp(&buf, resp); err != nil {
		t.Fatalf("writeBinaryResp error: %v", err)
	}

	decoded, err := readBinaryResp(&buf)
	if err != nil {
		t.Fatalf("readBinaryResp error: %v", err)
	}

	if decoded.Err != resp.Err {
		t.Errorf("Err mismatch: got %q, want %q", decoded.Err, resp.Err)
	}
	if decoded.Object != nil {
		t.Errorf("Expected nil Object, got %+v", decoded.Object)
	}
}

func TestBinaryCodec_ResponseEmpty(t *testing.T) {
	// For Delete operations, both Object and Err are empty/nil
	resp := InterQueryResp{}

	var buf bytes.Buffer
	if err := writeBinaryResp(&buf, resp); err != nil {
		t.Fatalf("writeBinaryResp error: %v", err)
	}

	decoded, err := readBinaryResp(&buf)
	if err != nil {
		t.Fatalf("readBinaryResp error: %v", err)
	}

	if decoded.Err != "" {
		t.Errorf("Expected empty Err, got %q", decoded.Err)
	}
	if decoded.Object != nil {
		t.Errorf("Expected nil Object, got %+v", decoded.Object)
	}
}
