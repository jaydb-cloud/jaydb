package server

import (
	"encoding/json"
	"testing"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/storage/memory"
	"github.com/valyala/fasthttp"
)

func TestServer_REST_API(t *testing.T) {
	memStore := memory.NewDriver()
	database, err := db.Open(db.Options{
		Storage:       memStore,
		ShardingDepth: 2,
	})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	srv, err := NewServer(Options{DB: database})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// 1. PUT create document (If-None-Match: *)
	ctx1 := &fasthttp.RequestCtx{}
	ctx1.Request.Header.SetMethod(fasthttp.MethodPut)
	ctx1.Request.SetRequestURI("/v1/kv/users/100/profile")
	ctx1.Request.Header.Set("If-None-Match", "*")
	ctx1.Request.SetBodyString(`{"name":"Alice","role":"admin"}`)

	srv.HandleRequest(ctx1)

	if ctx1.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected HTTP 200 on PUT create, got %d", ctx1.Response.StatusCode())
	}

	etag1 := string(ctx1.Response.Header.Peek("ETag"))
	if etag1 == "" {
		t.Fatalf("expected ETag header in response")
	}

	// 2. GET document
	ctx2 := &fasthttp.RequestCtx{}
	ctx2.Request.Header.SetMethod(fasthttp.MethodGet)
	ctx2.Request.SetRequestURI("/v1/kv/users/100/profile")

	srv.HandleRequest(ctx2)

	if ctx2.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected HTTP 200 on GET, got %d", ctx2.Response.StatusCode())
	}
	if string(ctx2.Response.Header.Peek("ETag")) != etag1 {
		t.Fatalf("expected ETag %s, got %s", etag1, ctx2.Response.Header.Peek("ETag"))
	}
	if string(ctx2.Response.Body()) != `{"name":"Alice","role":"admin"}` {
		t.Fatalf("expected body match, got '%s'", ctx2.Response.Body())
	}

	// 3. PUT update with wrong ETag -> HTTP 412 Precondition Failed
	ctx3 := &fasthttp.RequestCtx{}
	ctx3.Request.Header.SetMethod(fasthttp.MethodPut)
	ctx3.Request.SetRequestURI("/v1/kv/users/100/profile")
	ctx3.Request.Header.Set("If-Match", `"invalid-etag"`)
	ctx3.Request.SetBodyString(`{"name":"Bob"}`)

	srv.HandleRequest(ctx3)

	if ctx3.Response.StatusCode() != fasthttp.StatusPreconditionFailed {
		t.Fatalf("expected HTTP 412 on invalid ETag, got %d", ctx3.Response.StatusCode())
	}

	// 4. PUT update with correct ETag -> HTTP 200 OK
	ctx4 := &fasthttp.RequestCtx{}
	ctx4.Request.Header.SetMethod(fasthttp.MethodPut)
	ctx4.Request.SetRequestURI("/v1/kv/users/100/profile")
	ctx4.Request.Header.Set("If-Match", etag1)
	ctx4.Request.SetBodyString(`{"name":"Bob","role":"admin"}`)

	srv.HandleRequest(ctx4)

	if ctx4.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected HTTP 200 on valid ETag update, got %d", ctx4.Response.StatusCode())
	}

	etag2 := string(ctx4.Response.Header.Peek("ETag"))
	if etag2 == etag1 {
		t.Fatalf("expected ETag to update")
	}

	// 5. LIST documents under prefix /v1/list/users/100
	ctx5 := &fasthttp.RequestCtx{}
	ctx5.Request.Header.SetMethod(fasthttp.MethodGet)
	ctx5.Request.SetRequestURI("/v1/list/users/100")

	srv.HandleRequest(ctx5)

	if ctx5.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected HTTP 200 on LIST, got %d", ctx5.Response.StatusCode())
	}

	var items []db.Item
	if err := json.Unmarshal(ctx5.Response.Body(), &items); err != nil {
		t.Fatalf("failed to decode list json: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Meta.Key != "users/100/profile" {
		t.Fatalf("expected key 'users/100/profile', got '%s'", items[0].Meta.Key)
	}

	// 6. DELETE document
	ctx6 := &fasthttp.RequestCtx{}
	ctx6.Request.Header.SetMethod(fasthttp.MethodDelete)
	ctx6.Request.SetRequestURI("/v1/kv/users/100/profile")
	ctx6.Request.Header.Set("If-Match", etag2)

	srv.HandleRequest(ctx6)

	if ctx6.Response.StatusCode() != fasthttp.StatusNoContent {
		t.Fatalf("expected HTTP 204 on DELETE, got %d", ctx6.Response.StatusCode())
	}
}

func TestServer_NoHTTPProxying(t *testing.T) {
	memStore := memory.NewDriver()
	database, err := db.Open(db.Options{
		Storage:       memStore,
		ShardingDepth: 2,
	})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	// Even if NodeAddr is passed in Options (for backward compatibility),
	// the server must execute against s.db directly without any HTTP proxying.
	srv, err := NewServer(Options{
		DB:       database,
		NodeAddr: "127.0.0.1:9999",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPut)
	ctx.Request.SetRequestURI("/v1/kv/test/key")
	ctx.Request.SetBodyString(`"hello"`)
	srv.HandleRequest(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", ctx.Response.StatusCode())
	}
}

