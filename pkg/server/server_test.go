package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/cluster"
	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
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

func TestServer_ForwardsViaTCPMesh(t *testing.T) {
	ring := sharding.NewRing(3, 2)

	// 1. Setup Node 1
	mem1 := memory.NewDriver()
	db1, err := db.Open(db.Options{
		Storage:       mem1,
		ShardingDepth: 2,
		Ring:          ring,
	})
	if err != nil {
		t.Fatalf("db1 open error: %v", err)
	}
	defer db1.Close()

	node1, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:    "node-1",
		BindAddr:    "127.0.0.1",
		BindPort:    0,
		MeshPort:    0,
		Ring:        ring,
		DBHandler:   db1,
		PoolSize:    8,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("node1 error: %v", err)
	}
	defer node1.Close()

	// 2. Setup Node 2
	mem2 := memory.NewDriver()
	db2, err := db.Open(db.Options{
		Storage:       mem2,
		ShardingDepth: 2,
		Ring:          ring,
	})
	if err != nil {
		t.Fatalf("db2 open error: %v", err)
	}
	defer db2.Close()

	node2, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:    "node-2",
		BindAddr:    "127.0.0.1",
		BindPort:    0,
		MeshPort:    0,
		JoinAddrs:   []string{node1.GossipAddr()},
		Ring:        ring,
		DBHandler:   db2,
		PoolSize:    8,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("node2 error: %v", err)
	}
	defer node2.Close()

	time.Sleep(1 * time.Second)

	// Create Server for Node 2 with ClusterNode configured
	srv2, err := NewServer(Options{
		DB:          db2,
		ClusterNode: node2,
	})
	if err != nil {
		t.Fatalf("srv2 create error: %v", err)
	}

	// Pick a key owned by Node 1
	node1Addr := node1.SelfMeshAddr()
	var targetKey string
	for i := 0; i < 1000; i++ {
		cand := fmt.Sprintf("server-test-key-%d", i)
		if ring.GetNode(cand) == node1Addr {
			targetKey = cand
			break
		}
	}
	if targetKey == "" {
		t.Fatalf("failed to find key owned by node1")
	}

	// 1. PUT request sent to Node 2 for key owned by Node 1
	putCtx := &fasthttp.RequestCtx{}
	putCtx.Request.Header.SetMethod(fasthttp.MethodPut)
	putCtx.Request.SetRequestURI(fmt.Sprintf("/v1/kv/%s", targetKey))
	putCtx.Request.SetBodyString(`{"server":"mesh"}`)
	srv2.HandleRequest(putCtx)

	if putCtx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected HTTP 200 on mesh forwarded PUT, got %d: %s", putCtx.Response.StatusCode(), putCtx.Response.Body())
	}
	etag := string(putCtx.Response.Header.Peek("ETag"))
	if etag == "" {
		t.Fatalf("expected ETag in PUT response")
	}

	// Verify the object is stored on Node 1
	obj, err := db1.GetRaw(context.Background(), targetKey)
	if err != nil || obj == nil {
		t.Fatalf("expected object on node 1, got error: %v", err)
	}
	if string(obj.Value) != `{"server":"mesh"}` {
		t.Fatalf("expected object value match, got %s", string(obj.Value))
	}

	// Verify Node 2 does NOT cache this forwarded key
	items2, _ := db2.Cache().GetCacheSize()
	if items2 != 0 {
		t.Fatalf("forwarding node 2 should have 0 cached items, got %d", items2)
	}

	// 2. GET request sent to Node 2 for key owned by Node 1
	getCtx := &fasthttp.RequestCtx{}
	getCtx.Request.Header.SetMethod(fasthttp.MethodGet)
	getCtx.Request.SetRequestURI(fmt.Sprintf("/v1/kv/%s", targetKey))
	srv2.HandleRequest(getCtx)

	if getCtx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected HTTP 200 on mesh forwarded GET, got %d: %s", getCtx.Response.StatusCode(), getCtx.Response.Body())
	}
	if string(getCtx.Response.Body()) != `{"server":"mesh"}` {
		t.Fatalf("expected body match, got %s", string(getCtx.Response.Body()))
	}
	if string(getCtx.Response.Header.Peek("ETag")) != etag {
		t.Fatalf("expected ETag match, got %s", getCtx.Response.Header.Peek("ETag"))
	}

	// 3. DELETE request sent to Node 2 for key owned by Node 1
	delCtx := &fasthttp.RequestCtx{}
	delCtx.Request.Header.SetMethod(fasthttp.MethodDelete)
	delCtx.Request.SetRequestURI(fmt.Sprintf("/v1/kv/%s", targetKey))
	srv2.HandleRequest(delCtx)

	if delCtx.Response.StatusCode() != fasthttp.StatusNoContent {
		t.Fatalf("expected HTTP 204 on mesh forwarded DELETE, got %d: %s", delCtx.Response.StatusCode(), delCtx.Response.Body())
	}

	// Verify object is now deleted on Node 1
	_, err = db1.GetRaw(context.Background(), targetKey)
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound on node 1 after delete, got %v", err)
	}
}
