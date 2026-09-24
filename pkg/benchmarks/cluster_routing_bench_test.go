package benchmarks

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/cluster"
	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/metrics"
	"github.com/avivklas/jaydb/pkg/server"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage/memory"
	"github.com/valyala/fasthttp"
)

func BenchmarkForwarding_TCPMesh_vs_FastHTTP(b *testing.B) {
	ring := sharding.NewRing(3, 2)
	store := memory.NewDriver()
	db1, err := db.Open(db.Options{
		Storage:       store,
		ShardingDepth: 2,
		Ring:          ring,
	})
	if err != nil {
		b.Fatalf("failed to open db1: %v", err)
	}
	defer db1.Close()

	ctx := context.Background()
	key := "benchmark/forwarding/key"
	val := []byte(`{"id":123,"message":"hello benchmark"}`)
	if _, err := db1.Put(ctx, key, val); err != nil {
		b.Fatalf("failed to seed key: %v", err)
	}

	// 1. Setup TCP Mesh Node 1 (Owner)
	node1, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:    "node-1",
		BindAddr:    "127.0.0.1",
		BindPort:    0,
		MeshPort:    0,
		Ring:        ring,
		DBHandler:   db1,
		PoolSize:    16,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		b.Fatalf("failed to create node1: %v", err)
	}
	defer node1.Close()

	// Setup TCP Mesh Node 2 (Forwarder)
	node2, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:    "node-2",
		BindAddr:    "127.0.0.1",
		BindPort:    0,
		MeshPort:    0,
		JoinAddrs:   []string{node1.GossipAddr()},
		Ring:        ring,
		PoolSize:    16,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		b.Fatalf("failed to create node2: %v", err)
	}
	defer node2.Close()

	// 2. Setup HTTP Server for Node 1
	httpSrv, err := server.NewServer(server.Options{DB: db1})
	if err != nil {
		b.Fatalf("failed to create http server: %v", err)
	}
	httpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen on tcp: %v", err)
	}
	defer httpLn.Close()
	defer httpSrv.Close()

	go func() {
		_ = fasthttp.Serve(httpLn, httpSrv.HandleRequest)
	}()

	httpAddr := httpLn.Addr().String()

	// Setup HTTP Forwarder Client
	httpClient := &fasthttp.Client{
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	time.Sleep(1 * time.Second)

	targetMeshAddr := node1.SelfMeshAddr()
	meshReq := cluster.InterQueryReq{
		Op:  cluster.OpGet,
		Key: key,
	}

	// Warmup TCP mesh
	if _, err := node2.ExecuteInterQuery(ctx, targetMeshAddr, meshReq); err != nil {
		b.Fatalf("tcp mesh warmup failed: %v", err)
	}

	// Warmup HTTP client
	warmReq := fasthttp.AcquireRequest()
	warmResp := fasthttp.AcquireResponse()
	warmReq.SetRequestURI(fmt.Sprintf("http://%s/v1/kv/%s", httpAddr, key))
	if err := httpClient.Do(warmReq, warmResp); err != nil || warmResp.StatusCode() != 200 {
		b.Fatalf("http warmup failed: %v (status %d)", err, warmResp.StatusCode())
	}
	fasthttp.ReleaseRequest(warmReq)
	fasthttp.ReleaseResponse(warmResp)

	// 1. Direct Inter-Node Call (Node 2 -> Node 1)
	b.Run("Forward_TCP_Binary_Mesh", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			resp, err := node2.ExecuteInterQuery(ctx, targetMeshAddr, meshReq)
			if err != nil {
				b.Fatalf("tcp mesh query failed: %v", err)
			}
			if resp.Err != "" {
				b.Fatalf("tcp mesh query resp error: %s", resp.Err)
			}
		}
	})

	// Direct HTTP Call from Node 2 to Node 1 with identical metrics recording
	b.Run("Forward_HTTP_Client", func(b *testing.B) {
		targetURL := fmt.Sprintf("http://%s/v1/kv/%s", httpAddr, key)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			req.SetRequestURI(targetURL)
			req.Header.Set("X-Forwarded-By", "127.0.0.1:9999")

			err := httpClient.Do(req, resp)
			if err != nil || resp.StatusCode() != 200 {
				metrics.ClusterForwardedRequests.WithLabelValues(httpAddr, "error").Inc()
				fasthttp.ReleaseRequest(req)
				fasthttp.ReleaseResponse(resp)
				b.Fatalf("http client failed: %v (status %d)", err, resp.StatusCode())
			}
			metrics.ClusterForwardedRequests.WithLabelValues(httpAddr, "success").Inc()
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
		}
	})
}

func BenchmarkE2E_UserRequest_Proxy_vs_Mesh(b *testing.B) {
	ring := sharding.NewRing(3, 2)
	store1 := memory.NewDriver()
	store2 := memory.NewDriver()

	// 1. Setup Node 1 (Owner)
	node1, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:    "node-1",
		BindAddr:    "127.0.0.1",
		BindPort:    0,
		MeshPort:    0,
		Ring:        ring,
		PoolSize:    16,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		b.Fatalf("failed to create node1: %v", err)
	}
	defer node1.Close()

	db1, err := db.Open(db.Options{
		Storage:       store1,
		ShardingDepth: 2,
		Ring:          ring,
		ClusterNode:   node1,
	})
	if err != nil {
		b.Fatalf("failed to open db1: %v", err)
	}
	defer db1.Close()
	node1.RegisterHandler("", db1)

	// HTTP Server for Node 1
	httpSrv1, _ := server.NewServer(server.Options{DB: db1})
	httpLn1, _ := net.Listen("tcp", "127.0.0.1:0")
	defer httpLn1.Close()
	defer httpSrv1.Close()
	go func() { _ = fasthttp.Serve(httpLn1, httpSrv1.HandleRequest) }()
	httpAddr1 := httpLn1.Addr().String()

	// 2. Setup Node 2 (Forwarder) with TCP Mesh
	node2, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:    "node-2",
		BindAddr:    "127.0.0.1",
		BindPort:    0,
		MeshPort:    0,
		JoinAddrs:   []string{node1.GossipAddr()},
		Ring:        ring,
		PoolSize:    16,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		b.Fatalf("failed to create node2: %v", err)
	}
	defer node2.Close()

	db2, err := db.Open(db.Options{
		Storage:       store2,
		ShardingDepth: 2,
		Ring:          ring,
		ClusterNode:   node2,
	})
	if err != nil {
		b.Fatalf("failed to open db2: %v", err)
	}
	defer db2.Close()
	node2.RegisterHandler("", db2)

	// HTTP Server for Node 2 using TCP Mesh (current architecture)
	httpSrv2Mesh, _ := server.NewServer(server.Options{DB: db2})
	httpLn2Mesh, _ := net.Listen("tcp", "127.0.0.1:0")
	defer httpLn2Mesh.Close()
	defer httpSrv2Mesh.Close()
	go func() { _ = fasthttp.Serve(httpLn2Mesh, httpSrv2Mesh.HandleRequest) }()
	meshHttpAddr := httpLn2Mesh.Addr().String()

	// HTTP Server for Node 2 simulating HTTP proxying (legacy architecture)
	proxyClient := &fasthttp.Client{ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	legacyProxyHandler := func(reqCtx *fasthttp.RequestCtx) {
		// Simulates legacy proxyToNode behavior in Node 2
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		reqCtx.Request.CopyTo(req)
		targetURL := fmt.Sprintf("http://%s%s", httpAddr1, string(reqCtx.Request.URI().RequestURI()))
		req.SetRequestURI(targetURL)
		req.Header.Set("X-Forwarded-By", "node-2")

		if err := proxyClient.Do(req, resp); err != nil {
			metrics.ClusterForwardedRequests.WithLabelValues(httpAddr1, "error").Inc()
			reqCtx.Error("proxy error", fasthttp.StatusBadGateway)
			return
		}
		metrics.ClusterForwardedRequests.WithLabelValues(httpAddr1, "success").Inc()
		resp.CopyTo(&reqCtx.Response)
	}

	httpLn2Proxy, _ := net.Listen("tcp", "127.0.0.1:0")
	defer httpLn2Proxy.Close()
	go func() { _ = fasthttp.Serve(httpLn2Proxy, legacyProxyHandler) }()
	proxyHttpAddr := httpLn2Proxy.Addr().String()

	time.Sleep(1 * time.Second)

	node1MeshAddr := node1.SelfMeshAddr()
	var key string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("benchmark-key-%d", i)
		if ring.GetNode(candidate) == node1MeshAddr {
			key = candidate
			break
		}
	}
	if key == "" {
		b.Fatalf("failed to find key owned by node1")
	}

	val := []byte(`{"id":456,"user":"cluster"}`)
	ctx := context.Background()
	if _, err := db1.Put(ctx, key, val); err != nil {
		b.Fatalf("failed to seed key: %v", err)
	}

	client := &fasthttp.Client{ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}

	// Benchmark E2E HTTP Proxy
	b.Run("E2E_HTTP_Proxy", func(b *testing.B) {
		targetURL := fmt.Sprintf("http://%s/v1/kv/%s", proxyHttpAddr, key)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			req.SetRequestURI(targetURL)
			err := client.Do(req, resp)
			status := resp.StatusCode()
			body := string(resp.Body())
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			if err != nil || status != 200 {
				b.Fatalf("e2e http proxy failed: %v (status %d, body %s)", err, status, body)
			}
		}
	})

	// Benchmark E2E TCP Binary Mesh
	b.Run("E2E_TCP_Binary_Mesh", func(b *testing.B) {
		targetURL := fmt.Sprintf("http://%s/v1/kv/%s", meshHttpAddr, key)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			req.SetRequestURI(targetURL)
			err := client.Do(req, resp)
			status := resp.StatusCode()
			body := string(resp.Body())
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			if err != nil || status != 200 {
				b.Fatalf("e2e tcp mesh failed: %v (status %d, body %s)", err, status, body)
			}
		}
	})
}
