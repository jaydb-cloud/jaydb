package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/metrics"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// Server encapsulates the FastHTTP server wrapper around the embedded DB engine.
type Server struct {
	db       db.DB
	listener net.Listener
}

// Options configures the server instance.
type Options struct {
	DB       db.DB
	Ring     *sharding.Ring // Deprecated: inter-node routing is handled automatically by db.DB via the QUIC cluster mesh.
	NodeAddr string         // Deprecated: node address is managed by db.DB and cluster.Node.
}

// NewServer initializes a new FastHTTP server wrapping the embedded DB.
func NewServer(opts Options) (*Server, error) {
	if opts.DB == nil {
		return nil, fmt.Errorf("server: embedded DB instance is required")
	}
	return &Server{
		db: opts.DB,
	}, nil
}

// HandleRequest is the main FastHTTP request handler.
func (s *Server) HandleRequest(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	path := string(ctx.Path())
	method := string(ctx.Method())

	// Defer metrics recording
	defer func() {
		duration := time.Since(start).Seconds()
		status := fmt.Sprintf("%d", ctx.Response.StatusCode())
		pathPrefix := s.getPathPrefix(path)
		reqSize := len(ctx.PostBody())
		respSize := len(ctx.Response.Body())

		metrics.RecordHTTPRequest(method, pathPrefix, status, duration, reqSize, respSize)
	}()

	// Prometheus metrics endpoint
	if path == "/metrics" {
		promHandler := fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
		promHandler(ctx)
		return
	}

	if path == "/v1/health" {
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetContentType("application/json")
		_, _ = ctx.WriteString(`{"status":"ok"}`)
		return
	}

	if strings.HasPrefix(path, "/v1/kv/") {
		key := strings.TrimPrefix(path, "/v1/kv/")
		if key == "" {
			ctx.Error("key path is required", fasthttp.StatusBadRequest)
			return
		}

		// Security: Validate key length to prevent abuse
		if len(key) > 1024 {
			ctx.Error("key path too long (max 1024 bytes)", fasthttp.StatusBadRequest)
			return
		}

		// Security: Validate key contains no control characters
		for _, ch := range key {
			if ch < 32 || ch == 127 {
				ctx.Error("key contains invalid characters", fasthttp.StatusBadRequest)
				return
			}
		}

		switch string(ctx.Method()) {
		case fasthttp.MethodGet:
			s.handleGet(ctx, key)
		case fasthttp.MethodPut:
			s.handlePut(ctx, key)
		case fasthttp.MethodDelete:
			s.handleDelete(ctx, key)
		default:
			ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
		}
		return
	}

	if strings.HasPrefix(path, "/v1/list/") {
		prefix := strings.TrimPrefix(path, "/v1/list/")
		s.handleList(ctx, prefix)
		return
	}

	ctx.Error("not found", fasthttp.StatusNotFound)
}

// getPathPrefix extracts the path prefix for metrics labeling
func (s *Server) getPathPrefix(path string) string {
	if path == "/metrics" {
		return "/metrics"
	}
	if path == "/v1/health" {
		return "/v1/health"
	}
	if strings.HasPrefix(path, "/v1/kv/") {
		return "/v1/kv"
	}
	if strings.HasPrefix(path, "/v1/list/") {
		return "/v1/list"
	}
	return "unknown"
}


func (s *Server) handleGet(ctx *fasthttp.RequestCtx, key string) {
	// Use request context with timeout instead of Background()
	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	var rawData []byte
	meta, err := s.db.Get(reqCtx, key, &rawData)
	duration := time.Since(start).Seconds()

	if err != nil {
		metrics.RecordDBOperation("get", "error", duration)
		if err == storage.ErrNotFound {
			ctx.Error("document not found", fasthttp.StatusNotFound)
			return
		}
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	metrics.RecordDBOperation("get", "success", duration)
	metrics.ObserveObjectSize(len(rawData))

	ctx.Response.Header.Set("ETag", meta.ETag)
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody(rawData)
}

func (s *Server) handlePut(ctx *fasthttp.RequestCtx, key string) {
	body := ctx.PostBody()
	if len(body) == 0 {
		ctx.Error("request body is required", fasthttp.StatusBadRequest)
		return
	}

	// Security: Enforce maximum body size (10MB default)
	const maxBodySize = 10 * 1024 * 1024
	if len(body) > maxBodySize {
		ctx.Error("request body too large (max 10MB)", fasthttp.StatusRequestEntityTooLarge)
		return
	}

	// Use request context with timeout
	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ifMatch := string(ctx.Request.Header.Peek("If-Match"))
	ifNoneMatch := string(ctx.Request.Header.Peek("If-None-Match"))

	var putOpts []db.PutOption
	if ifNoneMatch == "*" {
		putOpts = append(putOpts, db.CreateOnly())
	} else if ifMatch != "" {
		putOpts = append(putOpts, db.WithExpectedETag(ifMatch))
	}

	start := time.Now()
	meta, err := s.db.Put(reqCtx, key, body, putOpts...)
	duration := time.Since(start).Seconds()

	if err != nil {
		if err == storage.ErrVersionMismatch || err == storage.ErrAlreadyExists {
			metrics.CASConflicts.WithLabelValues("put").Inc()
			metrics.RecordDBOperation("put", "cas_conflict", duration)
			ctx.Error("CAS precondition failed: "+err.Error(), fasthttp.StatusPreconditionFailed)
			return
		}
		metrics.RecordDBOperation("put", "error", duration)
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	metrics.RecordDBOperation("put", "success", duration)
	metrics.ObserveObjectSize(len(body))

	ctx.Response.Header.Set("ETag", meta.ETag)
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	respBody, _ := json.Marshal(meta)
	ctx.SetBody(respBody)
}

func (s *Server) handleDelete(ctx *fasthttp.RequestCtx, key string) {
	// Use request context with timeout
	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ifMatch := string(ctx.Request.Header.Peek("If-Match"))

	var delOpts []db.DeleteOption
	if ifMatch != "" {
		delOpts = append(delOpts, db.WithDeleteExpectedETag(ifMatch))
	}

	start := time.Now()
	err := s.db.Delete(reqCtx, key, delOpts...)
	duration := time.Since(start).Seconds()

	if err != nil {
		if err == storage.ErrNotFound {
			metrics.RecordDBOperation("delete", "not_found", duration)
			ctx.Error("document not found", fasthttp.StatusNotFound)
			return
		}
		if err == storage.ErrVersionMismatch {
			metrics.CASConflicts.WithLabelValues("delete").Inc()
			metrics.RecordDBOperation("delete", "cas_conflict", duration)
			ctx.Error("CAS precondition failed", fasthttp.StatusPreconditionFailed)
			return
		}
		metrics.RecordDBOperation("delete", "error", duration)
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	metrics.RecordDBOperation("delete", "success", duration)
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func (s *Server) handleList(ctx *fasthttp.RequestCtx, prefix string) {
	// Use request context with timeout
	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	limit := ctx.QueryArgs().GetUintOrZero("limit")
	if limit == 0 {
		limit = 100
	}

	// Security: Enforce maximum list limit to prevent resource exhaustion
	const maxListLimit = 10000
	if limit > maxListLimit {
		limit = maxListLimit
	}

	start := time.Now()
	items, err := s.db.List(reqCtx, prefix, limit)
	duration := time.Since(start).Seconds()

	if err != nil {
		metrics.RecordDBOperation("list", "error", duration)
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	metrics.RecordDBOperation("list", "success", duration)

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)

	buf := new(bytes.Buffer)
	_ = json.NewEncoder(buf).Encode(items)
	ctx.SetBody(buf.Bytes())
}

// ListenAndServe starts the FastHTTP server on addr.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = ln
	return fasthttp.Serve(ln, s.HandleRequest)
}

// Close gracefully stops the server listener.
func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
