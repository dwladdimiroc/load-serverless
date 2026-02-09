package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

const (
	FunctionBackendURL = "https://us-east1-powerful-vine-486914-k3.cloudfunctions.net/geo_average"
	VMBackendURL       = "http://10.142.0.3:8080"

	// Where the broker listens
	ListenAddr = ":8080"

	// Buffer size to allow safe retry on POST/PUT/PATCH
	MaxBodyBytes = int64(2 << 20) // 2MB
)

type Backend struct {
	Name  string
	Proxy *httputil.ReverseProxy
}

type Broker struct {
	backends []Backend
	rr       atomic.Uint64
}

func main() {
	functionURL := mustParseURL(FunctionBackendURL)
	vmURL := mustParseURL(VMBackendURL)

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,

		ForceAttemptHTTP2: true,

		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 2000,
		MaxConnsPerHost:     2000,

		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	functionProxy := newReverseProxy("function", functionURL, transport)
	vmProxy := newReverseProxy("vm", vmURL, transport)

	b := &Broker{
		backends: []Backend{
			{Name: "function", Proxy: functionProxy},
			{Name: "vm", Proxy: vmProxy},
		},
	}

	mux := http.NewServeMux()

	// Broker health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Proxy everything else (path is preserved for both backends)
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Buffer body for safe retry (POST/PUT/PATCH)
		var bodyCopy []byte
		var err error
		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			bodyCopy, err = readUpTo(r.Body, MaxBodyBytes)
			if err != nil {
				http.Error(w, "Request body too large or invalid", http.StatusRequestEntityTooLarge)
				return
			}
		}

		// Round robin selection
		i := int(b.rr.Add(1) % uint64(len(b.backends)))
		first := b.backends[i]
		second := b.backends[(i+1)%len(b.backends)]

		// Try first, then failover
		if serveBackend(first, w, r, bodyCopy) {
			return
		}
		if serveBackend(second, w, r, bodyCopy) {
			return
		}

		http.Error(w, "Both backends failed", http.StatusBadGateway)
	}))

	srv := &http.Server{
		Addr:              ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Broker listening on %s", ListenAddr)
	log.Printf("Function base: %s", functionURL.String())
	log.Printf("VM base:       %s", vmURL.String())
	log.Fatal(srv.ListenAndServe())
}

func serveBackend(be Backend, w http.ResponseWriter, r *http.Request, bodyCopy []byte) bool {
	r2 := r.Clone(r.Context())

	// Restore body for retry
	if bodyCopy != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
		r2.Body = io.NopCloser(bytes.NewReader(bodyCopy))
		r2.ContentLength = int64(len(bodyCopy))
	}

	rec := &statusRecorder{ResponseWriter: w, code: 0}
	be.Proxy.ServeHTTP(rec, r2)

	// Treat these as failures to trigger failover
	if rec.code == http.StatusBadGateway || rec.code == http.StatusServiceUnavailable || rec.code == http.StatusGatewayTimeout {
		log.Printf("Backend %s returned %d -> failover", be.Name, rec.code)
		return false
	}

	// If WriteHeader was never called, code remains 0 (assume OK)
	return true
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

func newReverseProxy(name string, target *url.URL, transport http.RoundTripper) *httputil.ReverseProxy {
	p := httputil.NewSingleHostReverseProxy(target)

	orig := p.Director
	p.Director = func(r *http.Request) {
		orig(r)
		// Useful forwarding headers
		r.Header.Set("X-Forwarded-Host", r.Host)
		if r.TLS != nil {
			r.Header.Set("X-Forwarded-Proto", "https")
		} else {
			r.Header.Set("X-Forwarded-Proto", "http")
		}
	}

	p.Transport = transport
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error (%s): %v", name, err)
		http.Error(w, "Bad gateway", http.StatusBadGateway)
	}
	return p
}

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		log.Fatalf("Invalid backend URL constant: %q", s)
	}
	return u
}

func readUpTo(rc io.ReadCloser, max int64) ([]byte, error) {
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}
