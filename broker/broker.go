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
	// ✅ Google Function URL (PUBLIC, no token).
	// - If your function URL is already the endpoint, put it exactly as GCP gives it.
	//   Examples:
	//   Cloud Run function style:  "https://your-function-xyz.a.run.app"
	//   Cloud Functions gen1 style: "https://REGION-PROJECT.cloudfunctions.net/functionGet"
	FunctionBackendURL = "https://REPLACE_ME_WITH_YOUR_FUNCTION_URL"

	// ✅ VM backend base URL (NO path here).
	// Example: "http://121.42.13.82:8080"
	VMBackendURL = "http://121.42.13.82:8080"

	// Broker listen address on the VM
	ListenAddr = ":8080"

	// Max buffered body size (for safe retry on POST/PUT/PATCH)
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
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// PROXIES:
	// - VM: preserves incoming path (so /functionGet stays /functionGet)
	// - Function: fixed path (ignores incoming path; uses functionURL.Path or "/")
	vmProxy := newReverseProxyPreservePath("vm", vmURL, transport)
	functionProxy := newReverseProxyFixedPath("function", functionURL, transport)

	b := &Broker{
		backends: []Backend{
			{Name: "function", Proxy: functionProxy},
			{Name: "vm", Proxy: vmProxy},
		},
	}

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Proxy everything else
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Buffer body for safe retry (important for POST)
		var bodyCopy []byte
		var err error

		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			bodyCopy, err = readUpTo(r.Body, MaxBodyBytes)
			if err != nil {
				http.Error(w, "Request body too large or invalid", http.StatusRequestEntityTooLarge)
				return
			}
		}

		// Round robin: alternate backends
		i := int(b.rr.Add(1) % uint64(len(b.backends)))
		first := b.backends[i]
		second := b.backends[(i+1)%len(b.backends)]

		// Try chosen backend, then failover
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
	log.Printf("Function URL: %s", functionURL.String())
	log.Printf("VM URL:       %s", vmURL.String())
	log.Fatal(srv.ListenAndServe())
}

func serveBackend(be Backend, w http.ResponseWriter, r *http.Request, bodyCopy []byte) bool {
	r2 := r.Clone(r.Context())

	// Restore buffered body so we can retry safely
	if bodyCopy != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
		r2.Body = io.NopCloser(bytes.NewReader(bodyCopy))
		r2.ContentLength = int64(len(bodyCopy))
	}

	rec := &statusRecorder{ResponseWriter: w, code: 0}
	be.Proxy.ServeHTTP(rec, r2)

	// Consider these as backend failures to trigger failover
	if rec.code == http.StatusBadGateway || rec.code == http.StatusServiceUnavailable || rec.code == http.StatusGatewayTimeout {
		log.Printf("Backend returned %d -> failover", rec.code)
		return false
	}

	// If WriteHeader was never called, code stays 0 (assume success)
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

// VM proxy: keep the incoming path.
// /functionGet -> http://VM_HOST:PORT/functionGet
func newReverseProxyPreservePath(name string, target *url.URL, transport http.RoundTripper) *httputil.ReverseProxy {
	p := httputil.NewSingleHostReverseProxy(target)
	p.Transport = transport
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error (%s): %v", name, err)
		http.Error(w, "Bad gateway", http.StatusBadGateway)
	}
	return p
}

// Function proxy: ignore the incoming path and force the target path.
// - If target URL has a path already (e.g. /functionGet), we use that exactly.
// - If target URL has no path, we use "/".
func newReverseProxyFixedPath(name string, target *url.URL, transport http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			// Set scheme + host to the function backend
			r.URL.Scheme = target.Scheme
			r.URL.Host = target.Host
			r.Host = target.Host

			// Force path (ignore incoming r.URL.Path)
			if target.Path != "" {
				r.URL.Path = target.Path
			} else {
				r.URL.Path = "/"
			}

			// Keep the query string from the original request (r.URL.RawQuery)
			// (We don't modify it.)
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("Proxy error (%s): %v", name, err)
			http.Error(w, "Bad gateway", http.StatusBadGateway)
		},
	}
}

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		log.Fatalf("Invalid URL constant: %q", s)
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
