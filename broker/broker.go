package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

const (
	FunctionBackendURL = "https://us-east1-powerful-vine-486914-k3.cloudfunctions.net"
	VMBackendURL       = "http://10.142.0.3:8080"

	ListenAddr   = ":8080"
	MaxBodyBytes = int64(2 << 20) // 2MB
)

type Backend struct {
	Name      string // "serverless" or "vm"
	BaseURL   *url.URL
	Transport http.RoundTripper
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

	b := &Broker{
		backends: []Backend{
			{Name: "serverless", BaseURL: functionURL, Transport: transport},
			{Name: "vm", BaseURL: vmURL, Transport: transport},
		},
	}

	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Main proxy handler (preserves path for both)
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Buffer body to allow retry on POST/PUT/PATCH
		var bodyCopy []byte
		var err error
		if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			bodyCopy, err = readUpTo(r.Body, MaxBodyBytes)
			if err != nil {
				http.Error(w, "Request body too large or invalid", http.StatusRequestEntityTooLarge)
				return
			}
		}

		// Round robin
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
	log.Printf("Serverless base: %s", functionURL.String())
	log.Printf("VM base:         %s", vmURL.String())
	log.Fatal(srv.ListenAndServe())
}

// serveBackend forwards the request to the chosen backend.
// It sets response headers to indicate which backend was used and the final URL.
func serveBackend(be Backend, w http.ResponseWriter, r *http.Request, bodyCopy []byte) bool {
	// Build final destination URL: base + incoming path + query
	targetURL := joinURL(be.BaseURL, r.URL.Path, r.URL.RawQuery)

	// Create outbound request
	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, nil)
	if err != nil {
		log.Printf("request build error (%s): %v", be.Name, err)
		return false
	}

	// Copy headers (excluding Hop-by-hop headers)
	copyHeaders(outReq.Header, r.Header)
	outReq.Host = be.BaseURL.Host

	// Restore body if needed
	if bodyCopy != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
		outReq.Body = io.NopCloser(bytes.NewReader(bodyCopy))
		outReq.ContentLength = int64(len(bodyCopy))
	} else {
		// For GET etc, forward original body if any (rare), else nil
		if r.Body != nil && r.Body != http.NoBody {
			// Not buffering for methods other than POST/PUT/PATCH
			outReq.Body = r.Body
		}
	}

	// Do request
	resp, err := (&http.Client{Transport: be.Transport}).Do(outReq)
	if err != nil {
		log.Printf("backend call error (%s) url=%s err=%v", be.Name, targetURL, err)
		return false
	}
	defer func() { _ = resp.Body.Close() }()

	// If upstream is "bad gateway-ish", allow failover
	if resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
		log.Printf("backend %s returned %d url=%s -> failover", be.Name, resp.StatusCode, targetURL)
		return false
	}

	// ---- IMPORTANT: write headers BEFORE writing body ----
	// Indicate which backend served the request + the final URL used
	w.Header().Set("X-Selected-Backend", be.Name) // "serverless" or "vm"
	w.Header().Set("X-Selected-URL", targetURL)

	// Copy upstream headers to client (you can filter if you want)
	copyHeaders(w.Header(), resp.Header)

	// Write status code
	w.WriteHeader(resp.StatusCode)

	// Stream body
	_, _ = io.Copy(w, resp.Body)

	// Log (optional)
	// log.Printf("served via=%s url=%s status=%d", be.Name, targetURL, resp.StatusCode)

	return true
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

// joinURL combines base URL + path + query into a full URL string.
func joinURL(base *url.URL, path string, rawQuery string) string {
	u := *base // copy
	if path == "" {
		path = "/"
	}
	// Ensure proper slashes:
	// base.Path usually empty; we want base + incoming path
	if u.Path == "" || u.Path == "/" {
		u.Path = path
	} else {
		// If base has path and incoming has path, join safely
		u.Path = stringsTrimRightSlash(u.Path) + "/" + stringsTrimLeftSlash(path)
	}
	u.RawQuery = rawQuery
	return u.String()
}

func stringsTrimLeftSlash(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}

func stringsTrimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

// copyHeaders copies headers from src to dst, skipping hop-by-hop headers.
func copyHeaders(dst, src http.Header) {
	// Hop-by-hop headers per RFC 7230 section 6.1
	// We remove them to avoid proxy issues.
	hopByHop := map[string]bool{
		"Connection":          true,
		"Proxy-Connection":    true,
		"Keep-Alive":          true,
		"Proxy-Authenticate":  true,
		"Proxy-Authorization": true,
		"Te":                  true,
		"Trailer":             true,
		"Transfer-Encoding":   true,
		"Upgrade":             true,
	}

	for k, vv := range src {
		if hopByHop[k] {
			continue
		}
		// Don't forward our own selection headers from client
		if k == "X-Selected-Backend" || k == "X-Selected-URL" {
			continue
		}
		dst.Del(k)
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
