package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		urlStr      = flag.String("url", "", "Target Function URL, e.g. https://...run.app (must accept POST)")
		n           = flag.Int("n", 1_000_000, "Number of requests")
		concurrency = flag.Int("c", 2000, "Number of concurrent workers")
		timeout     = flag.Duration("timeout", 10*time.Second, "Per-request timeout")
		maxBody     = flag.Int64("max-body", 1<<20, "Max response body bytes to read (safety)")
		seed        = flag.Int64("seed", 0, "Random seed (0 = time-based)")
		prec        = flag.Int("prec", 6, "Float precision for lat/lng in JSON (decimal places)")
	)
	flag.Parse()

	if *urlStr == "" {
		fmt.Fprintln(os.Stderr, "Missing -url")
		os.Exit(1)
	}
	if *n <= 0 || *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "-n and -c must be > 0")
		os.Exit(1)
	}
	if *prec < 0 || *prec > 15 {
		fmt.Fprintln(os.Stderr, "-prec should be between 0 and 15")
		os.Exit(1)
	}

	actualSeed := *seed
	if actualSeed == 0 {
		actualSeed = time.Now().UnixNano()
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,

		ForceAttemptHTTP2: true,

		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 10000,
		MaxConnsPerHost:     10000,

		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client := &http.Client{Transport: transport}

	latencies := make([]int64, *n) // ns for successful (2xx) requests only
	var (
		nextIdx     uint64
		okCount     uint64
		errCount    uint64
		status4xx   uint64
		status5xx   uint64
		statusOther uint64
	)
	var firstErr atomic.Value

	// Start barrier so workers begin together
	startCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(*concurrency)

	// Reuse buffers to reduce allocations
	bufPool := sync.Pool{
		New: func() any { return new(bytes.Buffer) },
	}

	beginAll := time.Now()

	for w := 0; w < *concurrency; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			<-startCh

			// One RNG per worker to avoid locks/contention
			rng := rand.New(rand.NewSource(actualSeed + int64(workerID)*1_000_003))

			for {
				i := int(atomic.AddUint64(&nextIdx, 1) - 1)
				if i >= *n {
					return
				}

				// Build random payload (4 points)
				buf := bufPool.Get().(*bytes.Buffer)
				buf.Reset()
				writeRandomPayload(buf, rng, *prec)
				payload := buf.Bytes()

				ctx, cancel := context.WithTimeout(context.Background(), *timeout)
				start := time.Now()

				req, err := http.NewRequestWithContext(ctx, http.MethodPost, *urlStr, bytes.NewReader(payload))
				if err != nil {
					cancel()
					bufPool.Put(buf)
					atomic.AddUint64(&errCount, 1)
					storeFirstErr(&firstErr, fmt.Errorf("new request: %w", err))
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err != nil {
					cancel()
					bufPool.Put(buf)
					atomic.AddUint64(&errCount, 1)
					storeFirstErr(&firstErr, fmt.Errorf("do request: %w", err))
					continue
				}

				// Read & discard body (critical for keep-alive reuse)
				_, _ = io.CopyN(io.Discard, resp.Body, *maxBody)
				_ = resp.Body.Close()
				cancel()

				// Done with buffer
				bufPool.Put(buf)

				dur := time.Since(start)

				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					latencies[i] = dur.Nanoseconds()
					atomic.AddUint64(&okCount, 1)
				} else {
					atomic.AddUint64(&errCount, 1)
					switch {
					case resp.StatusCode >= 400 && resp.StatusCode < 500:
						atomic.AddUint64(&status4xx, 1)
					case resp.StatusCode >= 500 && resp.StatusCode < 600:
						atomic.AddUint64(&status5xx, 1)
					default:
						atomic.AddUint64(&statusOther, 1)
					}
				}
			}
		}()
	}

	close(startCh)
	wg.Wait()

	totalDur := time.Since(beginAll)

	ok := int(atomic.LoadUint64(&okCount))
	errs := int(atomic.LoadUint64(&errCount))

	// Collect OK latencies
	okLat := make([]int64, 0, ok)
	for _, ns := range latencies {
		if ns > 0 {
			okLat = append(okLat, ns)
		}
	}

	// Report
	fmt.Println("==== Load Test Result ====")
	fmt.Printf("Go: %s | CPUs: %d | GOMAXPROCS: %d\n", runtime.Version(), runtime.NumCPU(), runtime.GOMAXPROCS(0))
	fmt.Printf("Target URL: %s\n", *urlStr)
	fmt.Printf("Requests: %d | Concurrency(workers): %d\n", *n, *concurrency)
	fmt.Printf("Seed: %d\n", actualSeed)
	fmt.Printf("Total time: %s\n", totalDur)
	fmt.Printf("OK: %d | Errors: %d\n", ok, errs)

	if errs > 0 {
		fmt.Printf("Errors breakdown: 4xx=%d 5xx=%d other=%d\n",
			atomic.LoadUint64(&status4xx),
			atomic.LoadUint64(&status5xx),
			atomic.LoadUint64(&statusOther),
		)
		if v := firstErr.Load(); v != nil {
			fmt.Printf("First error: %v\n", v.(error))
		}
	}

	rps := float64(ok+errs) / totalDur.Seconds()
	fmt.Printf("Throughput (total): %.2f req/s\n", rps)

	if len(okLat) == 0 {
		fmt.Println("No successful requests to compute latency stats.")
		return
	}

	sort.Slice(okLat, func(i, j int) bool { return okLat[i] < okLat[j] })

	var sum int64
	for _, ns := range okLat {
		sum += ns
	}
	min := okLat[0]
	max := okLat[len(okLat)-1]
	avg := float64(sum) / float64(len(okLat))

	fmt.Println("---- Latency (successful requests) ----")
	fmt.Printf("Count: %d\n", len(okLat))
	fmt.Printf("Min: %s\n", time.Duration(min))
	fmt.Printf("Avg: %s\n", time.Duration(int64(avg)))
	fmt.Printf("Max: %s\n", time.Duration(max))
	fmt.Printf("p50: %s\n", time.Duration(percentile(okLat, 0.50)))
	fmt.Printf("p90: %s\n", time.Duration(percentile(okLat, 0.90)))
	fmt.Printf("p95: %s\n", time.Duration(percentile(okLat, 0.95)))
	fmt.Printf("p99: %s\n", time.Duration(percentile(okLat, 0.99)))
}

// Generates 4 random points globally: lat [-90,90], lng [-180,180]
func writeRandomPayload(buf *bytes.Buffer, rng *rand.Rand, prec int) {
	buf.WriteString(`{"points":[`)
	for i := 0; i < 4; i++ {
		lat := -90.0 + rng.Float64()*180.0
		lng := -180.0 + rng.Float64()*360.0

		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(`{"lat":`)
		buf.WriteString(strconv.FormatFloat(lat, 'f', prec, 64))
		buf.WriteString(`,"lng":`)
		buf.WriteString(strconv.FormatFloat(lng, 'f', prec, 64))
		buf.WriteByte('}')
	}
	buf.WriteString(`]}`)
}

func percentile(sortedNs []int64, p float64) int64 {
	if len(sortedNs) == 0 {
		return 0
	}
	if p <= 0 {
		return sortedNs[0]
	}
	if p >= 1 {
		return sortedNs[len(sortedNs)-1]
	}
	rank := int(math.Ceil(p*float64(len(sortedNs)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sortedNs) {
		rank = len(sortedNs) - 1
	}
	return sortedNs[rank]
}

func storeFirstErr(slot *atomic.Value, err error) {
	if slot.Load() == nil {
		slot.Store(err)
	}
}
