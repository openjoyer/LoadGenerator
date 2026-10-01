package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/bits"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const buckets = 1024

type metrics struct {
	mu                                                                sync.Mutex
	started, completed, success, networkErrors, responseErrors, bytes uint64
	statuses                                                          map[int]uint64
	latency, lag                                                      [buckets]uint64
	maxLatency, maxLag                                                int64
}
type Report struct {
	RunID                                                     string  `json:"run_id"`
	Interrupted                                               bool    `json:"interrupted"`
	Stages                                                    []Stage `json:"stages"`
	Scheduled, Submitted, Started, Completed, Success, Missed uint64
	TransportErrors, ResponseErrors                           uint64
	SubmittedPayloadBytes                                     uint64
	EventsPerRequest, PayloadBytes, Concurrency               int
	Statuses                                                  map[int]uint64
	LatencyP50, LatencyP95, LatencyP99                        string
	StartLagP99                                               string
	LoadSeconds, TotalSeconds, AchievedStartRPS               float64
	HeapBytes                                                 uint64
	Notes                                                     string
}

func bucket(ns int64) int {
	if ns <= 0 {
		return 0
	}
	x := uint64(ns)
	b := bits.Len64(x)
	if b <= 4 {
		return int(x)
	}
	shift := b - 5
	idx := 16 + shift*16 + int((x>>shift)-16)
	if idx >= buckets {
		return buckets - 1
	}
	return idx
}
func upper(i int) int64 {
	if i < 16 {
		return int64(i)
	}
	s := (i - 16) / 16
	mant := 16 + (i-16)%16
	return int64(uint64(mant+1)<<s) - 1
}
func percentile(h [buckets]uint64, total uint64, q float64, max int64) time.Duration {
	if total == 0 {
		return 0
	}
	rank := uint64(float64(total) * q)
	if float64(rank) < float64(total)*q {
		rank++
	}
	var n uint64
	for i, v := range h {
		n += v
		if n >= rank {
			u := upper(i)
			if u > max {
				u = max
			}
			return time.Duration(u)
		}
	}
	return time.Duration(max)
}
func snapshot(all []*metrics) *metrics {
	out := &metrics{statuses: map[int]uint64{}}
	for _, m := range all {
		m.mu.Lock()
		out.started += m.started
		out.completed += m.completed
		out.success += m.success
		out.networkErrors += m.networkErrors
		out.responseErrors += m.responseErrors
		out.bytes += m.bytes
		for k, v := range m.statuses {
			out.statuses[k] += v
		}
		for i := range out.latency {
			out.latency[i] += m.latency[i]
			out.lag[i] += m.lag[i]
		}
		if m.maxLatency > out.maxLatency {
			out.maxLatency = m.maxLatency
		}
		if m.maxLag > out.maxLag {
			out.maxLag = m.maxLag
		}
		m.mu.Unlock()
	}
	return out
}

type job struct {
	sequence uint64
	due      time.Time
}

func Run(ctx context.Context, c Config, output io.Writer) (Report, error) {
	if err := c.validate(); err != nil {
		return Report{}, err
	}
	prefix, err := runPrefix(c.RunID)
	if err != nil {
		return Report{}, err
	}
	sample := newPayload(c, 0)
	size := len(sample.body)
	if size > 1<<20 {
		return Report{}, fmt.Errorf("payload %d bytes exceeds 1 MiB", size)
	}
	allocationSize := cap(sample.body)
	workers := c.Concurrency
	budget := int64(c.MemoryMiB) << 20
	if int64(workers)*int64(allocationSize) > budget {
		workers = int(budget / int64(allocationSize))
	}
	if workers < 1 {
		return Report{}, fmt.Errorf("payload memory budget too small")
	}
	fmt.Fprintf(output, "run=%s workers=%d payload≈%d bytes batch=%d; no application retries; HTTP/1.1\n", c.RunID, workers, size, c.Batch)
	transport := &http.Transport{
		Proxy:        http.ProxyFromEnvironment,
		DialContext:  (&net.Dialer{Timeout: c.Timeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: workers, MaxIdleConnsPerHost: workers, MaxConnsPerHost: workers,
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: c.Timeout, ResponseHeaderTimeout: c.Timeout,
		ForceAttemptHTTP2: false,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	jobs := make(chan job, workers)
	slots := make(chan struct{}, workers)
	all := make([]*metrics, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		m := &metrics{statuses: map[int]uint64{}}
		all[w] = m
		p := newPayload(c, w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				p.update(prefix, j.sequence, c.Batch)
				reader := bytes.NewReader(p.body)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, io.NopCloser(reader))
				if err != nil {
					<-slots
					continue
				}
				req.ContentLength = int64(len(p.body))
				req.Header.Set("Content-Type", "application/json")
				if c.Key != "" {
					req.Header.Set("Authorization", "Bearer "+c.Key)
				}
				started := time.Now()
				lag := started.Sub(j.due).Nanoseconds()
				if lag < 0 {
					lag = 0
				}
				m.mu.Lock()
				m.started++
				m.bytes += uint64(len(p.body))
				m.lag[bucket(lag)]++
				if lag > m.maxLag {
					m.maxLag = lag
				}
				m.mu.Unlock()
				resp, sendErr := client.Do(req)
				status := 0
				var bodyErr error
				if resp != nil {
					status = resp.StatusCode
					n, e := io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024+1))
					bodyErr = e
					if n > 64*1024 {
						bodyErr = fmt.Errorf("response too large")
					}
					_ = resp.Body.Close()
				}
				latency := time.Since(started).Nanoseconds()
				m.mu.Lock()
				m.completed++
				m.latency[bucket(latency)]++
				if latency > m.maxLatency {
					m.maxLatency = latency
				}
				if status != 0 {
					m.statuses[status]++
				}
				if sendErr != nil {
					m.networkErrors++
				} else if bodyErr != nil {
					m.responseErrors++
				} else if status >= 200 && status < 300 {
					m.success++
				}
				m.mu.Unlock()
				<-slots
			}
		}()
	}
	start := time.Now()
	var scheduled, submitted, missed atomic.Uint64
	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		last := time.Now()
		var prev uint64
		for {
			select {
			case <-monitorCtx.Done():
				return
			case now := <-ticker.C:
				m := snapshot(all)
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				fmt.Fprintf(output, "started/s=%.0f completed=%d success=%d missed=%d p99=%s heap=%.1fMiB inflight=%d\n", float64(m.started-prev)/now.Sub(last).Seconds(), m.completed, m.success, missed.Load(), percentile(m.latency, m.completed, .99, m.maxLatency), float64(mem.HeapAlloc)/(1<<20), m.started-m.completed)
				prev = m.started
				last = now
			}
		}
	}()
	var sequence uint64
	interrupted := false
	ticker := time.NewTicker(time.Millisecond)
	stageStart := start
outer:
	for _, stage := range c.Stages {
		total := requestCount(stage.Duration, stage.RPS)
		var accounted uint64
		for accounted < total {
			select {
			case <-ctx.Done():
				interrupted = true
				break outer
			case <-ticker.C:
			}
			elapsed := time.Since(stageStart)
			if elapsed < 0 {
				continue
			}
			if elapsed > stage.Duration {
				elapsed = stage.Duration
			}
			due := requestCount(elapsed, stage.RPS)
			if due > total {
				due = total
			}
			pending := due - accounted
			if pending == 0 {
				continue
			}
			scheduled.Add(pending)
			if pending > uint64(workers) {
				skip := pending - uint64(workers)
				missed.Add(skip)
				sequence += skip
				accounted += skip
				pending = uint64(workers)
			}
			for k := uint64(0); k < pending; k++ {
				nominal := stageStart.Add(requestOffset(accounted+1, stage.RPS))
				select {
				case slots <- struct{}{}:
					jobs <- job{sequence, nominal}
					submitted.Add(1)
				default:
					missed.Add(1)
				}
				sequence++
				accounted++
			}
		}
		if delay := time.Until(stageStart.Add(stage.Duration)); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				interrupted = true
				break outer
			case <-timer.C:
			}
		}
		stageStart = stageStart.Add(stage.Duration)
	}
	ticker.Stop()
	loadSeconds := time.Since(start).Seconds()
	close(jobs)
	wg.Wait()
	stopMonitor()
	<-monitorDone
	m := snapshot(all)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	r := Report{RunID: c.RunID, Interrupted: interrupted, Stages: c.Stages, Scheduled: scheduled.Load(), Submitted: submitted.Load(), Started: m.started, Completed: m.completed, Success: m.success, Missed: missed.Load(), TransportErrors: m.networkErrors, ResponseErrors: m.responseErrors, SubmittedPayloadBytes: m.bytes, EventsPerRequest: c.Batch, PayloadBytes: size, Concurrency: workers, Statuses: m.statuses, LatencyP50: percentile(m.latency, m.completed, .5, m.maxLatency).String(), LatencyP95: percentile(m.latency, m.completed, .95, m.maxLatency).String(), LatencyP99: percentile(m.latency, m.completed, .99, m.maxLatency).String(), StartLagP99: percentile(m.lag, m.started, .99, m.maxLag).String(), LoadSeconds: loadSeconds, TotalSeconds: time.Since(start).Seconds(), AchievedStartRPS: float64(m.started) / loadSeconds, HeapBytes: mem.HeapAlloc, Notes: "Success means HTTP 2xx, not persisted events. Payload bytes are attempted, not measured network bytes. Latencies include body reading; start lag is separate. Histogram percentiles are approximate upper bounds. Stages durations are nanoseconds. Random messages are a synthetic compression stress profile."}
	fmt.Fprintf(output, "finished: started=%d missed=%d success=%d actual=%.0fRPS p99=%s start_lag_p99=%s\n", r.Started, r.Missed, r.Success, r.AchievedStartRPS, r.LatencyP99, r.StartLagP99)
	return r, nil
}

func requestCount(d time.Duration, rps int) uint64 {
	return uint64(d/time.Second)*uint64(rps) + uint64(d%time.Second)*uint64(rps)/uint64(time.Second)
}

func requestOffset(n uint64, rps int) time.Duration {
	r := uint64(rps)
	return time.Duration(n/r)*time.Second + time.Duration((n%r)*uint64(time.Second)/r)
}
