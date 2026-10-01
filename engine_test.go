package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func testConfig(url string) Config {
	return Config{URL: url, Stages: []Stage{{100, 100 * time.Millisecond}}, Concurrency: 8, Batch: 2, MessageBytes: 64, Services: 3, MemoryMiB: 1, Timeout: time.Second, Format: "logflux", RunID: "0123456789abcdef"}
}
func TestPayload(t *testing.T) {
	c := testConfig("http://localhost")
	prefix, _ := runPrefix(c.RunID)
	seen := map[string]bool{}
	for _, format := range []string{"logflux", "legacy"} {
		c.Format = format
		p := newPayload(c, 0)
		for seq := uint64(0); seq < 2; seq++ {
			p.update(prefix, seq, c.Batch)
			var events []map[string]any
			if format == "logflux" {
				var body struct {
					Logs []map[string]any `json:"logs"`
				}
				if err := json.Unmarshal(p.body, &body); err != nil {
					t.Fatal(err)
				}
				events = body.Logs
			} else {
				if err := json.Unmarshal(p.body, &events); err != nil {
					t.Fatal(err)
				}
			}
			if len(events) != 2 {
				t.Fatal("batch size")
			}
			for _, e := range events {
				key := "eventId"
				if format == "legacy" {
					key = "event_id"
				}
				id := e[key].(string)
				if len(id) != 36 || id[14] != '4' {
					t.Fatal("invalid UUID")
				}
				if format == "logflux" {
					if seen[id] {
						t.Fatal("duplicate ID")
					}
					seen[id] = true
				}
				if _, err := time.Parse(time.RFC3339Nano, e["timestamp"].(string)); err != nil {
					t.Fatal(err)
				}
				if len(e["message"].(string)) != 64 {
					t.Fatal("message size")
				}
			}
		}
	}
}
func TestRun(t *testing.T) {
	var mu sync.Mutex
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Logs []map[string]any `json:"logs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Error("authorization")
		}
		mu.Lock()
		count++
		mu.Unlock()
		w.WriteHeader(202)
	}))
	defer server.Close()
	c := testConfig(server.URL)
	c.Key = "test"
	report, err := Run(context.Background(), c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if report.Scheduled != 10 || report.Submitted+report.Missed != 10 || report.Completed != report.Started || report.Success != uint64(count) || count == 0 {
		t.Fatalf("bad counters: %+v", report)
	}
}
func TestOverloadAndCancellation(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(30 * time.Millisecond); w.WriteHeader(429) }))
	defer s.Close()
	c := testConfig(s.URL)
	c.Concurrency = 1
	c.Stages = []Stage{{10000, 100 * time.Millisecond}}
	r, err := Run(context.Background(), c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if r.Missed == 0 || r.Success != 0 || r.Statuses[429] == 0 || r.Completed != r.Submitted {
		t.Fatalf("bad overload report: %+v", r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = Run(ctx, c, io.Discard)
	if err != nil || !r.Interrupted {
		t.Fatal("cancellation", err)
	}
}
func TestRedirectNotFollowed(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/other", 302) }))
	defer s.Close()
	r, err := Run(context.Background(), testConfig(s.URL), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if r.Statuses[302] == 0 || r.Success != 0 {
		t.Fatal("redirect incorrectly followed")
	}
}
func TestBounds(t *testing.T) {
	c := testConfig("http://localhost")
	c.Batch = 500
	c.MessageBytes = 10000
	if _, err := Run(context.Background(), c, io.Discard); err == nil {
		t.Fatal("oversize accepted")
	}
	if requestCount(24*time.Hour, 1000000) != 86400000000 {
		t.Fatal("rate overflow")
	}
	for _, s := range []string{"0@1s", "100@0s", "wat", "10@1h"} {
		_, err := parseStages(s)
		if s == "10@1h" && err != nil {
			t.Fatal(err)
		}
		if s != "10@1h" && err == nil {
			t.Fatal("invalid stage accepted")
		}
	}
}
func TestHistogram(t *testing.T) {
	for _, v := range []int64{1, 16, 31, 32, 1000, 1000000, 1000000000} {
		u := upper(bucket(v))
		if u < v || float64(u) > float64(v)*1.07+1 {
			t.Fatalf("%d -> %d", v, u)
		}
	}
}
func BenchmarkPayload(b *testing.B) {
	c := testConfig("http://localhost")
	c.Batch = 100
	c.MessageBytes = 512
	p := newPayload(c, 0)
	prefix, _ := runPrefix(c.RunID)
	b.SetBytes(int64(len(p.body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.update(prefix, uint64(i), c.Batch)
	}
}
