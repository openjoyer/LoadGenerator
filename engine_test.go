package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testConfig(url string) Config {
	return Config{URL: url, Stages: []Stage{{100, 100 * time.Millisecond}}, Concurrency: 8, MemoryMiB: 1, Timeout: time.Second, RunID: "0123456789abcdef"}
}
func TestPayload(t *testing.T) {
	p, err := compilePayload([]byte(`{"id":"{{uuid}}","other":"{{uuid}}","time":"{{timestamp}}","seq":"{{sequence}}","text":"{{random:64}}"}`), true)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := runPrefix("0123456789abcdef")
	seen := map[string]bool{}
	for n := uint64(0); n < 10; n++ {
		p.update(prefix, n, 0)
		var body map[string]string
		if err := json.Unmarshal(p.body, &body); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"id", "other"} {
			id := body[k]
			if len(id) != 36 || id[14] != '4' || seen[id] {
				t.Fatal("invalid/duplicate uuid")
			}
			seen[id] = true
		}
		if len(body["text"]) != 64 {
			t.Fatal("size")
		}
		if _, err := time.Parse(time.RFC3339Nano, body["time"]); err != nil {
			t.Fatal(err)
		}
	}
}
func TestRun(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("method")
		}
		w.WriteHeader(204)
	}))
	defer s.Close()
	r, err := Run(context.Background(), testConfig(s.URL), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if r.Scheduled != 10 || r.Submitted+r.Missed != 10 || r.Started != r.Completed || r.Success == 0 {
		t.Fatalf("counters %+v", r)
	}
}
func TestOverloadAndCancellation(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(30 * time.Millisecond); w.WriteHeader(429) }))
	defer s.Close()
	c := testConfig(s.URL)
	c.Concurrency = 1
	c.Stages = []Stage{{10000, 100 * time.Millisecond}}
	r, err := Run(context.Background(), c, io.Discard)
	if err != nil || r.Missed == 0 || r.Success != 0 || r.Statuses[429] == 0 {
		t.Fatalf("overload %+v %v", r, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = Run(ctx, c, io.Discard)
	if err != nil || !r.Interrupted {
		t.Fatal("cancel")
	}
}
func TestRedirect(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/other", 302) }))
	defer s.Close()
	r, err := Run(context.Background(), testConfig(s.URL), io.Discard)
	if err != nil || r.Success != 0 || r.Statuses[302] == 0 {
		t.Fatal("redirect", err)
	}
}
func TestBounds(t *testing.T) {
	if requestCount(24*time.Hour, 1000000) != 86400000000 {
		t.Fatal("overflow")
	}
	for _, s := range []string{"0@1s", "10@0s", "bad"} {
		if _, err := parseStages(s); err == nil {
			t.Fatal("invalid stage")
		}
	}
	if _, err := compilePayload([]byte("{{random:99999999}}"), true); err == nil {
		t.Fatal("unbounded body")
	}
}
func TestHistogram(t *testing.T) {
	for _, v := range []int64{1, 16, 31, 32, 1000, 1000000, 1000000000} {
		u := upper(bucket(v))
		if u < v || float64(u) > float64(v)*1.07+1 {
			t.Fatal(v, u)
		}
	}
}
func BenchmarkPayload(b *testing.B) {
	text := `{"items":[` + strings.TrimSuffix(strings.Repeat(`{"id":"{{uuid}}","time":"{{timestamp}}","message":"{{random:512}}"},`, 100), ",") + ` ]}`
	p, err := compilePayload([]byte(text), true)
	if err != nil {
		b.Fatal(err)
	}
	prefix, _ := runPrefix("0123456789abcdef")
	b.SetBytes(int64(len(p.body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.update(prefix, uint64(i), 0)
	}
}
