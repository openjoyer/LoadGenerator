package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Stage struct {
	RPS      int
	Duration time.Duration
}
type Config struct {
	URL                                        string
	Stages                                     []Stage
	Concurrency, Batch, MessageBytes, Services int
	MemoryMiB                                  int
	Timeout                                    time.Duration
	Format, Key, RunID                         string
}

func parseStages(s string) ([]Stage, error) {
	var result []Stage
	for _, part := range strings.Split(s, ",") {
		fields := strings.SplitN(part, "@", 2)
		if len(fields) != 2 {
			return nil, fmt.Errorf("stage must be RPS@duration")
		}
		rate, err := strconv.Atoi(fields[0])
		if err != nil || rate < 1 || rate > 1000000 {
			return nil, fmt.Errorf("RPS must be 1..1000000")
		}
		duration, err := time.ParseDuration(fields[1])
		if err != nil || duration < time.Millisecond || duration > 24*time.Hour {
			return nil, fmt.Errorf("stage duration must be 1ms..24h")
		}
		result = append(result, Stage{rate, duration})
	}
	return result, nil
}

func (c Config) validate() error {
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return fmt.Errorf("url must be HTTP(S), without embedded credentials")
	}
	if c.Concurrency < 1 || c.Concurrency > 100000 || c.Batch < 1 || c.Batch > 500 || c.MessageBytes < 1 || c.MessageBytes > 10000 || c.Services < 1 || c.Services > 10000 || c.MemoryMiB < 1 || c.MemoryMiB > 65536 || c.Timeout <= 0 {
		return fmt.Errorf("invalid limits: concurrency 1..100000, batch 1..500, message-bytes 1..10000, services 1..10000, memory-mib 1..65536; timeout > 0")
	}
	if c.Format != "logflux" && c.Format != "legacy" {
		return fmt.Errorf("format must be logflux or legacy")
	}
	if len(c.Stages) == 0 {
		return fmt.Errorf("at least one stage required")
	}
	return nil
}

func main() {
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func execute() error {
	var c Config
	var stages, keyEnv, reportPath, sink string
	flag.StringVar(&c.URL, "url", "http://localhost:8080/api/v1/logs", "target endpoint")
	flag.StringVar(&stages, "stages", "100@10s", "comma-separated RPS@duration, e.g. 100@30s,1000@60s")
	flag.IntVar(&c.Concurrency, "concurrency", 128, "maximum requests in flight")
	flag.IntVar(&c.Batch, "batch", 100, "events per request")
	flag.IntVar(&c.MessageBytes, "message-bytes", 512, "message field bytes, not total event size")
	flag.IntVar(&c.Services, "services", 20, "number of synthetic service names")
	flag.IntVar(&c.MemoryMiB, "memory-mib", 128, "budget for reusable payload buffers, not process RSS")
	flag.DurationVar(&c.Timeout, "timeout", 5*time.Second, "whole request timeout")
	flag.StringVar(&c.Format, "format", "logflux", "logflux: camelCase envelope; legacy: snake_case array")
	flag.StringVar(&keyEnv, "key-env", "LOGFLUX_API_KEY", "environment variable containing Bearer token")
	flag.StringVar(&reportPath, "report", "report.json", "JSON report path (replaced if it exists)")
	flag.StringVar(&sink, "sink", "", "run a local body-draining test server, e.g. 127.0.0.1:8090")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if sink != "" {
		return serveSink(ctx, sink)
	}
	var err error
	c.Stages, err = parseStages(stages)
	if err != nil {
		return err
	}
	if err = c.validate(); err != nil {
		return err
	}
	c.Key = os.Getenv(keyEnv)
	var id [8]byte
	if _, err = rand.Read(id[:]); err != nil {
		return err
	}
	c.RunID = hex.EncodeToString(id[:])
	report, err := Run(ctx, c, os.Stdout)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(reportPath, append(data, '\n'), 0600); err != nil {
		return err
	}
	fmt.Printf("report: %s\n", reportPath)
	return nil
}

func serveSink(ctx context.Context, address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		defer r.Body.Close()
		if _, err := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, 1<<20)); err != nil {
			w.WriteHeader(413)
			return
		}
		w.WriteHeader(202)
	})}
	fmt.Printf("Synthetic sink: http://%s (drains bodies; does NOT validate or store logs)\n", listener.Addr())
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			return err
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
	}
	return nil
}
