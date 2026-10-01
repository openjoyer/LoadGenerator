package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	Targets     []Target
	URL         string
	Stages      []Stage
	Concurrency int
	MemoryMiB   int
	Timeout     time.Duration
	Key, RunID  string
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
	if c.Concurrency < 1 || c.Concurrency > 100000 || c.MemoryMiB < 1 || c.MemoryMiB > 65536 || c.Timeout <= 0 {
		return fmt.Errorf("invalid concurrency, memory or timeout")
	}
	if err := validateTargets(c.Targets); err != nil {
		return err
	}
	for _, stage := range c.Stages {
		if stage.RPS < 1 || stage.RPS > 1000000 || stage.Duration < time.Millisecond || stage.Duration > 24*time.Hour {
			return fmt.Errorf("invalid stage: RPS 1..1000000, duration 1ms..24h required")
		}
	}
	if len(c.Stages) == 0 {
		return fmt.Errorf("at least one stage required")
	}
	return nil
}

func main() {
	if err := execute(); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func execute() error {
	opts, err := loadOptions(os.Args[1:])
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if opts.Sink != "" {
		return serveSink(ctx, opts.Sink)
	}
	c, reportPath := opts.Config, opts.Report
	printPlan(os.Stdout, c, reportPath)
	if opts.Check {
		fmt.Println("Configuration OK")
		return nil
	}
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
		defer r.Body.Close()
		if _, err := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, 16<<20)); err != nil {
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
