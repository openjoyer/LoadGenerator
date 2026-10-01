package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"
)

type Target struct {
	Name             string            `yaml:"name"`
	URL              string            `yaml:"url"`
	Weight           int               `yaml:"weight"`
	Method           string            `yaml:"method"`
	Headers          map[string]string `yaml:"headers"`
	HeadersEnv       map[string]string `yaml:"headers_env"`
	KeyEnv           string            `yaml:"api_key_env"`
	Key              string            `yaml:"-" json:"-"`
	Body             *string           `yaml:"body"`
	BodyFile         string            `yaml:"body_file"`
	BodyTemplate     *string           `yaml:"body_template"`
	BodyTemplateFile string            `yaml:"body_template_file"`
	ExpectedStatus   []int             `yaml:"expected_status"`
	compiled         *payload
}
type scenario struct {
	Targets []Target `yaml:"targets"`
	Load    struct {
		Stages []struct {
			RPS      int           `yaml:"rps"`
			Duration time.Duration `yaml:"duration"`
		} `yaml:"stages"`
	} `yaml:"load"`
	Limits struct {
		Concurrency int           `yaml:"concurrency"`
		MemoryMiB   int           `yaml:"payload_memory_mib"`
		Timeout     time.Duration `yaml:"request_timeout"`
	} `yaml:"limits"`
	Report struct {
		Path string `yaml:"path"`
	} `yaml:"report"`
}
type options struct {
	Config       Config
	Report, Sink string
	Check        bool
}

func loadOptions(args []string) (options, error) {
	var c Config
	var path, stages, keyEnv, report, sink, method, bodyFile string
	var check bool
	fs := flag.NewFlagSet("load-generator", flag.ContinueOnError)
	fs.StringVar(&path, "config", "", "YAML scenario")
	fs.BoolVar(&check, "check-config", false, "print plan without requests")
	fs.StringVar(&c.URL, "url", "http://127.0.0.1:8090/", "endpoint; replaces all YAML targets")
	fs.StringVar(&method, "method", "GET", "HTTP method (overrides all targets if explicit)")
	fs.StringVar(&bodyFile, "body-file", "", "static body file (overrides all target bodies)")
	fs.StringVar(&stages, "stages", "100@10s", "total RPS@duration stages")
	fs.IntVar(&c.Concurrency, "concurrency", 128, "requests in flight")
	fs.IntVar(&c.MemoryMiB, "memory-mib", 128, "payload buffer budget, not RSS")
	fs.DurationVar(&c.Timeout, "timeout", 5*time.Second, "request timeout")
	fs.StringVar(&keyEnv, "key-env", "", "Bearer token environment variable")
	fs.StringVar(&report, "report", "report.json", "report path")
	fs.StringVar(&sink, "sink", "", "synthetic HTTP receiver address")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected arguments")
	}
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	if sink != "" {
		if check || path != "" {
			return options{}, fmt.Errorf("sink cannot be combined with config/check")
		}
		return options{Sink: sink}, nil
	}
	base := "."
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return options{}, err
		}
		defer f.Close()
		base = filepath.Dir(path)
		var s scenario
		s.Limits.Concurrency = c.Concurrency
		s.Limits.MemoryMiB = c.MemoryMiB
		s.Limits.Timeout = c.Timeout
		s.Report.Path = report
		d := yaml.NewDecoder(io.LimitReader(f, 1<<20))
		d.KnownFields(true)
		if err = d.Decode(&s); err != nil {
			return options{}, fmt.Errorf("invalid YAML: %w", err)
		}
		var extra any
		if err = d.Decode(&extra); err != io.EOF {
			return options{}, fmt.Errorf("expected one YAML document")
		}
		c.Targets = s.Targets
		if !explicit["stages"] {
			for _, v := range s.Load.Stages {
				c.Stages = append(c.Stages, Stage{v.RPS, v.Duration})
			}
		}
		if !explicit["concurrency"] {
			c.Concurrency = s.Limits.Concurrency
		}
		if !explicit["memory-mib"] {
			c.MemoryMiB = s.Limits.MemoryMiB
		}
		if !explicit["timeout"] {
			c.Timeout = s.Limits.Timeout
		}
		if !explicit["report"] {
			report = s.Report.Path
		}
		if len(c.Targets) == 0 && !explicit["url"] {
			return options{}, fmt.Errorf("targets required")
		}
	}
	if path == "" || explicit["stages"] {
		var err error
		c.Stages, err = parseStages(stages)
		if err != nil {
			return options{}, err
		}
	}
	if path == "" || explicit["url"] {
		c.Targets = []Target{{Name: "default", URL: c.URL, Weight: 1, Method: method}}
	}
	for i := range c.Targets {
		t := &c.Targets[i]
		if explicit["method"] {
			t.Method = method
		}
		if t.Method == "" {
			t.Method = "GET"
		}
		if explicit["key-env"] {
			t.KeyEnv = keyEnv
		}
		bodyBase := base
		if explicit["body-file"] {
			t.Body = nil
			t.BodyTemplate = nil
			t.BodyTemplateFile = ""
			t.BodyFile = bodyFile
			bodyBase = "."
		}
		if err := prepareTarget(t, bodyBase); err != nil {
			return options{}, fmt.Errorf("target %s: %w", t.Name, err)
		}
	}
	c.URL = c.Targets[0].URL
	if err := c.validate(); err != nil {
		return options{}, err
	}
	if strings.TrimSpace(report) == "" {
		return options{}, fmt.Errorf("empty report path")
	}
	if _, _, err := payloadPlan(c); err != nil {
		return options{}, err
	}
	return options{c, report, sink, check}, nil
}
func token(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", r)) {
			return false
		}
	}
	return true
}
func prepareTarget(t *Target, base string) error {
	if t.Method == "" {
		t.Method = "GET"
	}
	if !token(t.Method) {
		return fmt.Errorf("invalid HTTP method")
	}
	headers := map[string]string{}
	set := func(k, v string) error {
		if !token(k) || strings.ContainsAny(v, "\r\n\x00") {
			return fmt.Errorf("invalid header")
		}
		k = http.CanonicalHeaderKey(k)
		switch k {
		case "Host", "Content-Length", "Transfer-Encoding", "Connection":
			return fmt.Errorf("transport-managed header %s is not supported", k)
		}
		if _, ok := headers[k]; ok {
			return fmt.Errorf("duplicate header %s", k)
		}
		headers[k] = v
		return nil
	}
	for k, v := range t.Headers {
		if err := set(k, v); err != nil {
			return err
		}
	}
	for k, env := range t.HeadersEnv {
		v, ok := os.LookupEnv(env)
		if !ok || v == "" {
			return fmt.Errorf("environment variable %s required", env)
		}
		if err := set(k, v); err != nil {
			return err
		}
	}
	if t.KeyEnv != "" {
		v, ok := os.LookupEnv(t.KeyEnv)
		if !ok || strings.TrimSpace(v) == "" {
			return fmt.Errorf("environment variable %s required", t.KeyEnv)
		}
		t.Key = v
	}
	if t.Key != "" {
		if err := set("Authorization", "Bearer "+t.Key); err != nil {
			return err
		}
	}
	t.Headers = headers
	sources := 0
	var data []byte
	dynamic := false
	if t.Body != nil {
		sources++
		data = []byte(*t.Body)
	}
	if t.BodyTemplate != nil {
		sources++
		dynamic = true
		data = []byte(*t.BodyTemplate)
	}
	for _, entry := range []struct {
		path     string
		template bool
	}{{t.BodyFile, false}, {t.BodyTemplateFile, true}} {
		if entry.path == "" {
			continue
		}
		sources++
		p := entry.path
		if !filepath.IsAbs(p) {
			p = filepath.Join(base, p)
		}
		f, err := os.Open(p)
		if err != nil {
			return fmt.Errorf("cannot read body file")
		}
		data, err = io.ReadAll(io.LimitReader(f, (16<<20)+1))
		f.Close()
		if err != nil {
			return fmt.Errorf("cannot read body file")
		}
		dynamic = entry.template
	}
	if sources > 1 {
		return fmt.Errorf("choose one of body, body_file, body_template, body_template_file")
	}
	if len(data) > 16<<20 {
		return fmt.Errorf("body exceeds 16 MiB")
	}
	var err error
	t.compiled, err = compilePayload(data, dynamic)
	if err != nil {
		return err
	}
	if len(t.compiled.body) > 16<<20 {
		return fmt.Errorf("expanded body exceeds 16 MiB")
	}
	for _, s := range t.ExpectedStatus {
		if s < 100 || s > 599 {
			return fmt.Errorf("invalid expected_status")
		}
	}
	return nil
}
func validateTargets(targets []Target) error {
	if len(targets) > 100 {
		return fmt.Errorf("at most 100 targets")
	}
	names := map[string]bool{}
	for _, t := range targets {
		if strings.TrimSpace(t.Name) == "" || names[t.Name] || strings.ContainsAny(t.Name, "\r\n") {
			return fmt.Errorf("target names must be unique and nonempty")
		}
		names[t.Name] = true
		u, err := url.Parse(t.URL)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
			return fmt.Errorf("target %s: invalid URL", t.Name)
		}
		if t.Weight < 1 || t.Weight > 1000000 {
			return fmt.Errorf("target %s: invalid weight", t.Name)
		}
	}
	return nil
}
func payloadPlan(c Config) (int, int, error) {
	total, maxSize := 0, 0
	for _, t := range c.Targets {
		if t.compiled == nil {
			return 0, 0, fmt.Errorf("target body not prepared")
		}
		n := len(t.compiled.body)
		total += n
		if n > maxSize {
			maxSize = n
		}
	}
	budget := int64(c.MemoryMiB) << 20
	// One immutable compiled copy plus one reusable copy per worker per target.
	workers := c.Concurrency
	if total > 0 {
		available := budget - int64(total)
		max := int(available / int64(total))
		if workers > max {
			workers = max
		}
	}
	if workers < 1 {
		return 0, 0, fmt.Errorf("payload budget too small for compiled bodies and a worker")
	}
	return maxSize, workers, nil
}
func printPlan(w io.Writer, c Config, report string) {
	_, workers, _ := payloadPlan(c)
	fmt.Fprintf(w, "Plan: workers=%d timeout=%s (total RPS across targets)\n", workers, c.Timeout)
	var total int
	for _, t := range c.Targets {
		total += t.Weight
	}
	var average float64
	for _, t := range c.Targets {
		u, _ := url.Parse(t.URL)
		u.RawQuery = ""
		share := float64(t.Weight) / float64(total)
		average += share * float64(len(t.compiled.body))
		fmt.Fprintf(w, "  %s: %s %s share=%.2f%% body=%d bytes\n", t.Name, t.Method, u.String(), share*100, len(t.compiled.body))
	}
	for _, s := range c.Stages {
		fmt.Fprintf(w, "  %s: TOTAL %d RPS | ≈%.2f MB/s body bytes\n", s.Duration, s.RPS, float64(s.RPS)*average/1e6)
	}
	fmt.Fprintf(w, "Report: %s (relative to working directory)\n", report)
}
func expected(t Target, status int) bool {
	if len(t.ExpectedStatus) == 0 {
		return status >= 200 && status < 300
	}
	for _, s := range t.ExpectedStatus {
		if s == status {
			return true
		}
	}
	return false
}
func chooseTarget(seq uint64, targets []Target) int {
	var total uint64
	for _, t := range targets {
		total += uint64(t.Weight)
	}
	x := seq + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	n := x % total
	for i, t := range targets {
		if n < uint64(t.Weight) {
			return i
		}
		n -= uint64(t.Weight)
	}
	return 0
}
