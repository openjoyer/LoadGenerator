package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const scenarioText = `targets:
  - name: one
    url: http://localhost:8090/
    weight: 1
    method: GET
load:
  stages:
    - rps: 500
      duration: 2s
limits:
  concurrency: 16
`

func scenarioFile(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "load.yaml")
	if err := os.WriteFile(p, []byte(s), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestScenarioPrecedence(t *testing.T) {
	p := scenarioFile(t, scenarioText)
	o, err := loadOptions([]string{"-config", p, "-check-config", "-concurrency", "32", "-method", "DELETE"})
	if err != nil {
		t.Fatal(err)
	}
	if o.Config.Concurrency != 32 || o.Config.Targets[0].Method != "DELETE" || o.Config.Stages[0].RPS != 500 {
		t.Fatal("precedence")
	}
}
func TestInvalid(t *testing.T) {
	for _, s := range []string{strings.Replace(scenarioText, "weight: 1", "weight: 0", 1), strings.Replace(scenarioText, "concurrency:", "concurency:", 1), strings.Replace(scenarioText, "duration: 2s", "duration: -1s", 1), scenarioText + "---\n{}"} {
		if _, err := loadOptions([]string{"-config", scenarioFile(t, s)}); err == nil {
			t.Fatal("invalid accepted")
		}
	}
}
func TestBodies(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "body.txt"), []byte("literal {{uuid}}"), 0600); err != nil {
		t.Fatal(err)
	}
	target := Target{BodyFile: "body.txt"}
	if err := prepareTarget(&target, dir); err != nil {
		t.Fatal(err)
	}
	if len(target.compiled.patches) != 0 {
		t.Fatal("static interpreted")
	}
	text := "{{unknown}}"
	target = Target{BodyTemplate: &text}
	if prepareTarget(&target, dir) == nil {
		t.Fatal("bad placeholder")
	}
	text = "hello"
	target = Target{Body: &text, BodyFile: "body.txt"}
	if prepareTarget(&target, dir) == nil {
		t.Fatal("ambiguous body")
	}
}
func TestHeadersAndMethods(t *testing.T) {
	t.Setenv("TEST_CUSTOM_KEY", "secret")
	var seen [2]atomic.Uint64
	methods := []string{"GET", "PUT"}
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != methods[i] {
				t.Error("method")
			}
			if r.Header.Get("X-Api-Key") != "secret" {
				t.Error("header")
			}
			b, _ := io.ReadAll(r.Body)
			if i == 0 && len(b) != 0 {
				t.Error("GET body")
			}
			if i == 1 && string(b) != "hello" {
				t.Error("PUT body")
			}
			seen[i].Add(1)
			w.WriteHeader(201)
		}))
		defer servers[i].Close()
	}
	body := "hello"
	c := testConfig(servers[0].URL)
	c.Stages = []Stage{{200, 200 * time.Millisecond}}
	c.Targets = []Target{{Name: "a", URL: servers[0].URL, Weight: 80, Method: "GET", HeadersEnv: map[string]string{"X-Api-Key": "TEST_CUSTOM_KEY"}, ExpectedStatus: []int{201}}, {Name: "b", URL: servers[1].URL, Weight: 20, Method: "PUT", Body: &body, HeadersEnv: map[string]string{"X-Api-Key": "TEST_CUSTOM_KEY"}, ExpectedStatus: []int{204}}}
	r, err := Run(context.Background(), c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if seen[0].Load() == 0 || seen[1].Load() == 0 || r.Targets[0].Success != seen[0].Load() || r.Targets[1].Success != 0 || r.Targets[1].Errors != seen[1].Load() {
		t.Fatalf("bad report %+v", r.Targets)
	}
}
func TestSecrets(t *testing.T) {
	t.Setenv("TEST_KEY", "")
	target := Target{KeyEnv: "TEST_KEY"}
	if prepareTarget(&target, ".") == nil {
		t.Fatal("empty key")
	}
	target = Target{Headers: map[string]string{"X-A": "bad\nvalue"}}
	if prepareTarget(&target, ".") == nil {
		t.Fatal("header injection")
	}
}
func TestDistribution(t *testing.T) {
	targets := []Target{{Weight: 80}, {Weight: 20}}
	n := 0
	for i := uint64(0); i < 100000; i++ {
		if chooseTarget(i, targets) == 0 {
			n++
		}
	}
	if n < 79000 || n > 81000 {
		t.Fatal(n)
	}
}
func TestExamples(t *testing.T) {
	t.Setenv("LOGFLUX_API_KEY", "test")
	for _, p := range []string{"scenarios/local.yaml", "scenarios/multiple.yaml", "scenarios/logflux.yaml"} {
		if _, err := loadOptions([]string{"-config", p, "-check-config"}); err != nil {
			t.Fatal(p, err)
		}
	}
}
