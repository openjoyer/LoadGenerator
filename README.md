# Load Generator

A native HTTP load generator written in Go. Define endpoints, request bodies, traffic weights, and RPS stages in YAML, then run repeatable tests from a single binary.

## Features

- Multiple weighted targets with independent HTTP methods, headers, and expected status codes.
- Static text or binary request bodies, plus reusable templates with dynamic values.
- Open-model scheduling: request arrivals do not wait for previous responses.
- Bounded concurrency and configurable payload-buffer memory budget.
- Live statistics and a JSON report with latency percentiles and per-target counters.
- Configuration validation and a traffic preview without sending requests.
- A local synthetic HTTP receiver for testing the generator itself.

## Quick start

Requires **Go 1.24 or later** to build. Docker is not required. Run the following commands from the repository root.

```bash
go build -o bin/load-generator .
```

Start the optional test receiver in one terminal:

```bash
./bin/load-generator -sink 127.0.0.1:8090
```

Validate and run a scenario in another terminal:

```bash
./bin/load-generator -config scenarios/local.yaml -check-config
./bin/load-generator -config scenarios/local.yaml
```

The receiver reads request bodies up to 16 MiB and returns `202`. It does not validate JSON or persist data. Running both processes on one machine measures their combined resource constraints.

For an existing HTTP endpoint, no receiver is needed:

```bash
./bin/load-generator -url http://localhost:8080/health -method GET -stages '100@10s'
```

## YAML scenarios

```yaml
targets:
  - name: catalog
    url: http://localhost:8080/items
    weight: 80
    method: GET
    expected_status: [200]

  - name: create
    url: http://localhost:8080/items
    weight: 20
    method: POST
    headers:
      Content-Type: application/json
    # Read the entire header value from an environment variable:
    # headers_env:
    #   X-API-Key: MY_API_KEY
    # Alternatively, construct Authorization: Bearer <value>:
    # api_key_env: MY_BEARER_TOKEN
    body_template: '{"id":"{{uuid}}","value":"{{random:256}}"}'
    expected_status: [201, 202]

load:
  stages:
    - rps: 100
      duration: 10s
    - rps: 1000
      duration: 30s

limits:
  concurrency: 256
  payload_memory_mib: 128
  request_timeout: 5s

report:
  path: ./report.json
```

**RPS is the total across all targets.** Weights specify approximate proportions and do not need to sum to 100. Each request goes to one target; actual proportions can differ under overload.

Target names must be unique, and weights must be positive. The default method is `GET`, the default body is empty, and any `2xx` response is successful unless `expected_status` is specified. `Content-Type` is not added automatically.

Unknown YAML fields, invalid settings, and missing or empty referenced secrets stop the run before traffic starts. `${ENV}` interpolation is not supported; use `headers_env` or `api_key_env` explicitly. Do not configure the same header through multiple sources.

Included examples:

| Scenario | Purpose |
|---|---|
| [local.yaml](scenarios/local.yaml) | POST requests with a small dynamic JSON body to the local receiver |
| [multiple.yaml](scenarios/multiple.yaml) | GET and PUT requests split between two receivers on ports 8090 and 8091 |

## Request bodies

Choose at most one body source per target:

| Field | Behavior |
|---|---|
| `body` | Literal text; YAML block strings are supported |
| `body_file` | Static file, including binary data; read once |
| `body_template` | Inline text with dynamic placeholders |
| `body_template_file` | Template file compiled once before the run |

Body-file paths in YAML are relative to the **scenario file's directory**. Report paths are relative to the **process working directory**. Request bodies are limited to 16 MiB.

Static bodies are sent without interpreting `{{...}}`. The generator does not validate bodies as JSON: XML, plain text, binary data, and other formats can be used.

### Template placeholders

| Placeholder | Value |
|---|---|
| `{{uuid}}` | A distinct UUID for each occurrence and request within a run |
| `{{timestamp}}` | Current UTC time in RFC3339 format with nanoseconds |
| `{{sequence}}` | Request sequence number, zero-padded to 20 digits |
| `{{random:512}}` | 512 pseudorandom ASCII characters safe inside a JSON string |

In JSON, put **all placeholders inside quotes**, including `sequence`. Substitutions apply only to bodies, not URLs or headers. UUIDs use a random run prefix and a counter; they are not cryptographically random values. Unknown placeholders are rejected.

Templates have fixed output sizes and are compiled before requests start. Workers update values in reusable buffers rather than rebuilding JSON for every request. To send literal placeholder syntax, use `body` or `body_file`.

## CLI overrides

Configuration precedence is **defaults → YAML → explicitly supplied flags**.

```bash
./bin/load-generator -config scenarios/local.yaml -concurrency 512 -stages '200@10s'
./bin/load-generator -url http://localhost:8080/upload -method PUT -body-file ./sample.bin
```

| Flag | Behavior |
|---|---|
| `-config` | Load a YAML scenario |
| `-check-config` | Validate and print the plan; no requests or report file |
| `-url` | Replace the entire target list with one new target |
| `-method` | Override the method for all selected targets |
| `-body-file` | Replace all selected target bodies with a static file; path relative to the working directory |
| `-key-env` | Set the Bearer-token environment variable for all selected targets |
| `-stages` | Override the total RPS profile, such as `100@10s,1000@30s` |
| `-concurrency` | Override the maximum number of requests in flight |
| `-memory-mib` | Override the payload-buffer budget |
| `-timeout` | Override the request timeout |
| `-report` | Override the output report path |
| `-sink` | Run the synthetic receiver instead of generating traffic |

**Using `-url` discards the original targets' headers, bodies, authentication, and expected statuses.** To change only an endpoint address, edit its URL in the YAML file.

The former domain-specific flags `-batch`, `-message-bytes`, `-services`, `-format`, and the YAML `payload` section have been removed. Describe the desired data directly in a request body or template.

## Scheduling and resource limits

The scheduler runs at approximately 1 ms granularity, so arrivals have small bursts. Stages change the requested rate in steps. There is currently no Poisson arrival model, dedicated burst schedule, or per-target random body-profile selector.

When no execution slot is available, the generator records a missed send instead of building an unbounded queue. Targets share the concurrency limit: a slow target can affect others. Use separate processes when isolation is required.

Payload memory is budgeted for the compiled bodies plus reusable worker copies. A small budget reduces the effective worker count. This is **not a process RSS limit**: HTTP/TLS buffers, goroutine stacks, metadata, and other allocations consume additional memory.

The client uses HTTP/1.1 keep-alive with bounded concurrency. Application retries and redirects are disabled. TLS certificates are verified, and system `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` settings are respected. Transport-managed headers such as `Host`, `Content-Length`, `Connection`, and `Transfer-Encoding` cannot be overridden.

Actual throughput depends on payload size, CPU, network capacity, TLS, and the target. No fixed RPS is guaranteed. For large tests, run the generator on a separate machine and monitor both sides.

## Reports

A live summary is printed once per second, followed by a JSON report at completion. `Ctrl+C` cancels requests and saves a partial report. An existing report file is overwritten.

| Field | Meaning |
|---|---|
| `Scheduled`, `Submitted`, `Missed` | Scheduled sends, admitted jobs, and skipped sends; `Scheduled = Submitted + Missed` |
| `Started`, `Completed` | HTTP calls started and finished |
| `Success` | Expected status with a successfully read, bounded response body |
| `targets` | Started, successful, and failed requests grouped by target name |
| `TransportErrors`, `ResponseErrors` | Transport failures and response-body failures |
| `LatencyP50/P95/P99` | Time from HTTP call start through response-body reading |
| `StartLagP99` | Delay between scheduled and actual request start |
| `AchievedStartRPS` | Average start rate across the entire profile, including slower stages |
| `PayloadBytes` | Maximum target body size, not the average |
| `SubmittedPayloadBytes` | Sum of attempted body sizes, not measured network bytes |
| `HeapBytes` | Go heap at the end, not peak memory or process RSS |
| `Stages.Duration` | Stage duration in nanoseconds |

Percentiles are approximate upper estimates from a fixed histogram, with roughly 6.25% resolution. Missed sends are excluded from latency, so evaluate latency together with `Missed`, start lag, and errors. Statistics are cumulative; per-stage reports are not implemented.

A successful HTTP response does not prove that an application persisted or processed the data. Validate downstream effects separately. Reports omit target URLs and authentication values; the printed plan also omits query strings, bodies, and headers. Avoid putting secrets in target names or URL paths.

## Scope

This tool generates independent HTTP requests. It does not currently implement dependent user journeys, response-based token extraction, WebSocket or gRPC traffic, request compression, database reconciliation, or a distributed coordinator.

## Integration guides

- [Using Load Generator with LogFlux](docs/logflux.md): sending batches of synthetic logs, setting an API key, and checking the ingestion pipeline. LogFlux is an example integration; no application-specific format is built into the generator.

## Development

```bash
go test -race ./...
go vet ./...
go test -run '^$' -bench BenchmarkPayload -benchmem
```

Tests cover body generation, configuration, routing, methods, headers, overload, cancellation, and response handling. The payload benchmark measures template updates only, not end-to-end HTTP throughput.
