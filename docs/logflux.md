# Using Load Generator with LogFlux

[Back to the main README](../README.md)

LogFlux is an example integration. Its event format lives in a template file, not in the generator's Go code.

## Included files

- [Scenario](../scenarios/logflux.yaml): endpoint, API-key source, RPS stages, and resource limits.
- [Batch template](../scenarios/bodies/logflux-batch.json.tmpl): 100 synthetic log events per request.

Run the commands below from the Load Generator repository root.

## Test against the local receiver first

Start the synthetic receiver in a separate terminal:

```bash
./bin/load-generator -sink 127.0.0.1:8090
```

Copy the scenario within the same directory:

```bash
cp scenarios/logflux.yaml scenarios/logflux-local.yaml
```

In the copy, change only the target URL to `http://127.0.0.1:8090/api/v1/logs`. Keeping the file in `scenarios/` preserves the relative path to its body template.

Then validate and run:

```bash
export LOGFLUX_API_KEY='test-only'
./bin/load-generator -config scenarios/logflux-local.yaml -check-config
./bin/load-generator -config scenarios/logflux-local.yaml -stages '10@10s'
```

The receiver returns `202` after reading the body; it does not validate or store events. Do not use `-url` to change just the address of this scenario: that flag replaces the entire target and discards its body and headers.

## Connect to LogFlux

1. Start the application services, Kafka, and databases.
2. Create a project and a write API key.
3. Verify the actual endpoint and JSON contract. The included scenario targets `POST /api/v1/logs` from the specification.
4. Supply the key through the environment and start with a small load:

```bash
export LOGFLUX_API_KEY='your-write-key'
./bin/load-generator -config scenarios/logflux.yaml -check-config
./bin/load-generator -config scenarios/logflux.yaml -stages '10@10s'
```

This profile requires **HTTP 202**. At the time the integration example was created, the application had an unfinished `/api/logs` handler and a DTO using `event_id`. Check the current implementation before testing. A placeholder handler returning `200` is a failure under this profile.

Do not store real API keys in YAML or commit them to Git.

## Customize the log data

The template contains `{"logs":[...]}` with **100 events**. Each event has a fresh `eventId`, a current timestamp, a service name, the `load-test` environment, a severity level, and a 512-character message. Five events out of 100 use `ERROR`.

The template is read once. Dynamic values are updated in reusable buffers before each request.

| Change | How |
|---|---|
| Events per request | Add or remove objects in the template's `logs` array |
| Message length | Change `{{random:512}}` to another supported length |
| Service distribution | Edit the `service` values in the template |
| Error proportion | Change the number of events with `level: ERROR` |
| Endpoint or response contract | Edit `url` or `expected_status` in the scenario |

Random message text is a compression stress profile, not a realistic model of repeated business log messages. For more representative data, use sanitized message patterns with dynamic fields.

If your API expects snake_case, change `eventId` to `event_id`, adjust severity casing, and change the request envelope to match the actual handler. A DTO alone does not define whether the endpoint expects an array or an object wrapping that array.

## Translate RPS into event volume

With 100 events per request:

| HTTP RPS | Events per second |
|---:|---:|
| 100 | 10,000 |
| 1,000 | 100,000 |
| 5,000 | 500,000 |

The generator counts HTTP requests and body bytes; it does not interpret bodies as logs. `-check-config` prints the expanded body size and estimated payload throughput.

Respect the target's limits. The LogFlux specification allows up to 500 events and 1 MiB per request, even though the generator itself supports bodies up to 16 MiB.

## Verify the whole pipeline

The generator report measures HTTP behavior. Also inspect:

- Kafka consumer lag and processing errors.
- The number of unique events that reach ClickHouse.
- Time from ingestion acceptance to search visibility.
- Recovery after downstream components become unavailable.

A timed-out request may already have been accepted. The generator does not retry automatically. Project rate limits may produce `429`; this does not necessarily indicate a Kafka or ClickHouse capacity limit.

To simulate multiple projects, define multiple targets with separate `api_key_env` variables and weights. They may reference the same body-template file. RPS remains the total across all targets.
