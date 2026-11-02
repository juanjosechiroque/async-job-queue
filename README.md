# Async Job Queue

[![CI](https://github.com/juanjosechiroque/async-job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/juanjosechiroque/async-job-queue/actions/workflows/ci.yml)

Go service that generates PDFs asynchronously. `POST /jobs` returns immediately; poll for status, download when done.

## Status

In-memory job store, 4-worker pool, 100-job bounded queue. Full queue returns `503`. Job creation is rate limited per IP with token buckets (10/min, 100/hour) — `429` beyond that. No database, no auto file cleanup.

## Run

```bash
go run ./cmd/server
```

- API: `http://localhost:8080`
- pprof (debug only, not public): `http://127.0.0.1:6060/debug/pprof/`
- `Ctrl+C` for graceful shutdown — drains the queue, 30s timeout.

Requires Go 1.27+ (`go.mod`). No env vars, no external services.

## API

**Create a job**

```
POST /jobs
{"lines": 100}
```

`lines`: 1–250000. Returns `202`:

```json
{"jobId": "a8K2mP91xQ", "status": "queued", "downloadUrl": "/jobs/a8K2mP91xQ/file"}
```

**Check status**

```
GET /jobs/{jobId}
```

Completed jobs include `downloadUrl`; failed jobs include `error`.

**Download**

```
GET /jobs/{jobId}/file
```

| Job state | HTTP | Response |
|---|---:|---|
| `queued` / `processing` | `409` | job not ready |
| `completed` | `200` | PDF |
| `failed` | `500` | `{"error":"job failed"}` |
| unknown ID | `404` | `{"error":"job not found"}` |

**Errors**

| Case | HTTP |
|---|---:|
| bad method | `405` |
| invalid body | `400` |
| `lines` out of range | `400` |
| rate limit exceeded (job creation only) | `429` |
| unknown job | `404` |
| shutting down | `503` |
| queue full | `503` |

No internal error detail ever reaches the client.

## Try it

```bash
curl -X POST localhost:8080/jobs -d '{"lines":10}'
curl localhost:8080/jobs/{id}
curl localhost:8080/jobs/{id}/file -o result.pdf
```

## Notes

- Job state is in-memory only — lost on restart. Existing PDF files stay in `storage/` but become unreachable.
- No idempotency: identical requests create separate jobs.
- Files write atomically (temp file + rename) — no partial PDF is ever served.
- Rate limits and the job queue are per-process, not shared across replicas.

## Verify

```bash
make check   # fmt, vet, race-enabled tests
```

CI runs the same checks on every push/PR to `main`.

## Structure

```text
cmd/server/main.go   server startup, shutdown
internal/jobs/       handler, service, store
internal/ratelimit/  per-IP rate limiting middleware
internal/pdf/        PDF generation
internal/text/       filler text
```
