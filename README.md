# Async Job Queue

[![CI](https://github.com/juanjosechiroque/async-job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/juanjosechiroque/async-job-queue/actions/workflows/ci.yml)

Go service that generates PDFs asynchronously. `POST /jobs` returns immediately; poll for status, download when done.

The workload models report/invoice generation — `lines` stands in for how much content a real report would have.

## Status

Postgres-backed job metadata, 4 polling workers, 100-job bounded queue. Full queue returns `503`. Job creation is rate limited per IP with token buckets (10/min, 100/hour) — `429` beyond that. No automatic file or record cleanup.

## Run

```bash
docker compose up -d --wait
export DATABASE_URL='postgres://async_job_queue:async_job_queue@localhost:5432/async_job_queue?sslmode=disable'
go run ./cmd/server
```

- API: `http://localhost:8080`
- pprof (debug only, not public): `http://127.0.0.1:6060/debug/pprof/`
- `Ctrl+C` for graceful shutdown — drains the queue, 10s timeout.

Requires Go 1.27+ (`go.mod`), Docker Compose for local Postgres, and `DATABASE_URL`. The Compose volume is named, so database data survives `docker compose down` and a later `up` (use `down -v` to remove it).

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

- Job records persist in Postgres across server restarts. Workers claim queued rows with `SELECT ... FOR UPDATE SKIP LOCKED`, so concurrent workers — including workers from multiple server instances — cannot process the same queued job twice.
- PDFs remain files under `storage/`. Retention/cleanup of those files is a manual concern, now more important because job records persist; cleanup of files or records remains explicitly out of scope.
- No idempotency: identical requests create separate jobs.
- Files write atomically (temp file + rename) — no partial PDF is ever served.
- Rate limits are per-process. Queue capacity and job claims are shared through Postgres.

## Verify

```bash
make check   # fmt, vet, race-enabled tests
```

CI runs the same checks on every push/PR to `main`.

## Structure

```text
cmd/server/main.go   server startup, shutdown
internal/jobs/       handler, service, JobStore interface, in-memory store
internal/postgres/   pgxpool-backed JobStore and embedded schema
internal/ratelimit/  per-IP rate limiting middleware
internal/pdf/        PDF generation
internal/text/       filler text
```
