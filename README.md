# Async Job Queue

[![CI](https://github.com/juanjosechiroque/async-job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/juanjosechiroque/async-job-queue/actions/workflows/ci.yml)

Go service that generates PDFs asynchronously. `POST /jobs` returns immediately; poll for status, download when done.

The workload models report/invoice generation — `lines` stands in for how much content a real report would have.

## Status

Postgres-backed job metadata, 4 polling workers, 100-job bounded queue (`503` when full), per-IP rate limit on job creation (10/min, 100/hour, `429` beyond). No file or record cleanup. Design, measurements, and known limitations: [ARCHITECTURE.md](ARCHITECTURE.md).

## Run

```bash
docker compose up -d --wait
export DATABASE_URL='postgres://async_job_queue:async_job_queue@localhost:5432/async_job_queue?sslmode=disable'
go run ./cmd/server
```

- API: `http://localhost:8080`
- pprof (debug only, not public): `http://127.0.0.1:6060/debug/pprof/`
- `Ctrl+C` stops new claims, lets jobs already in progress finish (10s timeout), and leaves queued jobs for the next start.

Requires Go 1.27+ and Docker Compose. Database data survives `docker compose down`; use `down -v` to wipe it.
On startup, the server applies embedded, numbered SQL migrations automatically. The initial migration also accepts an existing development `jobs` table.

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
| store unavailable | `500` |
| request canceled or timed out during a store operation | `503` |

No internal error detail ever reaches the client.

## Try it

```bash
curl -X POST localhost:8080/jobs -d '{"lines":10}'
curl localhost:8080/jobs/{id}
curl localhost:8080/jobs/{id}/file -o result.pdf
```

## Notes

- Job records persist across restarts. Workers claim rows with `FOR UPDATE SKIP LOCKED`, so no job is processed twice, even across instances. Running several instances has caveats — see Known limitations in [ARCHITECTURE.md](ARCHITECTURE.md).
- PDFs are files under `storage/`. There is no retention or cleanup for files or records.
- No idempotency: identical requests create separate jobs.
- PDFs are written atomically (temp file + rename), so a partial file is never served.

## Verify

```bash
make check   # fmt, vet, race-enabled tests
```

The Postgres integration test runs when `DATABASE_URL` is set (e.g. after `docker compose up -d --wait`) and is skipped otherwise. CI runs everything against a Postgres service container on every push/PR to `main`.

## Structure

```text
cmd/server/main.go   server startup, shutdown
internal/jobs/       handler, service, JobStore interface, in-memory store
internal/postgres/   pgxpool-backed JobStore and embedded SQL migrations
internal/ratelimit/  per-IP rate limiting middleware
internal/pdf/        PDF generation
internal/text/       filler text
```
