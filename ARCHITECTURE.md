# Architecture

Describes implemented behavior only, not a plan. Job metadata is in Postgres; PDFs are local files.

## Overview

```text
Client
  |  POST /jobs
  v
:8080 -> rate limiter (10/IP/min, 100/IP/hour) -> Handler -> Service -> Postgres jobs table
                                                                  |
                                                  4 polling workers claim queued rows
                                                  (FOR UPDATE SKIP LOCKED; 200ms idle poll)
                                                                  |
                                                        PDF generator -> ./storage/{id}.pdf
```

## Startup / shutdown

`cmd/server/main.go` wires dependencies and runs two servers: `:8080` (API) and `127.0.0.1:6060` (pprof, never public).

On `SIGINT`/`SIGTERM`:
1. Stop accepting new jobs.
2. Shut down both HTTP servers.
3. Workers keep claiming queued jobs and drain locally-created work, then exit.
4. Wait up to 10s for jobs to finish, then exit.

A full 100-job queue at the largest allowed size (250,000 lines) drains in ~2.8s measured (4 workers, ~103ms generation + ~9ms disk write per job) — 10s leaves ~3.5x margin for production I/O the local benchmark doesn't capture.

Workers don't use the originating request context — a job keeps running after its `202` is sent.

## Rate limiting

`internal/ratelimit` gates `POST /jobs` only, per source IP, in-memory. It uses independent `golang.org/x/time/rate` token buckets: 10/minute with a burst of 10, and 100/hour with a burst of 100 — unvalidated starting values chosen to bound abuse, not modeled from real traffic. Over either limit: `429`, with a `Retry-After` header in seconds. Uses the raw socket address, not `X-Forwarded-For`: trusting that header without a configured reverse-proxy hop count would let any client spoof a different IP and bypass its own limit. Resets on restart; not shared across replicas — exists to stop one client from filling the queue, not as a distributed rate limit.

## Job model

```go
type Job struct {
    ID, FilePath, Error string
    Lines                int
    Status               JobStatus // queued -> processing -> completed | failed
    CreatedAt, UpdatedAt time.Time
}
```

`internal/jobs.JobStore` abstracts metadata operations. `internal/postgres.Store` is used by the server through `pgxpool`; `internal/jobs.Store` remains an in-memory implementation used by fast unit tests. The embedded SQL schema creates the `jobs` table and `(status, created_at)` claim index. PDF bytes never live in the record.

ID: 10-char Base62, `crypto/rand`, collision-checked on insert.

Write path: generate -> temp file -> atomic rename -> mark `completed`. No partial PDF is ever servable. Panics in a worker are recovered and the job is marked `failed`; internal error detail is logged, never returned to the client.

## Concurrency

4 long-lived workers poll Postgres while idle (every 200ms) and atomically claim a queued row using `SELECT ... FOR UPDATE SKIP LOCKED`, changing it to `processing` in the same transaction. At most 4 PDF generations run at once per service process. `SKIP LOCKED` makes the claim safe across concurrent workers and multiple service processes using the same database: a row locked by one claimant is unavailable to the others.

`CreateJob` inserts through a transaction that serializes the queued-row count and insert. A count at or above 100 returns `ErrQueueFull` (`503`) immediately and does not persist the job, rather than leaving the request hanging until space frees up.

`jobQueueCapacity = 100` is an unvalidated starting value: generation is fast enough (~100ms worst case, 4 workers) that 100 queued jobs drain in under 3 seconds, too quick to have ever pressure-tested the number. A slower, I/O-bound generator (real rendering, disk, or network calls) would need to remeasure it.

The persisted queue can be consumed by multiple instances safely. This local setup starts one instance only. The rate limiter remains in-memory and is not shared across replicas.

## Persistence and retention

`DATABASE_URL` is read once at startup. Startup fails fast if it is absent or if the initial Postgres connection/schema application fails. `docker compose up -d --wait` starts the local Postgres service with a named data volume; its job records survive server and Compose restarts.

Completed PDFs are still local files in `storage/`. There is intentionally no file or database-record retention process. This manual file-retention concern is more important now that job records persist; implementing cleanup is explicitly out of scope.

## Observability

- `log/slog`; every job log line carries `job_id`, plus line count, state transitions, generation duration, PDF size.
- `pprof` on `127.0.0.1:6060`, isolated from the public API.
- No metrics or alerting yet.

## Measured impact: worker pool vs. unbounded

Load: `hey -c 100 -n 100 -m POST -d '{"lines":250000}' localhost:8080/jobs`, `pprof` sampled immediately after.

| | Goroutines | Heap in use |
|---|---:|---:|
| Before (unbounded) | 93 | 703 MB |
| After (4 workers) | 10 | 85 MB |

98% of baseline heap traced to `bytes.growSlice` in `pdf.Generator.Generate` — each concurrent job builds its full PDF in memory before writing. Four workers were chosen to cap simultaneous generators while still keeping the 100-request benchmark admissible through the 100-entry queue; the pool cut goroutines 89% and heap 88% at this load.
