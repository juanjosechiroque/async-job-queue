# Architecture

Describes implemented behavior only, not a plan. Job metadata lives in Postgres; PDFs are local files.

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

`cmd/server/main.go` wires dependencies and runs two servers: `:8080` (API) and `127.0.0.1:6060` (pprof, never public). `DATABASE_URL` is required; startup fails fast if it is missing or the Postgres connection/schema setup fails.

On `SIGINT`/`SIGTERM`:
1. Stop accepting new jobs.
2. Shut down both HTTP servers.
3. Workers stop claiming work; jobs already executing in this process finish, while queued rows remain in the store for a later process to claim.
4. Wait up to 10s, then exit.

A `CreateJob` that passes the accepting check just before shutdown may finish its Postgres insert after `StopAccepting`. That row remains `queued` and can be claimed by the next process; `Wait` only accounts for jobs claimed by this process's workers.

Why 10s: shutdown waits only for jobs already in progress. At most 4 run at once, in parallel, so the wait is about one job: ~112ms for the largest (~103ms generation + ~9ms disk write), excluding Postgres round-trips. A local shutdown with jobs in flight exited in under 1s; 10s leaves ample margin for slower production I/O.

Workers don't use the request context — a job keeps running after its `202` is sent. Polling and claiming use a service lifetime context canceled at shutdown. Once a job is claimed, its generation is not canceled, and final `completed`/`failed` writes use a context detached from shutdown cancellation; the Postgres store still limits each operation to five seconds so those writes can finish during the shutdown wait window.

## Rate limiting

`internal/ratelimit` gates `POST /jobs` per source IP, in memory, with two `golang.org/x/time/rate` token buckets: 10/minute (burst 10) and 100/hour (burst 100). Over either limit: `429` with a `Retry-After` header in seconds. It uses the socket address, not `X-Forwarded-For` (see Decisions).

## Job model

```go
type Job struct {
    ID, FilePath, Error string
    Lines                int
    Status               JobStatus // queued -> processing -> completed | failed
    CreatedAt, UpdatedAt time.Time
}
```

`jobs.JobStore` abstracts metadata storage with context-aware operations and explicit errors: `postgres.Store` (pgxpool) in the server, `jobs.Store` (in-memory) for fast unit tests. A missing row is `ErrJobNotFound`; database errors remain errors and are not treated as missing jobs. The embedded schema creates the `jobs` table and a `(status, created_at)` index matching the claim query. PDF bytes never live in the record.

ID: 10-char Base62, `crypto/rand`, collision-checked on insert.

Write path: generate -> temp file -> atomic rename -> mark `completed`. A partial PDF is never servable. Worker panics are recovered and the job is marked `failed`; error detail is logged, never returned to the client.

## Concurrency

4 workers per process poll Postgres (every 200ms while idle; woken immediately by jobs created in the same process) and claim a row with `SELECT ... FOR UPDATE SKIP LOCKED`, setting it to `processing` in the same transaction. At most 4 generations run at once per process. A row locked by one claimant is skipped by the others — across workers and across processes.

`CreateJob` counts queued rows and inserts in one transaction under an advisory lock. At 100 queued it returns `ErrQueueFull` (`503`) and persists nothing.

## Observability

- `log/slog`; every job log line carries `job_id`, plus line count, state transitions, generation duration, PDF size.
- `pprof` on `127.0.0.1:6060`, isolated from the public API.
- No metrics or alerting.

## Measured impact: worker pool vs. unbounded

Measured on the earlier in-memory version, before Postgres and the rate limiter; the cap of 4 concurrent generators is unchanged. From a single IP, the `hey` command below would now hit the rate limiter.

Load: `hey -c 100 -n 100 -m POST -d '{"lines":250000}' localhost:8080/jobs`, `pprof` sampled immediately after.

| | Goroutines | Heap in use |
|---|---:|---:|
| Before (unbounded) | 93 | 703 MB |
| After (4 workers) | 10 | 85 MB |

98% of baseline heap traced to `bytes.growSlice` in `pdf.Generator.Generate` — each concurrent job builds its full PDF in memory before writing. The pool cut goroutines 89% and heap 88%.

## Decisions and trade-offs

| Decision | Why | Cost |
|---|---|---|
| Poll Postgres every 200ms, not `LISTEN/NOTIFY` | Simplest correct claim loop; also finds jobs created by other instances | Up to 200ms pickup latency; idle load of 4 workers × 5 polls/s |
| 4 workers | Caps peak memory (93 → 10 goroutines, 703 → 85 MB above) | At most 4 concurrent generations per process |
| `503` when the queue is full, instead of blocking | A blocked request has no bound of its own and gives the client no retry signal | Clients must retry |
| Advisory lock on job creation | Count + insert cannot race past the 100 cap, even across processes; request goroutines do not hold the service mutex during database I/O | All job creation serializes on one database lock |
| In-flight creation may finish after shutdown starts | The database insert is outside the service mutex; the existing Postgres transaction still enforces capacity | A final `queued` row may wait for the next process to start |
| Final job-state writes detach from shutdown cancellation | A claimed job continues to completion and its final database update can finish while `Wait` is active; each Postgres operation remains capped at five seconds | Shutdown may wait for an in-flight store write until its operation timeout |
| Socket IP, not `X-Forwarded-For` | The header is spoofable without a trusted-proxy hop count | Behind a proxy, every client looks like one IP |
| PDFs on local disk, metadata in Postgres | Keeps large blobs out of the database | Files are not shared across instances (below) |

## Known limitations

- **Downloads are per-instance.** Claiming is safe across instances, but each instance writes PDFs to its own `storage/`, so `GET /jobs/{id}/file` returns `500` on any instance that didn't generate the file. Multi-instance needs shared storage.
- **No recovery of `processing` jobs.** If a process dies, or the shutdown deadline expires, mid-job, the row stays `processing` — there is no lease or requeue.
- **No retention.** PDFs and job records grow without bound; cleanup is out of scope.
- **Unvalidated numbers.** The queue cap (100) and rate limits (10/min, 100/hour) are starting values. Generation takes ~100ms at worst, too fast to pressure-test them; a slower workload needs re-measuring.
- **The rate limiter is per-process** and resets on restart.
- **No metrics or alerting**, only logs and `pprof`.
