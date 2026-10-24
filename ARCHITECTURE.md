# Architecture

Describes implemented behavior only, not a plan. In-memory job metadata, PDFs on local disk, no database.

## Overview

```text
Client
  |  POST /jobs
  v
:8080 -> rate limiter (10/IP/min, 100/IP/hour) -> Handler -> Service -> Store (in-memory)
                                                                  |
                                                        bounded queue (100)
                                                                  |
                                                          4 workers -> PDF generator -> ./storage/{id}.pdf
```

## Startup / shutdown

`cmd/server/main.go` wires dependencies and runs two servers: `:8080` (API) and `127.0.0.1:6060` (pprof, never public).

On `SIGINT`/`SIGTERM`:
1. Stop accepting new jobs.
2. Shut down both HTTP servers.
3. Close the queue — workers drain in-flight jobs, then exit.
4. Wait up to 30s for jobs to finish, then exit.

Workers don't use the originating request context — a job keeps running after its `202` is sent.

## Rate limiting

`internal/ratelimit` gates `POST /jobs` only, per source IP, in-memory. It uses independent `golang.org/x/time/rate` token buckets: 10/minute with a burst of 10, and 100/hour with a burst of 100. Over either limit: `429`, with a `Retry-After` header in seconds. Uses the raw socket address, not `X-Forwarded-For`: trusting that header without a configured reverse-proxy hop count would let any client spoof a different IP and bypass its own limit. Resets on restart; not shared across replicas — exists to stop one client from filling the queue, not as a distributed rate limit.

## Job model

```go
type Job struct {
    ID, FilePath, Error string
    Lines                int
    Status               JobStatus // queued -> processing -> completed | failed
    CreatedAt, UpdatedAt time.Time
}
```

`Store` = `map[string]Job` + `sync.RWMutex`. PDF bytes never live in the record.

ID: 10-char Base62, `crypto/rand`, collision-checked on insert.

Write path: generate -> temp file -> atomic rename -> mark `completed`. No partial PDF is ever servable. Panics in a worker are recovered and the job is marked `failed`; internal error detail is logged, never returned to the client.

## Concurrency

4 long-lived workers read from a channel buffered for 100 jobs — at most 4 PDF generations run at once. `CreateJob` enqueues non-blocking: full queue returns `ErrQueueFull` (`503`), job not persisted.

Single-instance only: the queue and rate limiter are both in-memory, nothing coordinates state across replicas.

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
