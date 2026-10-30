# Current architecture

## Scope

This document describes the implemented asynchronous PDF service.

The service stores job metadata in memory and generated PDFs on the local filesystem. It has no persistent job database or automatic retention policy. A bounded in-memory queue feeds a fixed worker pool.

## Overview

```text
HTTP client
    |
    | POST /jobs
    v
HTTP Server :8080
    |
    v
jobs.Handler -> jobs.Service -> in-memory jobs.Store
                     |
                     | bounded channel (100 waiting jobs)
                     v
              4 long-lived workers
                     |
                     v
              pdf.Generator + text.Generator
                     |
                     v
              ./storage/{jobId}.pdf
```

## Startup and shutdown

`cmd/server/main.go` creates `./storage`, wires `text.Generator`, `pdf.Generator`, `jobs.Service`, and `jobs.Handler`, then registers `/jobs` and `/jobs/`.

The server handles `SIGINT` and `SIGTERM` as follows:

1. Reject new job creation.
2. Stop accepting new HTTP connections with `http.Server.Shutdown`.
3. Close the job queue; workers drain all accepted jobs and then exit.
4. Wait for tracked background jobs to finish, sharing a 30-second deadline with HTTP shutdown.
5. Exit when jobs finish or the deadline expires.

Workers do not use the originating request context, so an accepted job can continue after its creation response has been sent.

## HTTP contract

### `POST /jobs`

Validates `lines` in `internal/jobs` between 1 and 250,000, creates a `queued` job in the in-memory store, and enqueues it for a worker before returning `202 Accepted` with:

```json
{
  "jobId": "a8K2mP91xQ",
  "status": "queued",
  "downloadUrl": "/jobs/a8K2mP91xQ/file"
}
```

There is no `statusUrl` response field. `POST /job` is not registered.

The queue buffers 100 waiting jobs. If it is full, creation returns `503 Service Unavailable` with `{"error":"job queue is full"}`; the rejected job is not retained in the store. If shutdown has begun, creation instead returns the existing `503` shutdown response.

### `GET /jobs/{jobId}`

Returns the job ID and state. A completed job receives its `downloadUrl`; a failed job receives only the client-safe error `job failed` in addition to its ID and state.

### `GET /jobs/{jobId}/file`

Returns a completed PDF as `application/pdf`. Queued and processing jobs return `409`; failed jobs return `500`; unknown jobs return `404`. `storage/` has no public static route.

## Job lifecycle and storage

The in-memory `Store` is a `map[string]Job` protected by `sync.RWMutex`. It tracks this model:

```go
type Job struct {
    ID        string
    Lines     int
    Status    JobStatus
    FilePath  string
    Error     string
    CreatedAt time.Time
    UpdatedAt time.Time
}
```

The record never holds PDF bytes. State transitions are:

```text
queued -> processing -> completed
                     -> failed
```

The job runner marks the job `processing`, invokes the existing PDF generator, writes the result to a temporary file in `storage/`, atomically renames it to `{jobId}.pdf`, and only then marks it `completed`. Generation, storage, or recovered-panic failures are marked `failed`; panic details are logged internally.

Job IDs contain exactly 10 Base62 characters and use `crypto/rand`. Before insertion, the service checks the store and retries on an ID collision.

## Concurrency and retention

`cmd/server/main.go` configures four workers. Each worker ranges over a channel buffered for 100 waiting jobs, so no more than four PDF generations execute simultaneously and accepted requests do not wait for queue capacity. `CreateJob` uses a non-blocking enqueue: when the channel is full it returns `ErrQueueFull`, which the HTTP handler maps to `503`.

A `sync.WaitGroup` tracks accepted jobs for shutdown. `StopAccepting` takes the creation lock, marks the service unavailable, and closes the queue. Existing workers then drain the channel and exit; no request can send after the close.

Both the job store and PDFs are retained until manual cleanup. Job state is lost after process restart, while existing storage files remain on disk but cannot be retrieved because no in-memory job metadata exists.

Job lifecycle events are logged with `log/slog`; every job-specific record has a structured `job_id`, and creation records include the requested line count. Processing, completed, and failed transitions include the generation duration on terminal transitions; completed records also include the PDF size. Generation, storage, and recovered-panic failures log their underlying details internally while the HTTP API continues to return the safe `job failed` message.

Go pprof is served by a separate `http.Server` at `127.0.0.1:6060` using `http.DefaultServeMux`. It is never registered on the public API mux or bound to a public interface, and it is shut down with the public server during graceful shutdown. Operational metrics and disk-usage monitoring are not implemented.

## Concurrency measurements

The load command is intentionally only an admission test: `POST /jobs` returns after enqueueing, while PDF work remains in background. The relevant snapshots were taken immediately after this command finished:

```bash
hey -c 100 -n 100 -m POST -H 'Content-Type: application/json' -d '{"lines":250000}' http://localhost:8080/jobs
curl http://127.0.0.1:6060/debug/pprof/goroutine?debug=1
go tool pprof -top -text http://127.0.0.1:6060/debug/pprof/heap
```

The installed Go 1.27 distribution did not contain `go tool pprof`; its error was `go: no such tool "pprof"`. The snapshots therefore use the official `github.com/google/pprof` CLI (`pprof -top`), which fetches the same heap endpoint. This CLI does not allow its `-top` and `-text` flags together.

| Snapshot | `hey` accepted | Goroutines (raw pprof total) | Heap in use (raw pprof total) |
|---|---:|---:|---:|
| Before worker pool | 100 (`202`) | 93 | 703.10 MB |
| After worker pool | 100 (`202`) | 10 | 84.65 MB |

The baseline heap profile attributed 690 MB (98.14%) to `bytes.growSlice`, reached from `pdf.(*Generator).Generate`; the goroutine profile showed PDF generation as the dominant background work. Four workers were selected to cap simultaneous generators while keeping the 100-request benchmark admissible through the 100-entry queue. At the same scale, the pool reduced the sampled goroutine total by 89% and in-use heap by 88%; the after profile attributed 80 MB to `bytes.growSlice`.
