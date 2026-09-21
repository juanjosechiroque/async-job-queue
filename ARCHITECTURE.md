# Current architecture

## Scope

This document describes the implemented asynchronous PDF service. It does not describe a future queue-based architecture.

The service stores job metadata in memory and generated PDFs on the local filesystem. It has no persistent job database, worker pool, configured concurrency limit, or automatic retention policy.

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
                     | one goroutine per accepted job
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
3. Wait for tracked background jobs to finish, sharing a 30-second deadline with HTTP shutdown.
4. Exit when jobs finish or the deadline expires.

Job goroutines do not use the originating request context, so an accepted job can continue after its creation response has been sent.

## HTTP contract

### `POST /jobs`

Validates `lines` in `internal/jobs` between 1 and 250,000, creates a `queued` job in the in-memory store, starts its goroutine, and returns `202 Accepted` with:

```json
{
  "jobId": "a8K2mP91xQ",
  "status": "queued",
  "downloadUrl": "/jobs/a8K2mP91xQ/file"
}
```

There is no `statusUrl` response field. `POST /job` is not registered.

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

There is intentionally no application-level concurrency limit: each accepted request starts one goroutine. A `sync.WaitGroup` tracks those goroutines for shutdown.

Both the job store and PDFs are retained until manual cleanup. Job state is lost after process restart, while existing storage files remain on disk but cannot be retrieved because no in-memory job metadata exists.

Operational metrics, workload logs, disk-usage monitoring, and pprof exposure are not implemented yet.
