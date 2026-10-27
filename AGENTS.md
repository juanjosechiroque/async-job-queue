# Instructions for agents and contributors

## Context

This repository contains a Go HTTP service that generates PDFs asynchronously. The current implementation is described in [README.md](README.md) and [ARCHITECTURE.md](ARCHITECTURE.md).

## Before modifying the project

1. Read `README.md` and `ARCHITECTURE.md`.
2. Check the repository state with `git status --short`.
3. Preserve existing user changes.
4. Confirm that the documentation still describes the actual code, not a future architecture.

## Current structure

- `cmd/server`: server startup and dependency wiring.
- `internal/jobs`: handler, request model, business service, `JobStore` interface, in-memory store.
- `internal/postgres`: Postgres-backed `JobStore` and embedded schema.
- `internal/ratelimit`: per-IP rate limiting middleware.
- `internal/pdf`: PDF generation.
- `internal/text`: per-line text generation.

## Implementation rules

- Prefer the Go standard library before adding external dependencies.
- Keep validation logic in `internal/jobs` and generation logic in `internal/pdf`.
- Keep HTTP handlers thin: decode the request, invoke the service, and build the response.
- Validate `lines` before generating the PDF.
- Keep the allowed range between 1 and 250,000.
- Do not expose internal error details to the client.
- Escape text correctly before inserting it into the PDF.
- Do not add a queue, workers, persistent storage, or new endpoints as part of a minor refactor without first updating the architecture and HTTP contract.

## Current API

The implemented asynchronous endpoints are:

```text
POST /jobs
GET  /jobs/{id}
GET  /jobs/{id}/file
```

Job creation returns JSON; completed PDF files are returned by `GET /jobs/{id}/file`.

## Required verification

After modifying Go code:

```bash
make check
```

This runs `gofmt`, `go vet`, and `go test ./... -race`. When changing `internal/postgres` or claiming logic, also run with `DATABASE_URL` set (after `docker compose up -d --wait`) so the Postgres integration test runs instead of skipping.

When modifying the HTTP endpoint, verify at least:

- valid JSON body;
- valid line count;
- invalid JSON;
- out-of-range line count;
- unsupported HTTP method;
- PDF response headers;
- response file beginning with `%PDF`.

## Documentation

Update `README.md` and `ARCHITECTURE.md` when any of the following change:

- startup commands;
- endpoints;
- request or response formats;
- validation limits;
- package structure;
- generation behavior;
- runtime requirements.

Documentation must always distinguish implemented functionality from future proposals.
