# Current architecture

## Scope

This document describes only the platform that is currently implemented. The service is a synchronous HTTP API for generating PDFs.

The current architecture does not include a queue, worker pool, `jobId`, database, local PDF storage, or a status-query system.

## Overview

```text
HTTP client
    |
    | POST /job
    v
HTTP Server :8080
    |
    v
job.Handler
    |
    v
job.Service
    |
    v
pdf.Generator
    |
    v
text.Generator
    |
    v
application/pdf
```

## Startup

The entry point is `cmd/server/main.go`.

Dependencies are composed at startup in this order:

1. `text.NewGenerator()` creates the text generator.
2. `pdf.NewGenerator(textGenerator)` creates the PDF generator.
3. `job.NewService(pdfGenerator)` creates the business service.
4. `job.NewHandler(jobService)` creates the HTTP handler.
5. The handler is registered at `/job`.
6. `http.ListenAndServe(":8080", mux)` starts the server.

No environment variables are read and no external connections are initialized.

## Components

### `cmd/server`

Contains dependency wiring and HTTP server startup.

### `internal/job`

Contains the HTTP contract and business validation.

`GenerateRequest` represents the request body:

```go
type GenerateRequest struct {
	Lines int `json:"lines"`
}
```

`Service.GeneratePDF` validates that `lines` is between 1 and 20,000 before invoking the generator.

### `internal/pdf`

Generates a PDF 1.4 document in memory.

Current characteristics:

- 29 lines per page.
- Helvetica font.
- 612 × 792 point page size.
- Randomly generated text.
- The result is returned as `[]byte`.

The generator creates a local `rand.Rand` for each generation and builds the PDF objects, pages, `xref` table, and trailer.

### `internal/text`

Maintains a fixed set of sample texts and randomly selects one for each line.

## HTTP contract

### `POST /job`

The handler accepts only the `POST` method.

Request:

```http
POST /job HTTP/1.1
Content-Type: application/json

{"lines":10}
```

Successful response:

```http
HTTP/1.1 200 OK
Content-Type: application/pdf
Content-Disposition: attachment; filename=job-lines-10.pdf
```

The PDF is written directly to the response body.

## Error handling

The handler uses JSON responses for errors:

```json
{
  "error": "message"
}
```

Implemented rules:

- Invalid JSON: `400 Bad Request`.
- Invalid line count: `400 Bad Request`.
- Method not allowed: `405 Method Not Allowed`.
- Unexpected generation error: `500 Internal Server Error`.

Internal generation details are not exposed to the client; the generic message `could not generate PDF` is returned.

## Concurrency and memory

The application does not create workers or manage its own queue. PDF processing occurs inside the HTTP request.

The complete document is built in memory as `[]byte` before it is written to the response. Therefore:

- Each active request consumes memory proportional to its PDF size.
- The application does not define a global concurrency limit.
- There is no generation-specific backpressure.
- There is no retry or recovery if the process restarts.

The 20,000-line limit controls the maximum size of an individual request, but it does not impose a global limit on concurrent requests.

## Persistence

There is no persistence. The service does not store:

- jobs;
- statuses;
- generated PDFs;
- request history.

Once the response is complete, the result is no longer available on the server.

## Known limitations

- A request remains open until generation finishes.
- The client must wait for the PDF on the same connection.
- There is no `jobId` or later result lookup.
- The result cannot be recovered after the connection is closed.
- Memory usage grows with PDF size and the number of concurrent requests.
- The port is not configurable; it is currently fixed at `:8080`.
- There is no business logging or metrics.
- There is no authentication or authorization.

These are characteristics of the current version, not documentation errors.
