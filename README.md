# Async Job Queue

Go HTTP service that generates a PDF containing random text based on a requested number of lines.

## Current status

The platform is currently synchronous. Each request generates the PDF during the same HTTP connection and returns the file directly:

```text
POST /job -> validate request -> generate PDF -> return PDF
```

There is currently no job queue, explicit worker pool, result storage, database, or status-query endpoint.

## Requirements

- Go 1.27, according to the version declared in `go.mod`.
- No external services or environment variables are required.

## Run the server

From the project root:

```bash
go run ./cmd/server
```

The server listens on:

```text
http://localhost:8080
```

Press `Ctrl+C` to stop it.

## Available endpoint

### Generate a PDF

```http
POST /job
Content-Type: application/json
```

Request body:

```json
{
  "lines": 10
}
```

`lines` must be between `1` and `20000`.

Successful response:

```http
200 OK
Content-Type: application/pdf
Content-Disposition: attachment; filename=job-lines-10.pdf
```

The response body contains the generated PDF.

### Test with curl

```bash
curl -X POST http://localhost:8080/job \
  -H 'Content-Type: application/json' \
  -d '{"lines":10}' \
  -o result.pdf
```

### Test with Postman

1. Method: `POST`.
2. URL: `http://localhost:8080/job`.
3. Body → `raw` → `JSON`.
4. Use `{ "lines": 10 }`.
5. Choose **Send and Download** to save the PDF.

## Error responses

| Situation | HTTP | Response |
|---|---:|---|
| Method other than `POST` | `405` | `{"error":"method not allowed"}` |
| Invalid request body | `400` | `{"error":"request body must be valid JSON"}` |
| `lines` outside the allowed range | `400` | `{"error":"lines must be between 1 and 20000"}` |
| Internal generation error | `500` | `{"error":"could not generate PDF"}` |

Errors are returned as `application/json; charset=utf-8`.

## Verification

Run from the project root:

```bash
go test ./...
```

This command compiles all packages and runs the available tests.

## Current structure

```text
cmd/server/main.go       HTTP server entry point and dependency wiring
internal/job/handler.go  POST /job HTTP handler
internal/job/model.go    GenerateRequest request model
internal/job/service.go  Validation and generation service
internal/pdf/             PDF document generation
internal/text/            Per-line text selection
go.mod                    Go module and version
```

## Request flow

```text
Client
  |
  | POST /job {"lines": 10}
  v
HTTP Handler
  |
  v
Job Service
  |
  v
PDF Generator + Text Generator
  |
  v
PDF response
```

The PDF is built in memory during the request. It is not saved to disk, and no identifier is registered for later retrieval.
