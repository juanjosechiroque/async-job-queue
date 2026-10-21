.PHONY: fmt vet test build check

fmt:
	gofmt -l -w .

vet:
	go vet ./...

test:
	go test ./... -race

build:
	go build ./...

check: fmt vet test
