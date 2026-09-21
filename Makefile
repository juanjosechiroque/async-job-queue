.PHONY: fmt fmt-check vet test build check

fmt:
	gofmt -l -w .

fmt-check:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

test:
	go test ./... -race

build:
	go build ./...

check: fmt vet test
