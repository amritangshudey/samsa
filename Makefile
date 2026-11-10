FUZZTIME ?= 10s

.PHONY: build test race lint fuzz

build:
	CGO_ENABLED=0 go build -trimpath -o bin/samsa ./cmd/samsa

test:
	go vet ./...
	go test ./...

race:
	go test -race ./...

lint:
	golangci-lint run

# Runs every fuzz target in the module for FUZZTIME each.
fuzz:
	@for pkg in $$(go list ./...); do \
		for f in $$(go test -list '^Fuzz' $$pkg | grep '^Fuzz'); do \
			go test -run '^$$' -fuzz "^$$f\$$" -fuzztime $(FUZZTIME) $$pkg || exit 1; \
		done; \
	done
