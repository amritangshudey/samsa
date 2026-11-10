FUZZTIME ?= 10s

KAFKA_CLIENTS_VERSION ?= 4.4.0
SLF4J_VERSION ?= 2.0.17
MAVEN := https://repo1.maven.org/maven2
JARS := integration/.cache/kafka-clients-$(KAFKA_CLIENTS_VERSION).jar integration/.cache/slf4j-api-$(SLF4J_VERSION).jar

SEEDS ?= 1000

.PHONY: build test race lint fuzz integration sim

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

# The simulator. `make sim SEEDS=5000` runs that many seeds and prints the
# failing ones; `make sim SEED=1234` replays one seed with its event trace.
sim:
ifdef SEED
	go test ./internal/sim -count=1 -v -run '^TestSim$$' -seed=$(SEED)
else
	go test ./internal/sim -count=1 -v -timeout 2h -run '^TestSim$$' -seeds=$(SEEDS)
endif

# Real clients (franz-go, kcat, the Java client) against the built binary.
# Needs kcat and a JDK; set SAMSA_REQUIRE_CLIENTS=1 to fail when one is missing.
integration: build $(JARS)
	cd integration && SAMSA_BIN=$(CURDIR)/bin/samsa go test -count=1 -v ./...

integration/.cache/kafka-clients-%.jar:
	mkdir -p $(@D) && curl -sfL -o $@ $(MAVEN)/org/apache/kafka/kafka-clients/$*/kafka-clients-$*.jar

integration/.cache/slf4j-api-%.jar:
	mkdir -p $(@D) && curl -sfL -o $@ $(MAVEN)/org/slf4j/slf4j-api/$*/slf4j-api-$*.jar
