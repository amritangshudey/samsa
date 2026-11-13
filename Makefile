FUZZTIME ?= 10s

KAFKA_CLIENTS_VERSION ?= 4.4.0
SLF4J_VERSION ?= 2.0.17
MAVEN := https://repo1.maven.org/maven2
JARS := integration/.cache/kafka-clients-$(KAFKA_CLIENTS_VERSION).jar integration/.cache/slf4j-api-$(SLF4J_VERSION).jar
# The Kafka distribution, for its CLI tools (kafka-topics.sh and the rest),
# test-only. The SHA-512 is pinned here, copied from archive.apache.org's
# .sha512 file on 2026-11-13, so a changed download fails whichever mirror
# served it.
KAFKA_DIST_VERSION ?= 4.4.0
KAFKA_DIST_SHA512 := f5ceba8a265ab6bb2d68362b1f6859a95e15d60ef92ba83b43fdf065b557dc18e98d6383c15b9977320e25b3a2c8b6f11decb17b0fa6c0f163423793a0eb95cb
KAFKA_DIST := integration/.cache/kafka_2.13-$(KAFKA_DIST_VERSION)

SEEDS ?= 1000
# Replaying one seed: SKIP=1-4,7 skips those faults, CALM=1 calms the
# network, MUTATION="name" switches on a bug of the mutation suite, and
# TIMELINE=file.html draws the run (docs/design/a3-sim-tools.md).
REPLAY := $(if $(SKIP),-skip-faults=$(SKIP)) $(if $(CALM),-calm) $(if $(MUTATION),-mutation="$(MUTATION)") $(if $(TIMELINE),-timeline=$(abspath $(TIMELINE)))

# Empty unless set: the binary then reports the commit Go stamped into it.
VERSION ?=
LDFLAGS := -ldflags "-X main.version=$(VERSION)"
PLATFORMS := darwin/arm64 darwin/amd64 linux/arm64 linux/amd64

.PHONY: soak build test race lint fuzz integration chaos sim shrink bench release

build:
	CGO_ENABLED=0 go build -trimpath $(LDFLAGS) -o bin/samsa ./cmd/samsa
	# With deliberate bugs behind a hidden -unsafe flag, for the chaos test only.
	CGO_ENABLED=0 go build -trimpath -tags unsafe -o bin/samsa-unsafe ./cmd/samsa

# Static binaries for every platform in dist/, with SHA256SUMS. CI runs this
# on a version tag; see .github/workflows/release.yml.
release:
	test -n "$(VERSION)" || { echo "set VERSION, e.g. make release VERSION=v0.1.0"; exit 1; }
	mkdir -p dist
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		echo "build $$os/$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath $(LDFLAGS) -o dist/samsa-$(VERSION)-$$os-$$arch ./cmd/samsa || exit 1; \
	done
	cd dist && shasum -a 256 samsa-$(VERSION)-* > SHA256SUMS-$(VERSION)

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
	go test ./internal/sim -count=1 -v -run '^TestSim$$' -seed=$(SEED) $(REPLAY)
else
	go test ./internal/sim -count=1 -v -timeout 2h -run '^TestSim$$' -seeds=$(SEEDS)
endif

# Cut a failing seed down to the faults it needs: make shrink SEED=x
# (with MUTATION="name" for a seed that fails only under a mutation).
shrink:
	go test ./internal/sim -count=1 -v -run '^TestShrinkSeed$$' -shrink=$(SEED) $(REPLAY)

# Real clients (franz-go, kcat, the Java client) against the built binary.
# Needs kcat and a JDK; set SAMSA_REQUIRE_CLIENTS=1 to fail when one is missing.
integration: build $(JARS) $(KAFKA_DIST)
	cd integration && SAMSA_BIN=$(CURDIR)/bin/samsa go test -count=1 -v ./...

# The chaos test alone, for CHAOS_DURATION (default here 10m); CHAOS_SEED
# replays a scenario. Design: docs/design/a4-chaos.md.
CHAOS_DURATION ?= 10m
chaos: build
	cd integration && SAMSA_BIN=$(CURDIR)/bin/samsa CHAOS_DURATION=$(CHAOS_DURATION) go test -count=1 -v -timeout 2h -run '^TestChaos$$' .

# One node under a steady, churning workload; docs/design/g5-soak.md.
# SOAK_DURATION defaults to 24h. Samples and summary go to soak/.
SOAK_DURATION ?= 24h
soak: build
	cd integration && SAMSA_BIN=$(CURDIR)/bin/samsa SAMSA_SOAK=1 SAMSA_SOAK_DURATION=$(SOAK_DURATION) SAMSA_SOAK_OUT=$(CURDIR)/soak go test -count=1 -v -timeout 0 -run '^TestSoak$$' .

# Throughput and latency of the built binary; method in docs/design/phase-7.md.
# Takes about 15 minutes. Results go to bench.md.
bench: build
	cd integration && SAMSA_BIN=$(CURDIR)/bin/samsa SAMSA_BENCH=1 SAMSA_BENCH_OUT=$(CURDIR)/bench.md go test -count=1 -v -timeout 2h -run '^TestBench$$' .

integration/.cache/kafka-clients-%.jar:
	mkdir -p $(@D) && curl -sfL -o $@ $(MAVEN)/org/apache/kafka/kafka-clients/$*/kafka-clients-$*.jar

$(KAFKA_DIST):
	mkdir -p $(@D)/dl
	curl -sfL -o $(@D)/dl/kafka.tgz https://dlcdn.apache.org/kafka/$(KAFKA_DIST_VERSION)/kafka_2.13-$(KAFKA_DIST_VERSION).tgz || \
		curl -sfL -o $(@D)/dl/kafka.tgz https://archive.apache.org/dist/kafka/$(KAFKA_DIST_VERSION)/kafka_2.13-$(KAFKA_DIST_VERSION).tgz
	echo "$(KAFKA_DIST_SHA512)  $(@D)/dl/kafka.tgz" | shasum -a 512 -c -
	tar -xzf $(@D)/dl/kafka.tgz -C $(@D) && rm $(@D)/dl/kafka.tgz

integration/.cache/slf4j-api-%.jar:
	mkdir -p $(@D) && curl -sfL -o $@ $(MAVEN)/org/slf4j/slf4j-api/$*/slf4j-api-$*.jar
