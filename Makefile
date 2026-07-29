.PHONY: infra-up infra-down migrate build test fmt run-ingestor run-writer run-event-parser

GO_IMAGE ?= golang:1.24
WORKDIR := /workspace

infra-up:
	docker compose up -d redpanda clickhouse postgres redis minio

infra-down:
	docker compose down

migrate:
	docker compose run --rm migrate

build:
	docker run --rm -v "$(CURDIR):$(WORKDIR)" -w $(WORKDIR) $(GO_IMAGE) go build ./...

test:
	docker run --rm -v "$(CURDIR):$(WORKDIR)" -w $(WORKDIR) $(GO_IMAGE) go test ./...

fmt:
	docker run --rm -v "$(CURDIR):$(WORKDIR)" -w $(WORKDIR) $(GO_IMAGE) gofmt -w .

run-ingestor:
	docker compose up --build block-ingestor

run-writer:
	docker compose up --build block-writer

run-event-parser:
	docker compose up --build event-parser
