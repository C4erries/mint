# Use optional .env for local environment variables.
ENV_FILE ?= .env
ifneq (,$(wildcard $(ENV_FILE)))
include $(ENV_FILE)
export
endif

GO ?= go
GOLANGCI_LINT ?= golangci-lint
GO_TEST_FLAGS ?= -count=1
PROTOC ?= protoc
PROTO_GO_OPTS ?= paths=source_relative
PROTO_FILES := api/rtc/v1/query.proto api/permission/v1/permission.proto
DOCKER_COMPOSE ?= docker compose
RTC_COMPOSE_FILE ?= deploy/docker-compose/core.yml

.PHONY: help test test-race lint lint-fix proto proto-check check rtc-up rtc-down rtc-smoke

help:
	@echo "Targets:"
	@echo "  make test       - run go tests"
	@echo "  make test-race  - run go tests with race detector"
	@echo "  make lint       - run golangci-lint"
	@echo "  make lint-fix   - run golangci-lint with auto-fixes"
	@echo "  make proto      - generate Go code from proto files"
	@echo "  make proto-check - verify proto-generated files are up to date"
	@echo "  make rtc-up     - start RTC docker stack"
	@echo "  make rtc-down   - stop RTC docker stack"
	@echo "  make rtc-smoke  - run RTC smoke tests (requires docker stack)"
	@echo "  make check      - run test + lint"

test:
	$(GO) test ./... $(GO_TEST_FLAGS)

test-race:
	$(GO) test -race ./... $(GO_TEST_FLAGS)

lint:
	$(GOLANGCI_LINT) run ./...

lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

proto:
	$(PROTOC) --go_out=$(PROTO_GO_OPTS):. --go-grpc_out=$(PROTO_GO_OPTS):. $(PROTO_FILES)

proto-check: proto
	git diff --exit-code -- api

rtc-up:
	$(DOCKER_COMPOSE) -f $(RTC_COMPOSE_FILE) up -d

rtc-down:
	$(DOCKER_COMPOSE) -f $(RTC_COMPOSE_FILE) down

rtc-smoke:
	RTC_SMOKE_COMPOSE_FILE=$(RTC_COMPOSE_FILE) bash ./scripts/e2e/rtc-smoke.sh

check: test lint
