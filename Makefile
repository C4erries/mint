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

.PHONY: help test test-race lint lint-fix proto proto-check check

help:
	@echo "Targets:"
	@echo "  make test       - run go tests"
	@echo "  make test-race  - run go tests with race detector"
	@echo "  make lint       - run golangci-lint"
	@echo "  make lint-fix   - run golangci-lint with auto-fixes"
	@echo "  make proto      - generate Go code from proto files"
	@echo "  make proto-check- verify proto-generated files are up to date"
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

check: test lint
