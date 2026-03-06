# Use optional .env for local environment variables.
ENV_FILE ?= .env
ifneq (,$(wildcard $(ENV_FILE)))
include $(ENV_FILE)
export
endif

GO ?= go
GOLANGCI_LINT ?= golangci-lint
GO_TEST_FLAGS ?= -count=1

.PHONY: help test test-race lint check

help:
	@echo "Targets:"
	@echo "  make test       - run go tests"
	@echo "  make test-race  - run go tests with race detector"
	@echo "  make lint       - run golangci-lint"
	@echo "  make check      - run test + lint"

test:
	$(GO) test ./... $(GO_TEST_FLAGS)

test-race:
	$(GO) test -race ./... $(GO_TEST_FLAGS)

lint:
	$(GOLANGCI_LINT) run ./...

check: test lint
