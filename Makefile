SHELL := /bin/bash
GO := go
GOOS ?= $(shell uname -s | tr [:upper:] [:lower:])
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)
PROTOFILE := proto/skyflow.proto
# GOTEST ?= ${GO} test
GOTEST ?= ${GOPATH}/bin/richgo test
FLAGS := -ldflags="-s -w"


.PHONY: init
init:
	@echo "initializing........"
	${GO} mod tidy


.PHONY: lint
lint:
	@if golangci-lint config path > /dev/null 2>&1; \
	then \
		golangci-lint run --allow-parallel-runners; \
	else \
		echo ".golangci.yml file not found, use golangci-lint with selected linters. Please add .golangci.yml if needed"; \
		golangci-lint run --allow-parallel-runners --disable-all --enable=govet,ineffassign,staticcheck,typecheck; \
	fi

.PHONY: lint_fix
lint_fix:
	@if golangci-lint config path > /dev/null 2>&1; \
	then \
		golangci-lint run --allow-parallel-runners; \
	else \
		echo ".golangci.yml file not found, use golangci-lint with selected linters. Please add .golangci.yml if needed"; \
		golangci-lint run --fix --allow-parallel-runners --disable-all --enable=govet,ineffassign,staticcheck,typecheck; \
	fi


.PHONY: test
test:
	@echo "start unittest........"
	${GOTEST} -v ./...