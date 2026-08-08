SHELL := /bin/bash

.PHONY: serve dev build test test-int lint migrate-up migrate-down down logs bench

serve:
	@echo "TODO(phase-8): docker compose up --build, wait healthy, migrate"

dev:
	@echo "TODO(phase-8): run api + worker locally with Postgres in Docker"

build:
	go build ./cmd/...

test:
	go test -race ./...

test-int:
	go test -race -tags=integration ./...

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

down:
	@echo "TODO(phase-8): stop stack and remove volumes"

logs:
	@echo "TODO(phase-8): stream compose logs"

bench:
	go run ./cmd/bench
