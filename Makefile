SHELL := /bin/bash

.PHONY: serve dev build test test-int lint migrate-up migrate-down down logs bench

serve:
	docker compose up --build -d
	docker compose logs -f api worker

dev:
	docker compose up -d postgres
	@echo "postgres up on localhost:5432 - run 'make migrate-up' then 'go run ./cmd/api' / 'go run ./cmd/worker'"

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
	docker compose down -v

logs:
	docker compose logs -f

bench:
	go run ./cmd/bench
