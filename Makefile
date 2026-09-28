.PHONY: up down deps fmt vet test race it it-docker load migrate-up migrate-down

up:
	docker compose up --build -d

down:
	docker compose down -v

deps:
	docker compose up -d postgres keycloak ministack

fmt:
	gofmt -l -w .

vet:
	go vet ./...
	go vet -tags integration ./...

test:
	go test ./...

race:
	go test -race ./...

it: deps
	go test -race -tags integration -count=1 ./test/integration/...

it-docker:
	docker compose --profile test run --rm tests

load:
	go run ./cmd/loadtest -duration 30s -concurrency 32 -wallets 50

migrate-up:
	docker compose run --rm migrate migrate up

migrate-down:
	docker compose run --rm migrate migrate down 1
