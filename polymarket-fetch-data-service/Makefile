SHELL := /bin/bash
COMPOSE_PROJECT := polymarket-bot
GO ?= $(shell command -v go 2>/dev/null || echo /opt/homebrew/bin/go)
DOCKER ?= $(shell command -v docker 2>/dev/null || echo /opt/homebrew/bin/docker)

.PHONY: setup migrate test run run-bg stop logs kafka-ui clean

setup:
	@if [ ! -f .env ]; then cp configs/config.example.env .env; fi
	$(DOCKER) compose -p $(COMPOSE_PROJECT) up -d postgres redis kafka kafka-ui
	@until $(DOCKER) compose -p $(COMPOSE_PROJECT) exec -T postgres pg_isready -U postgres -d polymarket_bot >/dev/null 2>&1; do sleep 1; done
	@until $(DOCKER) compose -p $(COMPOSE_PROJECT) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 1; done
	$(MAKE) migrate

migrate:
	./scripts/migrate.sh

test:
	$(GO) test ./...

run:
	$(MAKE) setup
	$(GO) run ./cmd/app

run-bg:
	$(MAKE) setup
	@mkdir -p tmp
	@nohup $(GO) run ./cmd/app > tmp/fetch-data.log 2>&1 & echo $$! > tmp/fetch-data.pid
	@echo "Started fetch-data service. PID: $$(cat tmp/fetch-data.pid)"
	@echo "Logs: tail -f tmp/fetch-data.log"

stop:
	@if [ -f tmp/fetch-data.pid ]; then kill $$(cat tmp/fetch-data.pid) 2>/dev/null || true; rm -f tmp/fetch-data.pid; echo "Stopped fetch-data service"; else echo "No fetch-data PID found"; fi

logs:
	$(DOCKER) compose -p $(COMPOSE_PROJECT) logs -f postgres redis kafka kafka-ui

kafka-ui:
	@echo "Kafka UI: http://localhost:8081"

clean:
	$(DOCKER) compose -p $(COMPOSE_PROJECT) down -v
	rm -rf tmp
