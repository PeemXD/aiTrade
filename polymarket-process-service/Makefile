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
	@nohup $(GO) run ./cmd/app > tmp/process.log 2>&1 & echo $$! > tmp/process.pid
	@echo "Started process service. PID: $$(cat tmp/process.pid)"
	@echo "Logs: tail -f tmp/process.log"

stop:
	@if [ -f tmp/process.pid ]; then kill $$(cat tmp/process.pid) 2>/dev/null || true; rm -f tmp/process.pid; echo "Stopped process service"; else echo "No process PID found"; fi

logs:
	$(DOCKER) compose -p $(COMPOSE_PROJECT) logs -f postgres redis kafka kafka-ui

kafka-ui:
	@echo "Kafka UI: http://localhost:8081"

clean:
	$(DOCKER) compose -p $(COMPOSE_PROJECT) down -v
	rm -rf tmp
