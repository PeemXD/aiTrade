SHELL := /bin/bash

GO ?= $(shell command -v go 2>/dev/null || echo /opt/homebrew/bin/go)
DOCKER ?= $(shell command -v docker 2>/dev/null || echo /opt/homebrew/bin/docker)
FETCH_DATA_DIR := polymarket-fetch-data-service
PROCESS_DIR := polymarket-process-service
COMPOSE_PROJECT := polymarket-bot

.PHONY: setup migrate test run-fetch run-process run run-once kafka-ui logs clean

setup:
	@if [ ! -f .env ]; then cp configs/config.example.env .env; fi
	@if [ ! -f $(FETCH_DATA_DIR)/.env ]; then cp $(FETCH_DATA_DIR)/configs/config.example.env $(FETCH_DATA_DIR)/.env; fi
	@if [ ! -f $(PROCESS_DIR)/.env ]; then cp $(PROCESS_DIR)/configs/config.example.env $(PROCESS_DIR)/.env; fi
	$(DOCKER) compose -p $(COMPOSE_PROJECT) up -d postgres redis kafka kafka-ui
	@until $(DOCKER) compose -p $(COMPOSE_PROJECT) exec -T postgres pg_isready -U postgres -d polymarket_bot >/dev/null 2>&1; do sleep 1; done
	@until $(DOCKER) compose -p $(COMPOSE_PROJECT) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 1; done
	$(MAKE) migrate

migrate:
	$(DOCKER) compose -p $(COMPOSE_PROJECT) exec -T postgres psql -U postgres -d polymarket_bot < $(PROCESS_DIR)/migrations/001_init.sql

test:
	cd $(FETCH_DATA_DIR) && $(GO) test ./...
	cd $(PROCESS_DIR) && $(GO) test ./...

run-fetch:
	@if [ ! -f $(FETCH_DATA_DIR)/.env ]; then cp $(FETCH_DATA_DIR)/configs/config.example.env $(FETCH_DATA_DIR)/.env; fi
	cd $(FETCH_DATA_DIR) && $(GO) run ./cmd/app

run-process:
	@if [ ! -f $(PROCESS_DIR)/.env ]; then cp $(PROCESS_DIR)/configs/config.example.env $(PROCESS_DIR)/.env; fi
	cd $(PROCESS_DIR) && $(GO) run ./cmd/app

run:
	GO=$(GO) DOCKER=$(DOCKER) COMPOSE_PROJECT=$(COMPOSE_PROJECT) ./scripts/run-local.sh

run-once:
	curl -X POST http://localhost:8080/api/v1/live/run-once

kafka-ui:
	@echo "Kafka UI: http://localhost:8081"

logs:
	$(DOCKER) compose -p $(COMPOSE_PROJECT) logs -f postgres redis kafka kafka-ui

clean:
	$(DOCKER) compose -p $(COMPOSE_PROJECT) down -v
	rm -rf tmp
