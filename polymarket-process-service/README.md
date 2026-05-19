# polymarket-process-service

Processing and paper-trading service for the local AI prediction market trading system.

This project is independently deployable. It owns decision and trading workflow only:

- News-market matcher
- AI signal service
- Probability engine
- Risk engine
- Paper/real execution engine
- Position engine
- Exit engine

It reads durable state from PostgreSQL, connects to Kafka on startup, and paper trades by default.

## Project Structure

```text
cmd/
  app/

app/
  config.go
  secret.go
  aiSignal/
  newsMarketMatcher/
  probabilityEngine/
  riskEngine/
  executionEngine/
  positionEngine/
  exitEngine/

pkg/
  config/
  logger/
  model/
  repository/
  runtime/
  textmatch/

configs/
migrations/
scripts/
tests/
```

## Requirements

- Go 1.24+
- Docker / Docker Compose

## Local Setup

```bash
cp configs/config.example.env .env
make setup
```

`make setup` starts:

- PostgreSQL
- Redis
- Kafka

and applies schema migration.

## Run

```bash
make run
```

The service starts automatically. No curl call is required.

Runtime behavior:

1. Connect to Kafka.
2. Connect to PostgreSQL.
3. Load markets and news persisted by `polymarket-fetch-data-service`.
4. Match news to markets.
5. Call AI when configured.
6. Calculate probability and edge.
7. Evaluate deterministic risk checks.
8. Create paper trades when approved.
9. Monitor and close positions through exit logic.

## Test

```bash
make test
```

Normal tests do not require internet.

## Deploy

Build container:

```bash
docker build -t polymarket-process-service .
```

Run container with environment variables from `configs/config.example.env`.

Important env:

```env
DATABASE_URL=postgres://postgres:postgres@localhost:5432/polymarket_bot?sslmode=disable
KAFKA_BROKERS=localhost:9092
AI_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai
AI_API_KEY=
AI_MODEL=gemini-2.5-flash
EXECUTION_MODE=paper
ENABLE_REAL_TRADING=false
```

Real trading is disabled unless all explicit confirmation flags and credentials are configured.
