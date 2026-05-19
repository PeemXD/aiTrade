# polymarket-fetch-data-service

Fetch-data service for the local AI prediction market trading system.

This project is independently deployable. It owns external data ingestion only:

- Polymarket market scanner
- Polymarket market WebSocket stream
- GDELT/RSS news fetcher

It writes durable state to PostgreSQL, writes live/dedupe state to Redis, and connects to Kafka on startup.

## Project Structure

```text
cmd/
  app/

app/
  config.go
  secret.go
  marketScanner/
  marketStream/
  newsFetcher/

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
2. Connect to PostgreSQL and Redis.
3. Refresh selected Polymarket markets.
4. Start Polymarket WebSocket market stream.
5. Fetch GDELT/RSS news on interval.
6. Persist markets, live states, news, and audit logs.

## Test

```bash
make test
```

Normal tests do not require internet. External tests are guarded by `E2E_EXTERNAL=1`.

## Deploy

Build container:

```bash
docker build -t polymarket-fetch-data-service .
```

Run container with environment variables from `configs/config.example.env`.

Important env:

```env
DATABASE_URL=postgres://postgres:postgres@localhost:5432/polymarket_bot?sslmode=disable
REDIS_ADDR=localhost:6379
KAFKA_BROKERS=localhost:9092
POLYMARKET_GAMMA_BASE_URL=https://gamma-api.polymarket.com
POLYMARKET_CLOB_BASE_URL=https://clob.polymarket.com
POLYMARKET_WS_MARKET_URL=wss://ws-subscriptions-clob.polymarket.com/ws/market
NEWS_GDELT_BASE_URL=https://api.gdeltproject.org/api/v2/doc/doc
NEWS_RSS_FEEDS=https://www.coindesk.com/arc/outboundfeeds/rss/,https://cointelegraph.com/rss
```
