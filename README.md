# AI Trade Polymarket Bot

This repository keeps the existing two-service layout:

- `polymarket-fetch-data-service`: fetches Polymarket market data, market stream updates, and news, then publishes normalized events.
- `polymarket-process-service`: consumes Kafka events, runs news matching, AI signal generation, probability/risk evaluation, paper execution, position monitoring, REST API, and dashboard SSE.

The services communicate through local Kafka. PostgreSQL stores durable market/news/trade/audit data, Redis stores live state/cache data, Polymarket WebSocket is external data ingestion, REST is the control plane, and SSE is the internal dashboard stream.

## Local Infrastructure

Root `docker-compose.yml` starts:

- PostgreSQL on `localhost:5432`
- Redis on `localhost:6379`
- Kafka for host clients on `localhost:29092`
- Kafka for containers on `kafka:9092`
- Kafka UI on `http://localhost:8081`

Kafka is the shared event bus. All messages use the `EventEnvelope` JSON shape and topic-specific keys such as `market_id`, `news_id`, `trade_id`, and `position_id`.

## Run Locally

```bash
cp configs/config.example.env .env
make setup
make test
make run-fetch
make run-process
```

Useful checks:

```bash
curl http://localhost:8080/health
curl -N http://localhost:8080/api/v1/dashboard/stream
curl -X POST http://localhost:8080/api/v1/live/run-once
```

Kafka UI:

```text
http://localhost:8081
```

## Process API

The process service exposes:

- `GET /health`
- `GET /api/v1/dashboard/summary`
- `GET /api/v1/dashboard/stream`
- `GET /api/v1/markets`
- `GET /api/v1/news`
- `GET /api/v1/signals`
- `GET /api/v1/probability-decisions`
- `GET /api/v1/risk-decisions`
- `GET /api/v1/trades`
- `GET /api/v1/positions`
- `GET /api/v1/portfolio`
- `GET /api/v1/audit`
- `POST /api/v1/live/run-once`
- `POST /api/v1/live/start`
- `POST /api/v1/live/stop`
- `POST /api/v1/positions/{id}/close`

## Dashboard SSE

Connect with:

```bash
curl -N http://localhost:8080/api/v1/dashboard/stream
```

The stream emits `connected`, `heartbeat`, market/news/signal/probability/risk/trade/position/portfolio/audit events, and `error`. High-volume market, position, and portfolio updates are throttled so the stream cannot block Kafka consumers or trading flow.

## Safety

Execution defaults to paper mode. Real trading is ignored unless `EXECUTION_MODE=real`, `ENABLE_REAL_TRADING=true`, and `REAL_TRADING_CONFIRMATION=I_UNDERSTAND_REAL_TRADING` are all set.

Normal tests use mocks and in-memory stores, so `make test` and `go test ./...` do not require Kafka, Docker, or internet access.
# aiTrade
