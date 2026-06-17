# Real-Time Crypto Streaming Pipeline

Real-time BTC/ETH market data pipeline: Coinbase WebSocket → Kafka → Spark Structured Streaming → Elasticsearch + S3.

## Architecture

```
Coinbase WebSocket → Kafka (crypto-ticks) → Spark Structured Streaming
                                                      │
                              ┌───────────────────────┤
                              ▼                       ▼
                       Elasticsearch (Kibana)     Amazon S3 (Parquet)
```

## Stack

| Layer | Tech |
|---|---|
| Source | Coinbase WebSocket API |
| Queue | Apache Kafka 7.5.0 (KRaft) |
| Processing | Spark Structured Streaming |
| Hot storage | Elasticsearch + Kibana |
| Cold storage | S3 (Parquet, Hive-partitioned) |
| Infra | Docker Compose |

## Pipeline Stages

**Parse → Clean → Deduplicate → Enrich → Quality Filter → Window Aggregate → Sink**

- **Clean**: 10 filters (nulls, range checks, crossed-market, symbol normalisation)
- **Dedupe**: `dropDuplicates(["product_id", "event_time", "price"])`
- **Enrich**: `spread`, `spread_pct`, `mid_price`, `latency_seconds`, `quality_score`
- **Windows**: 1m / 5m / 15m OHLC aggregations written to `crypto_agg_*` indices and S3

## Quick Start

```bash
# 1. Generate Kafka cluster ID and paste into docker-compose.yml
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid

# 2. Start all services
docker-compose up -d

# 3. Create topic
docker exec -it kafka kafka-topics --create --topic crypto-ticks \
  --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

# 4. Run producer
source venv/bin/activate && python producer.py
```

## Monitoring

| UI | URL | Signal |
|---|---|---|
| Kafka UI | localhost:8080 | Consumer lag (alert > 500 msgs) |
| Spark UI | localhost:4040 | Micro-batch duration vs trigger |
| Kibana | localhost:5601 | Missing candles = pipeline stalled |

## Throughput

~989 msg/min average, ~35 msg/s peak, ~177 bytes/msg. 2 Kafka partitions (1 per symbol). Kafka retention: 6 hours.

## Known Production Gaps

- Checkpoints in `/tmp` — move to S3 or persistent volume
- No Dead Letter Queue for bad records
- WebSocket reconnect is manual — needs exponential backoff loop
- Kafka idempotent producer not yet enabled
- No automated alerting (Prometheus/PagerDuty)
