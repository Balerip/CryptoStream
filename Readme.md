# Real-Time Crypto Market Data Pipeline

A production-grade streaming pipeline that ingests live BTC/ETH tick data from Coinbase WebSocket, processes it through Apache Kafka and Spark Structured Streaming, and delivers multi-resolution OHLC aggregations to Elasticsearch and Amazon S3.

---

## Architecture Overview

```
Coinbase WebSocket (BTC-USD, ETH-USD)
        │
        ▼
  Ingestion Service
        │
        ▼
  Apache Kafka (crypto-ticks)
        │
        ▼
  Spark Structured Streaming
  ┌─────┴──────────────────────────────────┐
  │  Parse → Clean → Deduplicate           │
  │  → Enrich → Quality Score → Filter     │
  └─────┬──────────────────────────────────┘
        │
  ┌─────▼──────────────────────────────────┐
  │  Window Aggregations (1m / 5m / 15m)   │
  └─────┬──────────────────────────────────┘
        │
   ┌────┴────┐
   ▼         ▼
Elasticsearch  Amazon S3
(Kibana        (Parquet / Athena /
 dashboards)    historical analytics)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source | Coinbase WebSocket API |
| Message Queue | Apache Kafka 7.5.0 (KRaft mode) |
| Stream Processing | Apache Spark Structured Streaming |
| Hot Storage | Elasticsearch + Kibana |
| Cold Storage | Amazon S3 (Parquet, Hive-partitioned) |
| Containerisation | Docker / Docker Compose |

---

## Features

- **Real-time ingestion** at ~16.5 msg/s average (up to 35 msg/s at peak)
- **Multi-resolution aggregations**: 1-minute, 5-minute, and 15-minute OHLC windows
- **9-step data quality framework**: null checks, range validation, crossed-market detection, symbol normalisation, deduplication, spread enrichment, latency measurement, quality scoring, and suspicious spread flagging
- **Idempotent writes** to Elasticsearch via deterministic `doc_id` (`product_id_window_start_ts`)
- **Dual-sink output**: Elasticsearch for live dashboards, S3 Parquet for historical analysis
- **Exactly-once semantics** at the producer level (`enable_idempotence=True`, `acks=all`)
- **Skew monitoring** via `foreachBatch` — BTC:ETH split is ~63:37, within the safe threshold

---

## Data Flow

### 1. Ingestion
The WebSocket client subscribes to the Coinbase `ticker` channel for `BTC-USD` and `ETH-USD`. Each incoming tick is serialised as JSON and produced to the `crypto-ticks` Kafka topic.

```python
subscribe_msg = {
    "type": "subscribe",
    "channels": [
        {"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD"]}
    ]
}
```

### 2. Transformation Pipeline (Spark)

| Stage | What happens |
|---|---|
| **Parsing** | Binary Kafka bytes → UTF-8 → `from_json()` with explicit schema; `ingestion_time` stamped |
| **Cleaning** | 10 filter conditions (null checks, range checks, crossed-market check, symbol normalisation) |
| **Deduplication** | `dropDuplicates(["product_id", "event_time", "price"])` |
| **Enrichment** | Derives `spread`, `spread_pct`, `mid_price`, `latency_seconds`, `quality_score`, `is_suspicious_spread` |
| **Quality filter** | Drops ticks with `quality_score < 50` or `is_suspicious_spread = true` |

**Quality scoring:**

| Score | Condition | Meaning |
|---|---|---|
| 100 | latency < 5s AND spread_pct < 1% | Fresh tick, tight spread |
| 80 | latency < 10s AND spread_pct < 5% | Acceptable latency and spread |
| 50 | All other | Stale or wide-spread tick |

### 3. Windowed Aggregation

Three parallel streams run with independent watermarks:

| Stream | Window | Watermark | Trigger | Use |
|---|---|---|---|---|
| `q1m` | 1 min | 1 min | 2 sec | Live dashboard |
| `q5m` | 5 min | 6 min | 5 sec | Short-term trends |
| `q15m` | 15 min | 11 min | 10 sec | Medium-term trends + S3 |

Each window produces: `avg_price`, `min_price`, `max_price`, `tick_count`, `total_volume` per `product_id`.

### 4. Storage

**Elasticsearch (hot path)**
- Indices: `crypto_agg_1m`, `crypto_agg_5m`, `crypto_agg_15m`
- Retention: 1–2 days (rolling); old indices overwritten via `doc_id`
- Used by Kibana for live dashboards

**Amazon S3 (cold path)**
- Path: `s3://crypto-data-pk/aggregates/window=<1m|5m|15m>/year=.../month=.../day=.../`
- Format: Parquet, Hive-partitioned
- Retention: Indefinite (Glacier transition recommended after 90 days in production)

**Kafka (transit layer)**
- Retention: 6 hours (`KAFKA_LOG_RETENTION_MS`)
- Not an archive — sized for Spark recovery only

---

## Kafka Throughput Profile

| Metric | Value |
|---|---|
| Average messages/min | ~989 |
| Average messages/sec | ~16.5 |
| Peak messages/sec | ~35 |
| Message size (key + value) | ~177 bytes |
| Peak throughput | ~6.05 KB/s |
| Observed throughput | ~2.85 KB/s |
| Recommended partitions | 2 (1 per symbol) |

---

## Running the Pipeline

### Prerequisites
- Docker + Docker Compose
- AWS credentials (for S3 writes)
- Python 3.x with a virtual environment

### 1. Start Kafka

Generate a cluster ID and update `docker-compose.yml`:

```bash
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
# Paste the UUID into CLUSTER_ID in docker-compose.yml
docker-compose up -d
```

Verify KRaft mode:
```bash
docker logs kafka  # Should show: Running in KRaft mode
docker exec -it kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### 2. Create the Kafka Topic

```bash
docker exec -it kafka kafka-topics --create \
  --topic crypto-ticks \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1
```

### 3. Start the WebSocket Producer

```bash
.\venv\Scripts\Activate.ps1   # Windows
source venv/bin/activate       # macOS/Linux
python producer.py
```

Verify messages are arriving:
```bash
docker exec -it kafka kafka-console-consumer \
  --topic crypto-ticks \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.key=true
```

### 4. Start Spark Streaming

```bash
docker-compose up spark
```

### 5. View Dashboards

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark UI | http://localhost:4040 |
| Kibana | http://localhost:5601 |

---

## Spark Configuration

```python
spark = SparkSession.builder \
    .appName("CryptoStream-RealTime-Elastic") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .getOrCreate()
```

| Config | Value | Reason |
|---|---|---|
| `shuffle.partitions` | 2 | Matches Kafka partition count; avoids 200-task overhead |
| `maxRatePerPartition` | 100 msg/s | Caps micro-batch size to prevent OOM on burst |

---

## Reliability & Failure Handling

| Failure Scenario | Impact | Recovery |
|---|---|---|
| Spark container restart | Processing pauses | Resumes from checkpointed Kafka offset |
| WebSocket drops | Ingestion pauses | Exponential backoff reconnect (1s → 2s → 4s → 8s) |
| Kafka broker restart | All processing pauses | Named volume preserves log segments |
| Elasticsearch unavailable | Aggregates not indexed | `foreachBatch` retries; Spark query does not crash |
| S3 write failure | Parquet not written | `foreachBatch` retries; checkpoint not advanced until both sinks succeed |
| Bad tick | Single record dropped | Caught at cleaning/quality stage; pipeline continues |

**Recovery window:** Any outage under 6 hours is fully recoverable with zero data loss. Outages beyond 6 hours result in missing candles in Elasticsearch and S3 (Spark resumes from earliest available offset with `failOnDataLoss=false`).

---

## Monitoring

| Surface | URL | Key Signal |
|---|---|---|
| Kafka UI | localhost:8080 | Consumer group lag; alert if lag > ~500 messages |
| Spark UI | localhost:4040 | Micro-batch duration vs trigger interval |
| Kibana | localhost:5601 | Missing candles in `crypto_agg_1m` for > 2–3 minutes |
| `latency_seconds` field | Kibana panels | Average rising above 10s signals network or consumer lag issues |

---

## Known Limitations & Production Gaps

| Gap | Status | Recommended Fix |
|---|---|---|
| Checkpoint stored in `/tmp` | ⚠️ Dev only | Move to S3, HDFS, or persistent Docker volume |
| Kafka idempotent producer not enabled | ⚠️ Missing | Add `enable_idempotence=True` |
| No Dead Letter Queue | ⚠️ Missing | Route bad records to `crypto-ticks-dlq` Kafka topic |
| No backpressure control | ⚠️ Missing | Add `.option("maxOffsetsPerTrigger", "5000")` |
| WebSocket auto-reconnect | ⚠️ Missing | Implement exponential backoff reconnect loop |
| Elasticsearch RBAC disabled | ⚠️ Dev only | Enable `xpack.security` in production |
| No raw tick archive | ⚠️ Design gap | Write raw ticks to S3 bronze layer before transformation |
| No automated alerting | ⚠️ Dev only | Add Prometheus + Grafana + PagerDuty in production |
| Schema evolution not handled | ⚠️ Missing | Adopt Confluent Schema Registry with Avro/Protobuf |

---

## Elasticsearch Index Schema

Fields available in `crypto_agg_1m` / `crypto_agg_5m` / `crypto_agg_15m`:

| Field | Type | Description |
|---|---|---|
| `doc_id` | keyword | Unique ID (`product_id_window_start_ts`); prevents duplicates on retry |
| `product_id` | keyword | Symbol (`BTC-USD` or `ETH-USD`) |
| `avg_price` | float | Average price in the window |
| `min_price` | float | Minimum price in the window |
| `max_price` | float | Maximum price in the window |
| `tick_count` | integer | Number of ticks in the window |
| `total_volume` | float | Sum of `volume_24h` in the window |
| `window_start_ts` | date | ISO 8601 window open timestamp |
| `window_end_ts` | date | ISO 8601 window close timestamp |

---

## Troubleshooting

**Docker using cached code after file changes:**
```bash
docker-compose build --no-cache
docker-compose up -d
```

Verify the running file matches your edits:
```bash
docker-compose exec spark cat /app/consumer.py | Select-Object -Last 30
```

**Spark falling behind Kafka:**
Check consumer group lag in Kafka UI. If lag grows continuously, reduce `maxRatePerPartition` or increase Spark executor resources.

**Missing candles in Kibana:**
1. Check WebSocket producer is running and producing to Kafka
2. Check Spark UI for active streaming queries
3. Verify Elasticsearch is reachable from the Spark container

---

## Project Structure

```
├── producer.py           # WebSocket → Kafka ingestion service
├── consumer.py           # Spark Structured Streaming job
├── docker-compose.yml    # Kafka, Spark, Elasticsearch, Kibana services
├── requirements.txt      # Python dependencies
└── README.md
```

---

## Data Scope & Security

This pipeline processes market data only (price, bid, ask, volume, timestamp). No PII is present at any layer, so GDPR field-level encryption is not applicable to the current data scope.

AWS credentials are injected via environment variables in `docker-compose.yml` and never hardcoded. The IAM user (`spark-crypto-writer`) is scoped to write-only access on the `crypto-data-pk` bucket.
