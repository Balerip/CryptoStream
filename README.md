# Real-Time Crypto Streaming Pipeline

Real-time BTC/ETH market data pipeline: Coinbase WebSocket → Kafka → Spark Structured Streaming → Elasticsearch → Kibana.

---

## Architecture

```
Coinbase WebSocket
       │  BTC-USD / ETH-USD ticks
       ▼
  producer.py  ──────────────────→  Kafka (crypto_ticks, 2 partitions)
                                           │
                                           ▼
                                  Spark Structured Streaming
                                  Parse → Clean → Dedupe → Enrich → Aggregate
                                  1m / 5m / 15m OHLC candles
                                           │
                                           ▼
                                    Elasticsearch
                                  crypto_agg_1m / 5m / 15m
                                           │
                                           ▼
                                        Kibana
                                     localhost:5601
```

---
Stack
Layer	Technology
Source	Coinbase WebSocket API
Queue	Apache Kafka 7.5.0
Processing	Spark Structured Streaming 3.4.2
Storage	Elasticsearch 8.11.0
Visualisation	Kibana 8.11.0
Infra	Docker Compose
Quick Start
# Start all services
docker-compose up -d --build

# Check logs
docker-compose logs -f producer   # ticks flowing in
docker-compose logs -f spark      # micro-batches processing


## Monitoring
UI	URL
Kafka UI	http://localhost:8080
Spark UI	http://localhost:4040
Kibana	http://localhost:5601

### Known Gaps
No Dead Letter Queue for malformed records
No automated alerting
Single Kafka broker — no replication
