import json
import websocket
import threading
import time
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto_ticks")
WS_URL = "wss://ws-feed.exchange.coinbase.com"
TIME_LIMIT = int(os.getenv("PRODUCER_RUNTIME_SECONDS", "300"))  # 5 minutes default

# =============================================================================
# IDEMPOTENT KAFKA PRODUCER
# =============================================================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    
    # ========== IDEMPOTENCE CONFIGURATION ==========
    enable_idempotence=True,  # Prevents duplicates per partition
    
    # Automatically set by enable_idempotence=True:
    # - acks='all'              (wait for all in-sync replicas)
    # - retries=2147483647      (retry indefinitely)
    # - max_in_flight_requests_per_connection=5
    
    # ========== PERFORMANCE TUNING ==========
    compression_type='snappy',      # Compress messages
    linger_ms=10,                   # Batch messages for 10ms
    batch_size=16384,               # Batch up to 16KB
    request_timeout_ms=30000,       # 30 second timeout
    max_block_ms=60000,             # Block for max 60s if buffer full
)

ws = None
message_count = 0

# =============================================================================
# WEBSOCKET CALLBACKS
# =============================================================================
def on_message(ws, message):
    """Handle incoming WebSocket messages from Coinbase"""
    global message_count
    
    try:
        data = json.loads(message)
        msg_type = data.get("type")

        if msg_type == "subscriptions":
            print("📡 Subscription confirmed")
            print(f"   Subscribed to: {data.get('channels', [])}")
            
        elif msg_type == "ticker":
            # Extract relevant fields
            event = {
                "product_id": data.get("product_id"),
                "price": float(data.get("price", 0)),
                "bid": float(data.get("best_bid", 0)),
                "ask": float(data.get("best_ask", 0)),
                "volume_24h": float(data.get("volume_24h", 0)),
                "event_time": data.get("time")
            }
            
            # Send to Kafka with key (automatic partitioning by product_id)
            try:
                future = producer.send(
                    topic=TOPIC,
                    key=event["product_id"],  # BTC-USD or ETH-USD
                    value=event
                )
                
                # Optional: Wait for confirmation (uncomment for debugging)
                # record_metadata = future.get(timeout=10)
                
                message_count += 1
                if message_count % 10 == 0:  # Print every 10th message
                    print(f"✅ Sent {message_count} messages | Latest: {event['product_id']} @ ${event['price']:,.2f}")
                
            except KafkaError as e:
                print(f"❌ Kafka Error: {e}")
                
        elif msg_type == "error":
            print(f"❌ Error from Coinbase: {data}")
            
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error in on_message: {e}")


def on_open(ws):
    """Handle WebSocket connection opened"""
    print("🔗 WebSocket connected to Coinbase")
    
    subscribe_msg = {
        "type": "subscribe",
        "channels": [
            {
                "name": "ticker",
                "product_ids": ["BTC-USD", "ETH-USD"]
            }
        ]
    }
    
    ws.send(json.dumps(subscribe_msg))
    print("📡 Subscription request sent for BTC-USD and ETH-USD")


def on_error(ws, error):
    """Handle WebSocket errors"""
    print(f"❌ WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket connection closed"""
    print("🔌 WebSocket closed")
    print(f"   Status: {close_status_code}, Message: {close_msg}")
    
    # Flush any remaining messages
    producer.flush()
    print(f"📊 Total messages sent: {message_count}")


# =============================================================================
# WEBSOCKET THREAD
# =============================================================================
def start_websocket():
    """Run WebSocket connection in separate thread"""
    global ws
    
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Run forever with ping to keep connection alive
    ws.run_forever(
        ping_interval=20,  # Send ping every 20 seconds
        ping_timeout=10    # Timeout if no pong in 10 seconds
    )


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 COINBASE CRYPTO PRODUCER STARTING")
    print("=" * 60)
    print(f"📍 Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📍 Topic: {TOPIC}")
    print(f"📍 Runtime: {TIME_LIMIT} seconds")
    print(f"⚙️  Idempotence: ENABLED")
    print(f"⚙️  Compression: snappy")
    print("=" * 60)
    
    # Start WebSocket in background thread
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()
    
    # Main thread: Monitor runtime
    start_time = time.time()
    
    try:
        while True:
            time.sleep(1)
            elapsed = time.time() - start_time
            
            if elapsed >= TIME_LIMIT:
                print(f"\n⏰ Time limit reached ({TIME_LIMIT}s)")
                break
                
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user (Ctrl+C)")
    
    # Cleanup
    print("🛑 Stopping producer...")
    if ws:
        ws.close()
    
    ws_thread.join(timeout=5)
    producer.close()
    
    print("=" * 60)
    print("✅ PRODUCER STOPPED SUCCESSFULLY")
    print(f"📊 Total Messages Sent: {message_count}")
    print("=" * 60)