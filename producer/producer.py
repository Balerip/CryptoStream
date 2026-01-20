import json
import websocket
import threading
import time
from kafka import KafkaProducer

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",  # Docker network
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

TOPIC = "crypto_ticks"
WS_URL = "wss://ws-feed.exchange.coinbase.com"
TIME_LIMIT = 300  # 5 minutes (increase from 30s)
ws = None


def on_message(ws, message):
    data = json.loads(message)
    msg_type = data.get("type")

    if msg_type == "subscriptions":
        print("📡 Subscription confirmed")
    elif msg_type == "ticker":
        event = {
            "product_id": data.get("product_id"),
            "price": float(data.get("price", 0)),
            "bid": float(data.get("best_bid", 0)),
            "ask": float(data.get("best_ask", 0)),
            "volume_24h": float(data.get("volume_24h", 0)),
            "event_time": data.get("time")
        }
        producer.send(topic=TOPIC, key=event["product_id"], value=event)
        print(f"✅ Sent to Kafka: {event['product_id']} @ {event['price']}")
    elif msg_type == "error":
        print("❌ Error from Coinbase:", data)


def on_open(ws):
    print("🔗 WebSocket connected")
    subscribe_msg = {
        "type": "subscribe",
        "channels": [
            {"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD"]}
        ]
    }
    ws.send(json.dumps(subscribe_msg))
    print("📡 Subscribe payload sent")


def on_error(ws, error):
    print("❌ WebSocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print("🔌 WebSocket closed")


def start_ws():
    global ws
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)


if __name__ == "__main__":
    print("🚀 Starting Coinbase producer...")
    t = threading.Thread(target=start_ws)
    t.start()

    start_time = time.time()
    try:
        while True:
            time.sleep(1)
            if time.time() - start_time > TIME_LIMIT:
                print(f"⏰ Time limit reached ({TIME_LIMIT}s)")
                ws.close()
                break
    except KeyboardInterrupt:
        print("Stopping via Ctrl+C...")
        ws.close()

    t.join()
    print("✅ Producer stopped")