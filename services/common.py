"""Shared Kafka settings and small helpers used by every service."""
import os

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094")

ORDERS_TOPIC = "orders"
STATUS_TOPIC = "order-status"
DLQ_TOPIC = "orders.dlq"
PARTITIONS = 3
REPLICATION = 3   # one copy per broker; create_topics lowers it if fewer brokers are up

# Where orders come from. Each is a separate producer client so the graph shows three.
PRODUCERS = {
    "web": "the web shop (Send buttons on the Watch tab)",
    "mobile-app": "mobile app (simulated)",
    "partner-api": "Orders from a partner marketplace (simulated)",
}

# Shop catalogue. Small on purpose so the same customers keep showing up and
# key -> partition stickiness is easy to watch in the inspector.
CUSTOMERS = ["lara", "tim", "maya", "noah", "ava", "leo", "zoe", "eli"]
MENU = {
    "frozen yogurt": 4.5,
    "mango sorbet": 5.0,
    "espresso": 3.0,
    "croissant": 3.5,
    "bagel": 2.5,
    "smoothie": 6.0,
}

# Customer-facing grouping only. Kafka never sees restaurants; the order is the event.
RESTAURANTS = [
    {"name": "Frosty Scoop", "tagline": "Frozen desserts, 15 min", "emoji": "🍧", "items": ["frozen yogurt", "mango sorbet", "smoothie"]},
    {"name": "Daily Grind", "tagline": "Coffee and bakery, 10 min", "emoji": "☕", "items": ["espresso", "croissant", "bagel"]},
]
