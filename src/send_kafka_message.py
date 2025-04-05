from kafka import KafkaProducer
import json

# 🛰️ Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 🧪 New test users and books
messages = [
    {"user_id": 10010, "book_id": 5},
    {"user_id": 10010, "book_id": 6},
    {"user_id": 10011, "book_id": 7},
    {"user_id": 10011, "book_id": 8},
    {"user_id": 10012, "book_id": 9},
    {"user_id": 10013, "book_id": 10}
]

# 🚀 Send to Kafka
for msg in messages:
    print(f"📤 Sending: {msg}")
    producer.send("ratings", msg)

producer.flush()
print("✅ All new test messages sent to Kafka!")
