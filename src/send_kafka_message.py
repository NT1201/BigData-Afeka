from kafka import KafkaProducer
import json

# 🛰️ Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 🧪 New test users and books
messages = [
   {"user_id": 1, "book_id": 11},
    {"user_id": 2, "book_id": 12},
    {"user_id": 3, "book_id": 13},
]

# 🚀 Send to Kafka
for msg in messages:
    print(f"📤 Sending: {msg}")
    producer.send("ratings", msg)

producer.flush()
print("✅ All new test messages sent to Kafka!")
