from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate sending a few ratings
sample_ratings = [
    {"user_id": 1, "book_id": 101, "rating": 5},
    {"user_id": 2, "book_id": 102, "rating": 3},
    {"user_id": 1, "book_id": 103, "rating": 4}
]

for rating in sample_ratings:
    producer.send("ratings", rating)
    print(f"Sent: {rating}")
    time.sleep(1)

producer.flush()
