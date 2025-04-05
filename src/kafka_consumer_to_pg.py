from kafka import KafkaConsumer
import psycopg2
import json

# Connect to Kafka
consumer = KafkaConsumer(
    'ratings',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="bookrec",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS kafka_ratings (
    user_id INT,
    book_id INT,
    rating FLOAT
)
""")
conn.commit()

print("[✓] Listening for messages...")

# Read from Kafka and insert into PostgreSQL
for msg in consumer:
    data = msg.value
    print(f"Inserting: {data}")
    cursor.execute(
        "INSERT INTO kafka_ratings (user_id, book_id, rating) VALUES (%s, %s, %s)",
        (data['user_id'], data['book_id'], data['rating'])
    )
    conn.commit()
