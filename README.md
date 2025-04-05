# 📚 Book Recommendation System

---

## ✅ 1. Project Structure

| Filename                      | Purpose                                                                 |
|------------------------------|-------------------------------------------------------------------------|
| `pyspark_recommender.py`     | Trains ALS model, evaluates RMSE, saves user/item factors and predictions |
| `pyspark_consumer_to_minio.py` | Loads saved ALS model, listens to Kafka for rating events, predicts and saves to MinIO + PostgreSQL |
| `kafka_producer.py`          | Sends test rating messages to Kafka topic `ratings`                    |
| `send_kafka_message.py`      | One-off Kafka message sender                                           |
| `kafka_consumer_to_pg.py`    | (Optional) Stores raw Kafka rating messages to PostgreSQL              |
| `MinIO2pg.py`                | (Optional) Pulls prediction JSONs from MinIO and inserts into PostgreSQL |
| `pyspark_test.py`            | Quick Spark/PostgreSQL test utility                                   |
| `log4j2.properties`          | Logging settings for Spark (reduces log noise)                         |

---

## ✅ 2. Flow Diagram

```mermaid
flowchart TD
    A[Kafka Producer (ratings)] --> B[Kafka Topic: ratings]
    B --> C[pyspark_consumer_to_minio.py]
    C --> D[MinIO (JSON files)]
    C --> E[PostgreSQL (predictions table)]
    F[pyspark_recommender.py] --> C
    F --> G[PostgreSQL (ratings table)]
