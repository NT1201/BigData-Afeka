# 📚 Book Recommendation System

This project implements a **real-time hybrid book recommendation system** using:

- 🧠 ALS Collaborative Filtering (PySpark)
- 🧲 Content-based filtering (TF-IDF + Cosine Similarity)
- 💡 Hybrid scoring + explanation generation
- 📬 Kafka for real-time input
- ☁️ MinIO for file storage
- 🗃️ PostgreSQL for predictions
- 📊 Streamlit dashboard for visualization

---

## 📁 Project Structure

| File | Purpose |
|------|---------|
| `pyspark_recommender.py` | Trains ALS model, builds content vectors, saves factors |
| `pyspark_consumer_to_minio.py` | Consumes Kafka messages, predicts, saves to MinIO and PostgreSQL |
| `kafka_producer.py` | Sends user-book messages to Kafka |
| `streamlit_app.py` | Shows live dashboard with explanations and graphs |
| `requirements.txt` | Lists dependencies |
| `log4j2.properties` | Reduces Spark log noise |

---

## 🔗 Pipeline Overview

```mermaid
flowchart TD
    A[Kafka Producer (ratings)] --> B[Kafka Topic: ratings]
    B --> C[pyspark_consumer_to_minio.py]
    C --> D[MinIO (JSON files)]
    C --> E[PostgreSQL (predictions table)]
    F[pyspark_recommender.py] --> C
    F --> G[Parquet: user/item factors]
    F --> H[TF-IDF metadata vectors]
