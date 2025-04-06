import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# 📦 PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="bookrec",
    user="postgres",
    password="1234"
)

# 📥 Load predictions
query = """
SELECT user_id, book_id, prediction, explanation, timestamp
FROM predictions
ORDER BY timestamp DESC
LIMIT 200
"""
df = pd.read_sql(query, conn)

st.title("📚 Book Recommendation Dashboard")
st.subheader("🧠 Latest Predictions with Explanations")

# 📋 Data Table
st.dataframe(df)

# 📈 Multi-user prediction chart
st.markdown("### 📊 Predictions Across Users")

# Take recent 100, sort by prediction
top_preds = df.sort_values(by="prediction", ascending=False).head(100)

fig = px.bar(
    top_preds,
    x="book_id",
    y="prediction",
    color="user_id",
    barmode="group",
    title="Top 100 Predictions Grouped by User",
    labels={"book_id": "Book ID", "prediction": "Prediction Score", "user_id": "User"}
)
st.plotly_chart(fig)
