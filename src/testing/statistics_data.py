import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load datasets
books_file = "C:\\Users\\noamy\\OneDrive\\שולחן העבודה\\מערכות תבוניות\\שנה ב\\Bigdata\\final\\books.csv\\books.csv"  # Ensure the correct path
ratings_file = "C:\\Users\\noamy\\OneDrive\\שולחן העבודה\\מערכות תבוניות\\שנה ב\\Bigdata\\final\\ratings.csv\\ratings.csv"

df_books = pd.read_csv(books_file)
df_ratings = pd.read_csv(ratings_file)

# # Merge datasets on 'book_id'
# df = pd.merge(df_ratings, df_books, on='book_id')

# # Set visualization style
# sns.set_style("whitegrid")
# plt.rcParams.update({'font.size': 12})  

# # 🎯 1. Rating Distribution (Same as before)
# plt.figure(figsize=(8, 5))
# ax = sns.countplot(x=df['rating'], palette="Blues", edgecolor="black")
# plt.xlabel("Rating (1-5)")
# plt.ylabel("Count of Ratings")
# plt.title("Distribution of Ratings in Goodbooks-10k")

# # Add percentage labels
# total = len(df)
# for p in ax.patches:
#     percentage = f'{100 * p.get_height() / total:.1f}%'
#     ax.annotate(percentage, (p.get_x() + p.get_width() / 2, p.get_height()), 
#                 ha='center', va='bottom', fontsize=10)

# plt.show()

# # 🎯 2. Number of Ratings and Their Distribution
# plt.figure(figsize=(8, 5))
# sns.histplot(df_ratings['rating'], bins=5, discrete=True, kde=False, color="purple", edgecolor="black")
# plt.xlabel("Rating (1-5)")
# plt.ylabel("Number of Ratings")
# plt.title("Total Number of Ratings and Their Distribution")
# plt.grid(axis="y", linestyle="--", alpha=0.7)
# plt.show()

# # 🎯 3. Total Number of Books
# plt.figure(figsize=(6, 6))
# total_books = df_books.shape[0]

# plt.bar(["Books"], [total_books], color="lightblue", edgecolor="black")
# plt.ylabel("Number of Books")
# plt.title("Total Number of Books in the Dataset")
# plt.show()

#Merge datasets on 'book_id'
df = pd.merge(df_ratings, df_books, on='book_id')

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams.update({'font.size': 12})  

# 🎯 1. Top 10 Most Rated Books
most_rated_books = df.groupby("title")["rating"].count().reset_index().nlargest(10, "rating")

plt.figure(figsize=(10, 6))
sns.barplot(y=most_rated_books['title'], x=most_rated_books['rating'], palette="magma")
plt.xlabel("Number of Ratings")
plt.ylabel("Book Title")
plt.title("Top 10 Most Rated Books")
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.show()

# 🎯 2. Average Ratings Per Book
average_ratings = df.groupby("title")["rating"].mean().reset_index()

plt.figure(figsize=(8, 5))
sns.histplot(average_ratings["rating"], bins=20, kde=True, color="teal")
plt.xlabel("Average Rating Per Book")
plt.ylabel("Number of Books")
plt.title("Distribution of Average Ratings Per Book")
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()

# 🎯 3. User Activity Distribution (How Many Ratings Per User)
user_activity = df.groupby("user_id")["rating"].count()

plt.figure(figsize=(8, 5))
sns.histplot(user_activity, bins=50, kde=True, color="red")
plt.xlabel("Number of Ratings Per User")
plt.ylabel("Number of Users")
plt.title("Distribution of User Activity (Ratings Per User)")
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()