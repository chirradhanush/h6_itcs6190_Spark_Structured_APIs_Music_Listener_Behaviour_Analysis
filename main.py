# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, hour, row_number
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# ------------------ Load datasets ------------------
logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Join logs with song metadata
logs_with_meta = logs.join(songs, on="song_id", how="inner")

# ------------------ Task 1: User Favorite Genres ------------------
genre_counts = logs_with_meta.groupBy("user_id", "genre").agg(count("*").alias("play_count"))
window = Window.partitionBy("user_id").orderBy(col("play_count").desc())
user_fav_genres = genre_counts.withColumn("rank", 
    row_number().over(window)).filter(col("rank") == 1).drop("rank")

user_fav_genres.write.mode("overwrite").csv("output/user_favorite_genres")

# ------------------ Task 2: Average Listen Time ------------------
avg_listen_time = logs.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration_sec"))
avg_listen_time.write.mode("overwrite").csv("output/avg_listen_time_per_song")

# ------------------ Task 3: Genre Loyalty Scores ------------------
# Count total plays per user
user_total = logs_with_meta.groupBy("user_id").agg(count("*").alias("total_plays"))
# Count plays per genre per user
user_genre_counts = logs_with_meta.groupBy("user_id", "genre").agg(count("*").alias("genre_plays"))
# Find top genre plays per user
user_top_genre = user_genre_counts.groupBy("user_id")\
    .agg(spark_max("genre_plays").alias("max_genre_plays"))
# Compute loyalty score
loyalty = user_top_genre.join(user_total, "user_id")\
    .withColumn("loyalty_score", col("max_genre_plays") / col("total_plays"))\
    .filter(col("loyalty_score") > 0.8)

loyalty.write.mode("overwrite").csv("output/genre_loyalty_scores")

# ------------------ Task 4: Identify Night Owl Users ------------------
logs = logs.withColumn("hour", hour("timestamp"))
night_owl_users = logs.filter((col("hour") >= 0) & (col("hour") < 5))\
    .select("user_id").distinct()

night_owl_users.write.mode("overwrite").csv("output/night_owl_users")
