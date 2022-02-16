# Databricks notebook source
spark.sql("SHOW DATABASES").show()

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS moviedb;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS moviedb.tickets(id INT, seats INT, amount INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in moviedb

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO moviedb.tickets VALUES(1, 2, 200)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO moviedb.tickets VALUES (2, 1, 100),  (3, 3, 300)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.tickets

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SHOW TABLES IN moviedb

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from moviedb.movies_csv

# COMMAND ----------

df = spark.sql("SELECT * FROM moviedb.movies_csv")
df.printSchema()
df.show(5)

# COMMAND ----------

# get dataframe without queries
df = spark.table('moviedb.movies_csv')
df2 = spark.table('moviedb.tickets')

df.printSchema()
df2.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, LongType,StringType, IntegerType, DoubleType

ratingSchema = StructType()\
         .add("userId", IntegerType(), True)\
         .add("movieId", IntegerType(), True)\
         .add("rating", DoubleType(), True)\
         .add("timestamp", StringType(), True)


ratingDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(ratingSchema)\
          .load("/FileStore/tables/feb14-02-2022/ratings.csv")

# COMMAND ----------

# spark database, movies located there
# ratingDf in memory, part of spark session
# we want to join, work with sql between movies_csv and ratingDf

# temporary table/view out of dataframe
ratingDf.createOrReplaceTempView("ratings")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW TABLES in default

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW popular_movies AS
# MAGIC SELECT movieId, avg(rating) as avg_rating, 
# MAGIC                 count(userId) as total_ratings FROM ratings 
# MAGIC GROUP BY movieId
# MAGIC HAVING total_ratings >= 100
# MAGIC ORDER BY avg_rating DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM popular_movies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a permanent table in moviedb, called most_popular_movies
# MAGIC CREATE OR REPLACE TABLE moviedb.most_popular_movies AS
# MAGIC SELECT pm.movieId, title, avg_rating, total_ratings from popular_movies pm
# MAGIC INNER JOIN moviedb.movies_csv m ON pm.movieId=m.movieId

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from moviedb.most_popular_movies

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT upper(title), avg_rating from moviedb.most_popular_movies

# COMMAND ----------

