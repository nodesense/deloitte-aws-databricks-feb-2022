# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE brands(id INT, name STRING) LOCATION "/mnt/aws/brands"

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO brands VALUES (1, 'Apple')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO brands VALUES (2, 'Samsung')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update existing record, Samsung to LG
# MAGIC -- in delta log, it will mark the file that contain Samsung as deleted  part-00000-500582a5-0576-40ab-9888-5bf8c74cfc7f-c000.snappy.parquet
# MAGIC -- add new file, which has LG record
# MAGIC UPDATE brands set name='LG' where id=2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete it mark the file as removed
# MAGIC -- if any remaining in same file which are not deleted, they will be added to new file
# MAGIC DELETE from brands where id = 2

# COMMAND ----------

