# Databricks notebook source
# spark session, spark - entry point for spark sql, dataframe
# DataFrame is for structured data, Spark SQL Structured Data
# Structured Data - schema has columns, column name, data type
# NO Primary key, no unique key, no foreign key, no DB indexes
# all  your data in dataframe loaded into RDD partition only
# any dataframe/sql you execute, finally there is code generated for RDD, only RDD is what exexuted
# SQL, DF are basically called API
spark

# COMMAND ----------

# id, amount
orders = [
  (1, 500.0),
  (2, 450.0)
]

# create dataframe from hardcoded data
# datatype shall be infered by spark automatically
df = spark.createDataFrame(orders, schema=['id', 'amount'])
df.printSchema() # print schema in ascii format
df.show() # top 20  records in ascii table

# COMMAND ----------

# every dataframe has rdd , partitions
# df is collection of Row
print(df.rdd.collect())
print(df.rdd.glom().collect())
print(df.rdd.getNumPartitions())


# COMMAND ----------

# both rdd, df are immutable, once created, we cannot modify, delete data
# dataframe api
df2 = df.select("amount") # return a new dataframe that consist only amount column
df2.printSchema()
df2.show()

# COMMAND ----------

df3 = df.filter(df["amount"] >= 500)
df3.printSchema()
df3.show()
# print logical, analysed logical plan, physical plan
df3.explain(extended = True)

# COMMAND ----------

# spark SQL in python
# where, filter are both same/alias name

df5 = df.where (" amount >= 500   ") # SQL statement inside python
df5.show()

# COMMAND ----------

df6 = df.selectExpr ("amount", " amount * 1.2 ")
df6.printSchema()
df6.show()

# COMMAND ----------

# full sql
# you can create a temp table/view out of your data frame
# temp table shall be there unless you remove or end of your spark session /program
df.createOrReplaceTempView("orders") # spark will create temp table 
# temp table nothing but dataframe, it will just registed as table in sql catalog

# COMMAND ----------

# Spark SQL, returns dataframe
df6 = spark.sql("select * from orders")
df6.printSchema()
df6.show()

# COMMAND ----------

# databricks only, display function, accept dataframe, show graphical view, html view

df6 = spark.sql("select * from orders")

display(df6)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sql comment
# MAGIC -- sql queries 
# MAGIC -- databricks or notebook specific features jupyter etc
# MAGIC -- known as magic functions
# MAGIC -- any code written shall be taken and executed inside spark.sql, output dataframe is displayed using display func
# MAGIC 
# MAGIC select * from orders

# COMMAND ----------

