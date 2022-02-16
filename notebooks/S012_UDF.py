# Databricks notebook source

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 

print (convertCase("hello python"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title, upper(title) from moviedb.movies_csv LIMIT 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- upper is pre-built, exisiting sql function
# MAGIC -- we have custom requirements, capitalize first letter in word, no function available in spark
# MAGIC -- custom business logic, you apply tax, discount, penalities etc
# MAGIC SELECT upper('hello word')
# MAGIC -- SELECT camelCase('hello word') cause error as camelCase is not built in function
# MAGIC -- UDF - User Defined Function, let user to create a custom function camelCase and register with spark session

# COMMAND ----------

# define a lambda function
# register the lambda as udf in spark session
# use them in sql, dataframe
from  pyspark.sql.functions import udf
from  pyspark.sql.types import StringType 
camelCase = lambda s: convertCase(s)
# create a udf , special wrapper for your function
camelCaseUdf = udf(camelCase)

# register udf to use in spark sql
#                   sql function name,    udf,            return type
spark.udf.register("camelCase",   camelCaseUdf) # Fix me data type error


# COMMAND ----------

# MAGIC %sql
# MAGIC -- ler use use udf function we registerd camelCase
# MAGIC SELECT camelCase('toy story')

# COMMAND ----------

# MAGIC %sql
# MAGIC select   camelCase(title)  from moviedb.movies_csv

# COMMAND ----------

