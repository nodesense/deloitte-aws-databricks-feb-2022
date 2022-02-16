# Databricks notebook source
data = range (1, 20)
# spark functions liek parallize, textFile etc has default parameters, set via configuration
# uses defaultParallelism for number of parititions, ie 8
rdd = sc.parallelize(data)

# COMMAND ----------

print(sc.defaultMinPartitions) # 2 parttions
print(sc.defaultParallelism) # 8 parallell operations, sc.parallize use defaultParallelism config

# COMMAND ----------

# get number of paritions, lazy, no partitions created.. this is just plan
print("partitions", rdd.getNumPartitions())

# COMMAND ----------

# collect, will collect data from all partitions into driver
# action method
# should not be used with large data
result = rdd.collect()
print(result)

# COMMAND ----------

# take, action, collect data from first partion on wards until request met
data = rdd.take(3) # 1 result, likely to read from first partition
# if the first partition doesn't have enough records, then it read from 1st, then 2nd then 3rd ...
print(data)

# COMMAND ----------

# how to collect data from all paritions, by partition wise
# action method, return results as list of list , partition wise

data = rdd.glom().collect() 

print(data)

# COMMAND ----------

