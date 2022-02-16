# Databricks notebook source
# repartition - to distribute the data across more parititons, increase more number of partitions
# coalesce - to reduce number of partitions, write to csv/generate report etc

data = range(0, 20)
rdd = sc.parallelize(data, 4) # explicitly mentioning partitions

# COMMAND ----------

print(rdd.getNumPartitions())
print (rdd.glom().collect())

# COMMAND ----------

# create more partitions
rdd2 = rdd.repartition(8)
print(rdd2.getNumPartitions())
print (rdd2.glom().collect())

# COMMAND ----------

# to reduce partitions
rdd3 = rdd2.coalesce(1)

print(rdd3.getNumPartitions())
print (rdd3.glom().collect())

# COMMAND ----------

# repartition - shuffle the data across system, distribute data across partitions
# coalsece - try its best to reduce the shuffling data across system via network, network I/O

# COMMAND ----------

# numbers upto 10 should be in partiton 0, 10 and above, should be in partition 1
# semantic partition
rdd4 = rdd\
          .map (lambda n : float(n))\
          .partitionBy(2, lambda n: int(1))

print(rdd4.getNumPartitions())
print(rdd4.collect())


# COMMAND ----------

rdd = sc.parallelize([10,20,30,40,50,10,20,35]).map(lambda x : (float(x)/10, float(x)/10))
elements = rdd.partitionBy(2,lambda x: int(x > 3)).map(lambda x: x[0]).glom().collect()
elements

# COMMAND ----------

# tuple - pair of elements, represented using ()
# pairedRDD, first element in tuple considered to be key
orders = [
    ('IN', 100.0),
    ('USA', 200.0),
    ('CA', 300.0),
    ('UK', 400.0),
    ('IN', 500.0),
    ('USA', 700.0),
    ('IN', 600.0)
]

orderRdd = sc.parallelize(orders)

# COMMAND ----------

orderRdd.glom().collect()

# COMMAND ----------

# t[0] is country code, based on country code, partition shall be allocated
orderRddByCountry = orderRdd.partitionBy(4, lambda t: hash(t[0]))

orderRddByCountry.glom().collect()

# COMMAND ----------

