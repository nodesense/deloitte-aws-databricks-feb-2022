# Databricks notebook source
# load data into RDD [hardcoded data from driver]
# python list
# spark driver 
data = [1,2,3,4,5,6,7,8,9,10]
# spark context - sc
# load data into RDD
# Spark is lazy evaluation, this code will not load data into memory in executor until there is action applied
# sc.<TAB><TAB><TAB> - intellisense
# creation of rdd from hard coded data
rdd = sc.parallelize (data)
# to run, Shift + Enter 

# COMMAND ----------

# transformation, let say, you want multiple each number factor of 10
# python - lambda , 1 line annonymouse function
# for each number (n) 1,2... 10, lambda is called, output of 10, 20, 30..100 returned 
# RDD lineage, rdd is parent rdd, rdd2 is child rdd
rdd2 = rdd.map (lambda n: n * 10) # task in spark, applied on top of RDD

# COMMAND ----------

# actions, to get the results
# RDD [tree] is converted to DAG -  Directed Acylic Graph [graph]
# for each action, there will be job created
# each job shall have DAGs, DAGs shall be scheduled
# DAG shall be converted into Stages
# Each stage shall be converted into Tasks, based on number of partitions
# Each tasks shall be executed in exeuctors
# the final output may be returned to driver/written to Data Lake like s3, hdfs etc/DB/JDBC

s = rdd2.sum()
print(s)

# 1 job created, job id 0 
#   job 0, has 1 stage , stage id 0 [stage 0]
#      the stage 0, has 8 tasks, should match with partitions

# Spark Driver
   # Job, Stages, DAG, RDD, Task Scheduler, Task Queue
# Executor
      # partitions, tasks

# COMMAND ----------

min = rdd2.min() # action, this create job
print(min)

max = rdd2.max()
print(max)

# COMMAND ----------

