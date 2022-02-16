# Databricks notebook source
# /FileStore/tables/feb14-02-2022/words.txt
textFile = sc.textFile("/FileStore/tables/feb14-02-2022/words.txt")

textFile.getNumPartitions()

# COMMAND ----------

# numer of lines, action
textFile.count()

# COMMAND ----------

textFile.collect()

# COMMAND ----------

# \ line continuation WITHOUT SPACE AFTER, then it will consder next line part of same line

nonEmptyLines = textFile.map(lambda line: line.strip().lower())\
                        .filter(lambda line: line != '')

nonEmptyLines.collect()

# COMMAND ----------


# split the line into list of words
wordsList = nonEmptyLines.map(lambda line: line.split(" "))

wordsList.collect()

# COMMAND ----------

# flatMap
# flatten the list/array into elements, convert list into  elements

words = wordsList.flatMap(lambda arr: arr)
words.collect()

# COMMAND ----------

# word pair (scala, 1) (python, 1) (scala, 1), tuple
wordPairs = words.map (lambda word: (word, 1))
wordPairs.collect()

# COMMAND ----------

# word count, reduceByKey, what is key here, we got tuple (word, 1) , word is the key, 1 is value
"""
('scala', 1) <- the key spark appear first time, it moves directly into table, func is not called
('apache', 1) <- apache first time, moves to table , func not called
('scala', 1), <- scala second time, now it calls the function acc(1), value(1) : acc + value (1 + 1) = 2, then 2 is updated in table
('python', 1),<- python first time, moves to table , func not called
('spark', 1),<- spark first time, moves to table , func not called
('spark', 1),<- spark second time, now it calls the function acc(1), value(1) : acc + value (1 + 1) = 2, then 2 is updated in table
('python', 1),<- python second time, now it calls the function acc(1), value(1) : acc + value (1 + 1) = 2, then 2 is updated in table
('kafka', 1) <- kafka first time, moves to table , func not called
('scala', 1), <- scala 3rd time, now it calls the function acc(2), value(1) : acc + value (2 + 1) = 3, then 3 is updated in table
('aws', 1)

--
Virtually there is table inside reduceByKey

word     accumulator(acc)
scala       3
apache      1
python      2
spark       2
kafka       1


"""
wordCount = wordPairs.reduceByKey(lambda acc, value: acc + value)
                   
wordCount.collect()

# COMMAND ----------

wordCount2 = sc.textFile("/FileStore/tables/feb14-02-2022/words.txt")\
                .flatMap(lambda line: line.strip().lower().split(" "))\
                .filter(lambda line: line != '')\
                .map (lambda word: (word, 1))\
                .reduceByKey(lambda acc, value: acc + value)

wordCount2.collect()

# COMMAND ----------

# line continuation without \, by using ( )

wordCount3 = (
              sc.textFile("/FileStore/tables/feb14-02-2022/words.txt")
                .flatMap(lambda line: line.strip().lower().split(" "))
                .filter(lambda line: line != '')
                .map (lambda word: (word, 1))
                .reduceByKey(lambda acc, value: acc + value)
             )

wordCount3.collect()

# COMMAND ----------

