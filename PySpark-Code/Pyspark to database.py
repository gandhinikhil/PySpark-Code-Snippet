# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Connection to DB").getOrCreate()

# COMMAND ----------

df = spark.read.option("header",True).option("inferSchema",True).csv('/FileStore/tables/Book1.csv')
df.printSchema()


# COMMAND ----------

url = 'jdbc:postgresql://database-1.cc8wcb1vivyd.us-west-2.rds.amazonaws.com:5432/SampleTechno'
table = "states"
driver = "org.postgresql.Driver"
user = "postgres"
password ="GoldMoney$10071999"

# COMMAND ----------

df.write.format("jdbc").option("driver",driver).option("url",url).option("dbtable",table).option("user",user).option("password",password).save()

# COMMAND ----------

readDf= spark.read.format("jdbc").option("driver",driver).option("url",url).option("query","select  id from Book1 " ).option("user",user).option("password",password).load()
display(readDf)

# COMMAND ----------

readDf= spark.read.format("jdbc").option("driver",driver).option("url",url).option("query","select count(*), id from Book1 group by age" ).option("user",user).option("password",password).load()
display(readDf)

# COMMAND ----------

df.write.format("jdbc")\
    .option("driver", driver)\
    .option("dbtable", "states")\
    .option("user", "postgres")\
    .option("password", "Admin123")\
    .option("truncate", "True")\
    .mode("overwrite")\
    .save()



# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/states-1.csv')
rdd.collect()

# COMMAND ----------

rdd1 = rdd.flatMap(lambda x : x.split(","))
rdd1.collect()

# COMMAND ----------

rdd2  = rdd1.map(lambda x : (x,1))
rdd2.collect()

# COMMAND ----------

rdd3  =  rdd2.reduceByKey(lambda x,y:x+y)
rdd3.collect()

# COMMAND ----------

rdd4 =rdd3.sortByKey()

# COMMAND ----------

help(rdd4.collect())


# COMMAND ----------


