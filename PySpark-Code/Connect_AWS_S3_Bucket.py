# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/rootkey.csv

# COMMAND ----------

aws_key_df = spark.read.format('csv').option('header','true').option('inferschema','true').load('/FileStore/tables/rootkey.csv')
aws_key_df.columns

# COMMAND ----------

Access_key = aws_key_df.select('Access key ID').take(1)[0]['Access key ID']
Secret_key = aws_key_df.select('Secret access key').take(1)[0]['Secret access key']

# COMMAND ----------

import urllib
Encoded_secret_key = urllib.parse.quote(string = Secret_key, safe = "")

# COMMAND ----------

AWS_S3_Bucket = 'myawsbucket-sample1'
Mount_Name = '/mnt/mount_s3'

source_url = "s3a://%s:%s@%s"%(Access_key,Encoded_secret_key,AWS_S3_Bucket)


# COMMAND ----------

dbutils.fs.mount(source_url,Mount_Name)


# COMMAND ----------

# MAGIC %fs ls '/FileStore/tables'

# COMMAND ----------

# MAGIC %fs ls '/mnt/mount_s3'

# COMMAND ----------

aws_s3_df = spark.read.format('csv').option('header','true').option('inferschema','true').load('/mnt/mount_s3/acc_16.csv')
display(aws_s3_df)


# COMMAND ----------


