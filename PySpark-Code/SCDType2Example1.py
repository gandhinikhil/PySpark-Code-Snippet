# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table scd2Demo1(pk1 int,
# MAGIC                                  pk2 string,
# MAGIC                                  dim1 int,
# MAGIC                                  dim2 int,
# MAGIC                                  dim3 int,
# MAGIC                                  dim4 int,
# MAGIC                                  active_status string,
# MAGIC                                  start_date timestamp,
# MAGIC                                  end_date timestamp)
# MAGIC                                  using delta
# MAGIC                                  location '/FileStore/tables/scd2Demo'
# MAGIC                                  

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo1 values (111,'unit1',200,500,800,400,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo1 values (222,'unit2',900,null,700,100,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo1 values (333,'unit3',300,900,250,650,'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

from delta import *
targetTable = DeltaTable.forPath(spark,'/FileStore/tables/scd2Demo1')

targetDF = targetTable.toDF()
display(targetDF)

# COMMAND ----------


