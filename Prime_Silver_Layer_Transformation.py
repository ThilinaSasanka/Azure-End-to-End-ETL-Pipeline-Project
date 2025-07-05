# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlsprimeworc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsprimeworc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsprimeworc.dfs.core.windows.net", "04c76a0f-1365-4536-a041-09e7024f8afb")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsprimeworc.dfs.core.windows.net","ohG8Q~ZAHsEHbCJsDMm4YwChk..~S516m2f71cfp")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsprimeworc.dfs.core.windows.net", "https://login.microsoftonline.com/b8d181b7-58fc-42cc-8853-9c4880ddfada/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://bronze@adlsprimeworc.dfs.core.windows.net/')  # Check and update Azure credentials if needed

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Understanding**
# MAGIC

# COMMAND ----------

df_silver = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adlsprimeworc.dfs.core.windows.net/amazon_prime_titles.csv")
df_silver.display()

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Cleaning**

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = df_silver.na.fill({"rating": "Unrated", "country":"Unknown"})
df_silver.display()

# COMMAND ----------

df_silver = df_silver.dropDuplicates()
df_silver.display()

# COMMAND ----------

df_silver = df_silver.na.fill({"rating": "Unrated", "country":"Unknown", "description":"Unknown", "date_added":"01/01/2025", "release_year":"2020", "duration":"Unknown", "cast":"Unknown", "listed_in":"Unknown"})
df_silver.display()

# COMMAND ----------

df_silver = df_silver.dropna('any')
df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumnRenamed('title', 'Content_title')
df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn('is_country', when(col('country') == 'Unknown',0).otherwise(1))
df_silver.display()

# COMMAND ----------

df_silver.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://silver@adlsprimeworc.dfs.core.windows.net/amazon_prime_titles_silver.csv")\
    .save()