# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlsprimeworc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsprimeworc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsprimeworc.dfs.core.windows.net", "04c76a0f-1365-4536-a041-09e7024f8afb")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsprimeworc.dfs.core.windows.net","ohG8Q~ZAHsEHbCJsDMm4YwChk..~S516m2f71cfp")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsprimeworc.dfs.core.windows.net", "https://login.microsoftonline.com/b8d181b7-58fc-42cc-8853-9c4880ddfada/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://silver@adlsprimeworc.dfs.core.windows.net/')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_gold = spark.read.format("parquet")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://silver@adlsprimeworc.dfs.core.windows.net/amazon_prime_titles_silver.csv")
df_gold.display()

# COMMAND ----------

df_gold = df_gold.withColumn("date_added", to_date(df_gold["date_added"], "mm/dd/yyyy"))
df_gold = df_gold.withColumn("year_added", year(df_gold["date_added"]))
df_gold.display()

# COMMAND ----------

df_gold = df_gold.withColumn("category_1",split(df_gold["listed_in"], ",")[0])
df_gold = df_gold.withColumn("category_2",split(df_gold["listed_in"], ",")[1])

df_gold.display()

# COMMAND ----------

df_gold = df_gold.withColumn("category_2", when(df_gold["category_2"].isNull(), "Unknown").otherwise(df_gold["category_2"]))
df_gold.display()

# COMMAND ----------

df_gold.write.format("delta")\
    .mode("append")\
    .option("path", "abfss://gold@adlsprimeworc.dfs.core.windows.net/amazon_prime_titles_gold.csv")\
    .save()