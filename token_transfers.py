# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df = spark.read.parquet("s3://rwa-xyz-recruiting-data/eth_logs_decoded/")

# COMMAND ----------

display(df)

# COMMAND ----------

# excluding 2023-01-01 as it is part of last week of 2022
df = df.filter(col("BLOCK_TIMESTAMP") > "2023-01-01") 
df_fields = df.select('ADDRESS', 'BLOCK_TIMESTAMP', 'DATA', 'TOPIC0', 'TOPIC1', 'TOPIC2')

# COMMAND ----------

# include only Transfer, exclude Mint, Burn, and self-transfer
df_transfers = df_fields.filter((df_fields.TOPIC0 == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") 
                                & (df_fields.TOPIC1 != "0x0000000000000000000000000000000000000000000000000000000000000000") 
                                & (df_fields.TOPIC2 != "0x0000000000000000000000000000000000000000000000000000000000000000") & 
                                (df_fields.TOPIC1 != df_fields.TOPIC2))

display(df_transfers)

# COMMAND ----------

# df_transfers.count()

# COMMAND ----------

tokens = "dbfs:/user/hive/warehouse/tokens"
erc20s = spark.read.format("delta").load(tokens)
erc20s = erc20s.fillna(18, subset=['decimals'])
display(erc20s)

# COMMAND ----------

erc20_transfers = df_transfers.join(erc20s, df_transfers['ADDRESS'] == erc20s['address'], 'left')
erc20_transfers = erc20_transfers.select(df_transfers["*"], erc20s['name'], erc20s['decimals'])
erc20_transfers = erc20_transfers.where(col("name").isNotNull())
display(erc20_transfers)

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType

@udf(FloatType())
def hex_to_ethereum(hex_value, precision):
    wei_value = int(hex_value, 16) if len(hex_value) > 2 else 0 if hex_value[:2] == '0x' else None
    value = wei_value / 10**precision
    return value

# Apply the UDF to convert the hexadecimal Ethereum value to float
erc20_transfers_values = erc20_transfers.withColumn("transfer_amount", hex_to_ethereum(col("DATA"), col('decimals')))


# COMMAND ----------

erc20_transfers_values= erc20_transfers_values.filter((col('transfer_amount')!=0) & (col('transfer_amount').isNotNull()))

# COMMAND ----------

display(erc20_transfers_values)

# COMMAND ----------

from pyspark.sql.functions import sum, date_format, weekofyear, count

erc20_transfers_values = erc20_transfers_values.withColumn("week", weekofyear(col("BLOCK_TIMESTAMP")))

# Group by ADDRESS and week, and calculate the count/sum of transfer amount
grouped_df = erc20_transfers_values.groupBy("ADDRESS", "week").agg(
    count(erc20_transfers_values.transfer_amount).alias("count_transfers"),
    sum(erc20_transfers_values.transfer_amount).alias("total_transfer_amount")
)


# COMMAND ----------

# display(grouped_df)

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


# Create a window specification
window_spec = Window.partitionBy("week").orderBy(col("count_transfers").desc(), col("total_transfer_amount").desc())

# Add row number based on the window specification
grouped_df_per_week = grouped_df.withColumn("row_num", row_number().over(window_spec))

# Filter to keep the first row per week
first_row_per_week = grouped_df_per_week.filter(col("row_num") == 1).drop("row_num")

# Show the resulting DataFrame
first_row_per_week.show()
