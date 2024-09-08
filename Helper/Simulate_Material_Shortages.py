# Databricks notebook source
dbutils.widgets.text('catalogName',  'demand_cat' , 'Catalog Name')
dbutils.widgets.text('dbName',  'demand_db' , 'Database Name')

# COMMAND ----------

catalogName = dbutils.widgets.get('catalogName')
dbName = dbutils.widgets.get('dbName')

# COMMAND ----------

import os
import random
import pyspark.sql.functions as f
from pyspark.sql.types import FloatType

# COMMAND ----------

demand_raw_df = spark.read.table(f"{catalogName}.{dbName}.forecast_raw")
all_skus = demand_raw_df.select('SKU').distinct().rdd.flatMap(lambda x: x).collect()
material_shortages_sku = random.sample(list(set(all_skus)), 2)
all_raw =  demand_raw_df.filter(f.col("SKU").isin(material_shortages_sku)).select('RAW').distinct().rdd.flatMap(lambda x: x).collect()
material_shortages_raw = random.sample(list(set(all_raw)), 3)
maximum_date =  max(demand_raw_df.select('Date').distinct().rdd.flatMap(lambda x: x).collect())

# COMMAND ----------

def random_fraction(z):
  return(random.uniform(0.5, 0.9))

random_fractionUDF = udf(lambda z: random_fraction(z),FloatType())

# COMMAND ----------

material_shortage_df = (demand_raw_df.
                          filter((f.col("SKU").isin(material_shortages_sku)) & (f.col("Date") == maximum_date)  & (f.col("RAW").isin(material_shortages_raw))).
                          withColumn("fraction", random_fractionUDF(  f.col("Demand_Raw")  )).
                          withColumn("available_demand", f.floor(f.col("fraction") * f.col("Demand_Raw"))).
                          select("RAW", "Date", "available_demand") 
 )
#display(material_shortage_df)

# COMMAND ----------

# Write the data 
material_shortage_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable(f"{catalogName}.{dbName}.material_shortage")
