# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

import os
import re
import mlflow
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", "10")
catalog_prefix = "demand_planning_catalog"
db_prefix = "demand_planning"

# COMMAND ----------

# Get dbName and cloud_storage_path, reset and create database
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

dbName = db_prefix+"_"+current_user_no_at
catalogName = catalog_prefix+"_"+current_user_no_at
reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP CATALOG IF EXISTS {catalogName} CASCADE")

spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalogName}""")
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {catalogName}.{dbName}""")
spark.sql(f"""USE {catalogName}.{dbName}""")

# COMMAND ----------

print(catalogName)
print(dbName)

# COMMAND ----------

reset_all = dbutils.widgets.get('reset_all_data')
reset_all_bool = (reset_all == 'true')

# COMMAND ----------

dirname = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
filename = "01-data-generator"
if (os.path.basename(dirname) != '_resources'):
  dirname = os.path.join(dirname,'_resources')
generate_data_notebook_path = os.path.join(dirname,filename)

def generate_data():
  dbutils.notebook.run(generate_data_notebook_path, 600, {"reset_all_data": reset_all, "catalogName": catalogName, "dbName": dbName})

if reset_all_bool:
  generate_data()
else:
  try:
    print("Skipping Reset")
  except: 
    generate_data()

# COMMAND ----------

mlflow.set_experiment('/Users/{}/parts_demand_forecasting'.format(current_user))
