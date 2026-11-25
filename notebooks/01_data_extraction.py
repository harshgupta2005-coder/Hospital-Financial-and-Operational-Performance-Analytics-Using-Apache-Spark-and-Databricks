# Databricks notebook source
import requests

url = "https://data.cms.gov/data-api/v1/dataset/bcddce31-68ff-4475-9024-77f6eb3a1e96/data"

# Get total records
total_records = requests.get(url + "/stats").json()["total_rows"]

print("Total records:", total_records)

# COMMAND ----------

all_data = []

for offset in range(0, total_records, 1000):
    data = requests.get(url, params={"size": 1000, "offset": offset}).json()
    all_data.extend(data)

# COMMAND ----------

print("Total records extracted:", len(all_data))

# COMMAND ----------

import json

with open("/Volumes/workspace/bronze/raw_data/hospital_cost_report_raw.json", "w") as file:
    json.dump(all_data, file)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/bronze/raw_data/"))

# COMMAND ----------

df = spark.read.json("/Volumes/workspace/bronze/raw_data/hospital_cost_report_raw.json")

df.show()

# COMMAND ----------

print("Number of rows:", df.count())
print("Number of columns:", len(df.columns))

# COMMAND ----------

for col in df.columns:
    df = df.withColumnRenamed(col, col.replace(" ", "_").replace(",", "").replace("(", "").replace(")", "").replace("-", "_").replace("/", "_"))

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.hospital_cost_report")

# COMMAND ----------

bronze_df = spark.read.table("workspace.bronze.hospital_cost_report")

print("Rows:", bronze_df.count())
print("Columns:", len(bronze_df.columns))

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) - COUNT(Hospital_Name) AS Missing_Hospital_Name,
# MAGIC     COUNT(*) - COUNT(Provider_CCN) AS Missing_Provider_CCN,
# MAGIC     COUNT(*) - COUNT(Total_Costs) AS Missing_Total_Costs,
# MAGIC     COUNT(*) - COUNT(Net_Income) AS Missing_Net_Income
# MAGIC FROM workspace.bronze.hospital_cost_report;

# COMMAND ----------

from pyspark.sql.functions import col

for c in df.columns:
    print(c, df.filter(col(c).isNull() | (col(c) == "")).count())

# COMMAND ----------

for c in df.columns:
    if df.filter(col(c).isNull() | (col(c) == "")).count() == 6109:
        print(c)

# COMMAND ----------

