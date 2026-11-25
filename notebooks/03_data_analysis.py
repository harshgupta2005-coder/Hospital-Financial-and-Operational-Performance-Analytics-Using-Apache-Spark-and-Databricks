# Databricks notebook source
from pyspark.sql.functions import count, countDistinct, sum, avg

silver_df = spark.table("workspace.silver.hospital_cost_report_cleaned")

state_summary = silver_df.groupBy("State_Code").agg(
    count("*").alias("Total_Reports"),
    countDistinct("Provider_CCN").alias("Total_Hospitals"),
    sum("Total_Costs").alias("Total_Costs"),
    sum("Total_Income").alias("Total_Income"),
    sum("Net_Income").alias("Net_Income"),
    avg("Cost_To_Charge_Ratio").alias("Average_Cost_To_Charge_Ratio")
)

state_summary.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.hospital_financial_summary_by_state"
)

print("Gold table created successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

gold_df = spark.table(
    "workspace.gold.hospital_financial_summary_by_state"
)

display(
    gold_df.orderBy(
        col("Total_Income").desc()
    )
)

# COMMAND ----------

from pyspark.sql.functions import col

top_states_cost = gold_df.select(
    "State_Code",
    "Total_Hospitals",
    "Total_Costs",
    "Total_Income",
    "Net_Income"
).orderBy(
    col("Total_Costs").desc()
).limit(10)

display(top_states_cost)

# COMMAND ----------

from pyspark.sql.functions import when, round, col

profitability = gold_df.withColumn(
    "Net_Income_Margin_Percent",
    when(
        col("Total_Income") != 0,
        round(
            col("Net_Income") / col("Total_Income") * 100, 2
        )
    )
)

display(
    profitability.select(
        "State_Code",
        "Total_Income",
        "Net_Income",
        "Net_Income_Margin_Percent"
    ).orderBy(col("Net_Income_Margin_Percent").desc())
)

# COMMAND ----------

from pyspark.sql.functions import col

negative_income_states = gold_df.filter(
    col("Net_Income") < 0
).select(
    "State_Code",
    "Total_Hospitals",
    "Total_Income",
    "Net_Income"
).orderBy(
    col("Net_Income")
)

display(negative_income_states)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

control_summary = silver_df.groupBy(
    "Type_of_Control"
).agg(
    count("*").alias("Total_Reports"),
    countDistinct("Provider_CCN").alias("Total_Hospitals"),
    sum("Total_Costs").alias("Total_Costs"),
    sum("Total_Income").alias("Total_Income"),
    sum("Net_Income").alias("Net_Income")
)

display(
    control_summary.orderBy("Total_Hospitals", ascending=False)
)

# COMMAND ----------

control_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.hospital_summary_by_control"
    )

print("Ownership summary saved successfully!")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

location_summary = silver_df.groupBy(
    "Rural_Versus_Urban"
).agg(
    count("*").alias("Total_Reports"),
    countDistinct("Provider_CCN").alias("Total_Hospitals"),
    sum("Total_Costs").alias("Total_Costs"),
    sum("Total_Income").alias("Total_Income"),
    sum("Net_Income").alias("Net_Income")
)

display(location_summary)

# COMMAND ----------

location_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.hospital_summary_by_location"
    )

print("Location summary saved successfully!")

# COMMAND ----------

gold_tables = [
    "workspace.gold.hospital_financial_summary_by_state",
    "workspace.gold.hospital_summary_by_control",
    "workspace.gold.hospital_summary_by_location"
]

for table in gold_tables:
    result = spark.table(table)
    print("Table:", table)
    print("Records:", result.count())
    print("Columns:", len(result.columns))
    print("----------------------")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

provider_summary = silver_df.groupBy(
    "Provider_Type"
).agg(
    count("*").alias("Total_Reports"),
    countDistinct("Provider_CCN").alias("Total_Hospitals"),
    sum("Total_Costs").alias("Total_Costs"),
    sum("Total_Income").alias("Total_Income"),
    sum("Net_Income").alias("Net_Income")
)

display(
    provider_summary.orderBy(
        "Total_Hospitals", ascending=False
    )
)

# COMMAND ----------

provider_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.hospital_summary_by_provider_type"
    )

print("Provider type summary saved successfully!")

# COMMAND ----------

