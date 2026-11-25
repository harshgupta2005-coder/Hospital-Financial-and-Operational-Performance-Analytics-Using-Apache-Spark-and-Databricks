# Databricks notebook source
state_df = spark.table(
    "workspace.gold.hospital_financial_summary_by_state"
)

control_df = spark.table(
    "workspace.gold.hospital_summary_by_control"
)

location_df = spark.table(
    "workspace.gold.hospital_summary_by_location"
)

provider_df = spark.table(
    "workspace.gold.hospital_summary_by_provider_type"
)

print("All Gold tables loaded successfully!")

# COMMAND ----------

datasets = {
    "State Analysis": state_df,
    "Ownership Analysis": control_df,
    "Location Analysis": location_df,
    "Provider Analysis": provider_df
}

for name, data in datasets.items():
    print(name)
    print("Records:", data.count())
    print("Columns:", len(data.columns))
    print("--------------------")

# COMMAND ----------

display(
    state_df.orderBy(
        "Total_Income",
        ascending=False
    )
)

# COMMAND ----------

display(
    control_df.orderBy(
        "Total_Hospitals",
        ascending=False
    )
)

# COMMAND ----------

display(
    location_df.orderBy(
        "Total_Hospitals",
        ascending=False
    )
)

# COMMAND ----------

display(
    provider_df.orderBy(
        "Total_Hospitals",
        ascending=False
    )
)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

silver_df = spark.table(
    "workspace.silver.hospital_cost_report_cleaned"
)

kpi_df = silver_df.agg(
    count("*").alias("Total_Reports"),
    countDistinct("Provider_CCN").alias("Total_Hospitals"),
    sum("Total_Costs").alias("Reported_Total_Costs"),
    sum("Total_Income").alias("Reported_Total_Income"),
    sum("Net_Income").alias("Reported_Net_Income")
)

display(kpi_df)

# COMMAND ----------

kpi_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.hospital_overall_kpis"
    )

print("Overall KPI table saved successfully!")

# COMMAND ----------


state_dashboard = state_df.select(
    "State_Code",
    "Total_Reports",
    "Total_Hospitals",
    "Total_Costs",
    "Total_Income",
    "Net_Income"
)

display(state_dashboard.orderBy(
    "Total_Costs", ascending=False
))

# COMMAND ----------

state_dashboard.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.dashboard_state_data"
    )

print("State dashboard data saved successfully!")

# COMMAND ----------

control_dashboard = control_df.select(
    "Type_of_Control",
    "Total_Reports",
    "Total_Hospitals",
    "Total_Costs",
    "Total_Income",
    "Net_Income"
)

display(control_dashboard.orderBy(
    "Total_Hospitals", ascending=False
))

# COMMAND ----------

control_dashboard.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.dashboard_control_data"
    )

print("Ownership dashboard data saved successfully!")

# COMMAND ----------

location_dashboard = location_df.select(
    "Rural_Versus_Urban",
    "Total_Reports",
    "Total_Hospitals",
    "Total_Costs",
    "Total_Income",
    "Net_Income"
)

display(location_dashboard)


# COMMAND ----------

location_dashboard.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.dashboard_location_data"
    )

print("Location dashboard data saved successfully!")

# COMMAND ----------

provider_dashboard = provider_df.select(
    "Provider_Type",
    "Total_Reports",
    "Total_Hospitals",
    "Total_Costs",
    "Total_Income",
    "Net_Income"
)

display(provider_dashboard.orderBy(
    "Total_Hospitals", ascending=False
))

# COMMAND ----------

provider_dashboard.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.dashboard_provider_data"
    )

print("Provider dashboard data saved successfully!")


# COMMAND ----------

dashboard_tables = [
    "workspace.gold.hospital_overall_kpis",
    "workspace.gold.dashboard_state_data",
    "workspace.gold.dashboard_control_data",
    "workspace.gold.dashboard_location_data",
    "workspace.gold.dashboard_provider_data"
]

for table in dashboard_tables:
    data = spark.table(table)
    print("Table:", table)
    print("Records:", data.count())
    print("Columns:", len(data.columns))
    print("----------------------")

# COMMAND ----------

