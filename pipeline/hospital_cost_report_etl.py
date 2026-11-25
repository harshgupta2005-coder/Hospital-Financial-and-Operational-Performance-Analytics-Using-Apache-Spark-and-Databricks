from pyspark import pipelines as dp
from pyspark.sql.functions import count, countDistinct, sum


@dp.materialized_view(name="etl_dashboard_state_data")
def etl_dashboard_state_data():
    silver_df = spark.read.table(
        "workspace.silver.hospital_cost_report_cleaned"
    )

    return silver_df.groupBy("State_Code").agg(
        count("*").alias("Total_Reports"),
        countDistinct("Provider_CCN").alias("Total_Hospitals"),
        sum("Total_Costs").alias("Total_Costs"),
        sum("Total_Income").alias("Total_Income"),
        sum("Net_Income").alias("Net_Income")
    )


@dp.materialized_view(name="etl_dashboard_control_data")
def etl_dashboard_control_data():
    silver_df = spark.read.table(
        "workspace.silver.hospital_cost_report_cleaned"
    )

    return silver_df.groupBy("Type_of_Control").agg(
        count("*").alias("Total_Reports"),
        countDistinct("Provider_CCN").alias("Total_Hospitals"),
        sum("Total_Costs").alias("Total_Costs"),
        sum("Total_Income").alias("Total_Income"),
        sum("Net_Income").alias("Net_Income")
    )


@dp.materialized_view(name="etl_dashboard_location_data")
def etl_dashboard_location_data():
    silver_df = spark.read.table(
        "workspace.silver.hospital_cost_report_cleaned"
    )

    return silver_df.groupBy("Rural_Versus_Urban").agg(
        count("*").alias("Total_Reports"),
        countDistinct("Provider_CCN").alias("Total_Hospitals"),
        sum("Total_Costs").alias("Total_Costs"),
        sum("Total_Income").alias("Total_Income"),
        sum("Net_Income").alias("Net_Income")
    )


@dp.materialized_view(name="etl_dashboard_provider_data")
def etl_dashboard_provider_data():
    silver_df = spark.read.table(
        "workspace.silver.hospital_cost_report_cleaned"
    )

    return silver_df.groupBy("Provider_Type").agg(
        count("*").alias("Total_Reports"),
        countDistinct("Provider_CCN").alias("Total_Hospitals"),
        sum("Total_Costs").alias("Total_Costs"),
        sum("Total_Income").alias("Total_Income"),
        sum("Net_Income").alias("Net_Income")
    )


@dp.materialized_view(name="etl_dashboard_overall_kpis")
def etl_dashboard_overall_kpis():
    silver_df = spark.read.table(
        "workspace.silver.hospital_cost_report_cleaned"
    )

    return silver_df.agg(
        count("*").alias("Total_Reports"),
        countDistinct("Provider_CCN").alias("Total_Hospitals"),
        sum("Total_Costs").alias("Reported_Total_Costs"),
        sum("Total_Income").alias("Reported_Total_Income"),
        sum("Net_Income").alias("Reported_Net_Income")
    )
