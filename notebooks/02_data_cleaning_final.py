# Databricks notebook source
from pyspark.sql.functions import col, expr, trim, when, sum
from pyspark.sql.types import StringType

df = spark.table("workspace.bronze.hospital_cost_report")

# Remove the completely empty column
df = df.drop("DRG_Amounts_Other_Than_Outlier_Payments")

print("Total records:", df.count())
print("Total columns:", len(df.columns))

# COMMAND ----------

numeric_columns = """
Net_Income Total_Assets Total_Costs Total_Income Total_Liabilities
Total_Current_Assets Total_Current_Liabilities Total_Fixed_Assets
Total_Long_Term_Liabilities Total_Other_Assets Total_Other_Expenses
Total_Other_Income Total_Patient_Revenue Accounts_Payable Accounts_Receivable
Cash_on_Hand_and_in_Banks Buildings Fixed_Equipment Inventory Investments
Land Land_Improvements Leasehold_Improvements Major_Movable_Equipment
Other_Assets Other_Current_Assets Prepaid_Expenses Temporary_Investments
Mortgage_Payable Notes_Payable Notes_and_Loans_Payable_Short_Term
Other_Current_Liabilities Other_Long_Term_Liabilities Payroll_Taxes_Payable
Salaries_Wages_and_Fees_Payable Total_Fund_Balances
Total_Liabilities_and_Fund_Balances Deferred_Income Inpatient_Revenue
Inpatient_Total_Charges Net_Income_from_Service_to_Patients Net_Patient_Revenue
Net_Revenue_from_Medicaid Net_Revenue_from_Stand_Alone_CHIP Outpatient_Revenue
Outpatient_Total_Charges Medicaid_Charges Stand_Alone_CHIP_Charges
Combined_Outpatient_+_Inpatient_Total_Charges Depreciation_Cost
Less_Total_Operating_Expense Overhead_Non_Salary_Costs Total_Bad_Debt_Expense
Cost_of_Charity_Care Cost_of_Uncompensated_Care
Total_Unreimbursed_and_Uncompensated_Care Allowable_DSH_Percentage
Cost_To_Charge_Ratio DRG_Amounts_After_October_1 DRG_Amounts_Before_October_1
Disproportionate_Share_Adjustment FTE___Employees_on_Payroll
Managed_Care_Simulated_Payments Number_of_Interns_and_Residents_FTE
Outlier_Payments_For_Discharges Total_IME_Payment Total_Salaries_adjusted
Total_Salaries_From_Worksheet_A Wage_Related_Costs_Core
Wage_Related_Costs_for_Interns_and_Residents Wage_Related_Costs_RHC_FQHC
Wage_Related_Costs_for_Part___A_Teaching_Physicians Number_of_Beds
Total_Bed_Days_Available Total_Days_V_+_XVIII_+_XIX_+_Unknown
Total_Discharges_V_+_XVIII_+_XIX_+_Unknown
""".split()

for c in numeric_columns:
    df = df.withColumn(c, expr(f"try_cast(nullif(trim(`{c}`), '') AS DOUBLE)"))

print("Numeric conversions completed:", len(numeric_columns))

# COMMAND ----------

date_columns = [
    "Fiscal_Year_Begin_Date",
    "Fiscal_Year_End_Date"
]

for c in date_columns:
    df = df.withColumn(
        c,
        expr(f"try_cast(nullif(trim(`{c}`), '') AS DATE)")
    )

df.select(*date_columns).printSchema()

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.hospital_cost_report_cleaned")

# COMMAND ----------

silver_df = spark.table("workspace.silver.hospital_cost_report_cleaned")

print("Total records:", silver_df.count())
print("Total columns:", len(silver_df.columns))
silver_df.printSchema()

# COMMAND ----------

silver_df = spark.table("workspace.silver.hospital_cost_report_cleaned")

print("Total records:", silver_df.count())
print("Total columns:", len(silver_df.columns))


# COMMAND ----------

from pyspark.sql.functions import col, trim

# Check duplicate report IDs
duplicate_ids = (
    silver_df.groupBy("rpt_rec_num")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate report IDs:", duplicate_ids.count())

# Check missing values
key_columns = [
    "Provider_CCN", "Hospital_Name",
    "State_Code", "Total_Costs",
    "Total_Income", "Net_Income"
]

for c in key_columns:
    value = trim(col(c).cast("string"))
    missing = silver_df.filter(
        value.isNull() | (value == "")
    ).count()
    print(c, "missing:", missing)

# COMMAND ----------

