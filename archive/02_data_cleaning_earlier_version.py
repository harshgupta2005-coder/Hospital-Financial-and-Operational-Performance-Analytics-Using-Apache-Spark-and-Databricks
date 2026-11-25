# Databricks notebook source
df = spark.read.table("workspace.bronze.hospital_cost_report")

display(df)

# COMMAND ----------

df = df.drop("DRG_Amounts_Other_Than_Outlier_Payments")

# COMMAND ----------

print("Rows:", df.count())
print("Columns:", len(df.columns))

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.hospital_cost_report")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM workspace.silver.hospital_cost_report;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("Total_Costs").show(10)

# COMMAND ----------

from pyspark.sql.functions import col

columns = [
    "Total_Costs",
    "Total_Assets",
    "Total_Income",
    "Net_Income",
    "Total_Liabilities",
    "Total_Patient_Revenue"
]

for c in columns:
    df = df.withColumn(c, col(c).cast("double"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print("Total columns:", len(df.columns))
print("Empty column present:", "DRG_Amounts_Other_Than_Outlier_Payments" in df.columns)

# COMMAND ----------

columns = [
    "Total_Current_Assets",
    "Total_Current_Liabilities",
    "Total_Fixed_Assets",
    "Total_Long_Term_Liabilities",
    "Total_Other_Assets",
    "Total_Other_Expenses",
    "Total_Other_Income"
]

for c in columns:
    df = df.withColumn(c, col(c).cast("double"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

df = df.withColumn(
    "Fiscal_Year_Begin_Date",
    to_date("Fiscal_Year_Begin_Date")
)

df = df.withColumn(
    "Fiscal_Year_End_Date",
    to_date("Fiscal_Year_End_Date")
)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

columns = [
    "Number_of_Beds",
    "Total_Bed_Days_Available",
    "Total_Days_V_+_XVIII_+_XIX_+_Unknown",
    "Total_Discharges_V_+_XVIII_+_XIX_+_Unknown"
]

for c in columns:
    df = df.withColumn(c, col(c).cast("double"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

asset_columns = [
    "Accounts_Receivable",
    "Cash_on_Hand_and_in_Banks",
    "Buildings",
    "Fixed_Equipment",
    "Inventory",
    "Investments",
    "Land",
    "Land_Improvements",
    "Leasehold_Improvements",
    "Major_Movable_Equipment",
    "Other_Assets",
    "Other_Current_Assets",
    "Prepaid_Expenses",
    "Temporary_Investments"
]

for column in asset_columns:
    df = df.withColumn(column, col(column).cast("double"))

# COMMAND ----------

df.select(*asset_columns).printSchema()

# COMMAND ----------

liability_columns = [
    "Accounts_Payable",
    "Mortgage_Payable",
    "Notes_Payable",
    "Notes_and_Loans_Payable_Short_Term",
    "Other_Current_Liabilities",
    "Other_Long_Term_Liabilities",
    "Payroll_Taxes_Payable",
    "Salaries_Wages_and_Fees_Payable",
    "Total_Fund_Balances",
    "Total_Liabilities_and_Fund_Balances",
    "Deferred_Income"
]

for column in liability_columns:
    df = df.withColumn(column, col(column).cast("double"))

# COMMAND ----------

df.select(*liability_columns).printSchema()

# COMMAND ----------

revenue_columns = [
    "Inpatient_Revenue",
    "Inpatient_Total_Charges",
    "Net_Income_from_Service_to_Patients",
    "Net_Patient_Revenue",
    "Net_Revenue_from_Medicaid",
    "Net_Revenue_from_Stand_Alone_CHIP",
    "Outpatient_Revenue",
    "Outpatient_Total_Charges",
    "Medicaid_Charges",
    "Stand_Alone_CHIP_Charges",
    "Combined_Outpatient_+_Inpatient_Total_Charges"
]

for column in revenue_columns:
    df = df.withColumn(column, col(column).cast("double"))

# COMMAND ----------

df.select(*revenue_columns).printSchema()

# COMMAND ----------

expense_columns = [
    "Depreciation_Cost",
    "Less_Total_Operating_Expense",
    "Overhead_Non_Salary_Costs",
    "Total_Bad_Debt_Expense",
    "Cost_of_Charity_Care",
    "Cost_of_Uncompensated_Care",
    "Total_Unreimbursed_and_Uncompensated_Care",
    "Less_Contractual_Allowance_and_Discounts_on_Patients'_Accounts",
    "Less:_Allowances_for_Uncollectible_Notes_and_Accounts_Receivable"
]

for column in expense_columns:
    df = df.withColumn(column, col(column).cast("double"))

# COMMAND ----------

df.select(*expense_columns).printSchema()

# COMMAND ----------

other_numeric_columns = [
    "Allowable_DSH_Percentage",
    "Cost_To_Charge_Ratio",
    "DRG_Amounts_After_October_1",
    "DRG_Amounts_Before_October_1",
    "Disproportionate_Share_Adjustment",
    "FTE___Employees_on_Payroll",
    "Managed_Care_Simulated_Payments",
    "Number_of_Interns_and_Residents_FTE",
    "Outlier_Payments_For_Discharges",
    "Total_IME_Payment",
    "Total_Salaries_adjusted",
    "Total_Salaries_From_Worksheet_A",
    "Wage_Related_Costs_Core",
    "Wage_Related_Costs_for_Interns_and_Residents",
    "Wage_Related_Costs_RHC_FQHC",
    "Wage_Related_Costs_for_Part___A_Teaching_Physicians"
]

for column in other_numeric_columns:
    df = df.withColumn(column, col(column).cast("double"))

# COMMAND ----------

df.select(*other_numeric_columns).printSchema()

# COMMAND ----------

from pyspark.sql.functions import expr

for field in df.schema.fields:
    if field.dataType.simpleString() == "double":
        column = field.name
        df = df.withColumn(
            column,
            expr(f"try_cast(`{column}` AS DOUBLE)")
        )

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, trim
from pyspark.sql.types import StringType

missing_columns = []

for field in df.schema.fields:

    if isinstance(field.dataType, StringType):
        condition = col(field.name).isNull() | (trim(col(field.name)) == "")
    else:
        condition = col(field.name).isNull()

    missing_columns.append(
        sum(when(condition, 1).otherwise(0)).alias(field.name)
    )

missing_counts = df.agg(*missing_columns).first().asDict()

for column, count in missing_counts.items():
    if count > 0:
        print(column, ":", count)

# COMMAND ----------

df = spark.table("workspace.silver.hospital_cost_report")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import expr

numeric_columns = [
    # Financial columns
    "Net_Income", "Total_Assets", "Total_Costs",
    "Total_Current_Assets", "Total_Current_Liabilities",
    "Total_Fixed_Assets", "Total_Income", "Total_Liabilities",
    "Total_Long_Term_Liabilities", "Total_Other_Assets",
    "Total_Other_Expenses", "Total_Other_Income",
    "Total_Patient_Revenue",

    # Asset columns
    "Accounts_Receivable", "Cash_on_Hand_and_in_Banks",
    "Buildings", "Fixed_Equipment", "Inventory",
    "Investments", "Land", "Land_Improvements",
    "Leasehold_Improvements", "Major_Movable_Equipment",
    "Other_Assets", "Other_Current_Assets",
    "Prepaid_Expenses", "Temporary_Investments",

    # Liability columns
    "Accounts_Payable", "Mortgage_Payable", "Notes_Payable",
    "Notes_and_Loans_Payable_Short_Term",
    "Other_Current_Liabilities", "Other_Long_Term_Liabilities",
    "Payroll_Taxes_Payable", "Salaries_Wages_and_Fees_Payable",
    "Total_Fund_Balances",
    "Total_Liabilities_and_Fund_Balances", "Deferred_Income",

    # Revenue columns
    "Inpatient_Revenue", "Inpatient_Total_Charges",
    "Net_Income_from_Service_to_Patients", "Net_Patient_Revenue",
    "Net_Revenue_from_Medicaid", "Net_Revenue_from_Stand_Alone_CHIP",
    "Outpatient_Revenue", "Outpatient_Total_Charges",
    "Medicaid_Charges", "Stand_Alone_CHIP_Charges",
    "Combined_Outpatient_+_Inpatient_Total_Charges",

    # Expense columns
    "Depreciation_Cost", "Less_Total_Operating_Expense",
    "Overhead_Non_Salary_Costs", "Total_Bad_Debt_Expense",
    "Cost_of_Charity_Care", "Cost_of_Uncompensated_Care",
    "Total_Unreimbursed_and_Uncompensated_Care",
    "Less_Contractual_Allowance_and_Discounts_on_Patients'_Accounts",
    "Less:_Allowances_for_Uncollectible_Notes_and_Accounts_Receivable",

    # Other financial and operational columns
    "Allowable_DSH_Percentage", "Cost_To_Charge_Ratio",
    "DRG_Amounts_After_October_1", "DRG_Amounts_Before_October_1",
    "Disproportionate_Share_Adjustment",
    "FTE___Employees_on_Payroll",
    "Managed_Care_Simulated_Payments",
    "Number_of_Interns_and_Residents_FTE",
    "Outlier_Payments_For_Discharges", "Total_IME_Payment",
    "Total_Salaries_adjusted", "Total_Salaries_From_Worksheet_A",
    "Wage_Related_Costs_Core",
    "Wage_Related_Costs_for_Interns_and_Residents",
    "Wage_Related_Costs_RHC_FQHC",
    "Wage_Related_Costs_for_Part___A_Teaching_Physicians",

    # Operational columns
    "Number_of_Beds", "Total_Bed_Days_Available",
    "Total_Days_V_+_XVIII_+_XIX_+_Unknown",
    "Total_Discharges_V_+_XVIII_+_XIX_+_Unknown"
]

for column in numeric_columns:
    df = df.withColumn(
        column,
        expr(f"try_cast(nullif(trim(`{column}`), '') AS DOUBLE)")
    )

# COMMAND ----------

df.select(*numeric_columns).printSchema()

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.hospital_cost_report")

# COMMAND ----------

from pyspark.sql.functions import expr

date_columns = [
    "Fiscal_Year_Begin_Date",
    "Fiscal_Year_End_Date"
]

for column in date_columns:
    df = df.withColumn(
        column,
        expr(f"try_cast(nullif(trim(`{column}`), '') AS DATE)")
    )

# COMMAND ----------

df.select(*date_columns).printSchema()

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.hospital_cost_report")
    

# COMMAND ----------

spark.catalog.refreshTable("workspace.silver.hospital_cost_report")
spark.catalog.clearCache()

# COMMAND ----------

