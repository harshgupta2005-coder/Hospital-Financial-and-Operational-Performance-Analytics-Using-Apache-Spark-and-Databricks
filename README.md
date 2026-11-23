# Hospital Financial and Operational Performance Analytics Using Apache Spark and Databricks

## Overview
A data engineering and analytics project using the CMS Hospital Provider Cost Report, Apache Spark, Databricks, Delta Lake, and a published Databricks AI/BI Dashboard.

## Dataset
- Source: [CMS Hospital Provider Cost Report](https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report)
- API: [https://data.cms.gov/data-api/v1/dataset/bcddce31-68ff-4475-9024-77f6eb3a1e96/data](https://data.cms.gov/data-api/v1/dataset/bcddce31-68ff-4475-9024-77f6eb3a1e96/data)
- Extract size: 6,109 records and 117 original columns.
- The dataset contains hospital/provider cost report records, not individual patient-level data.

## Architecture
CMS API -> Raw JSON in Unity Catalog Volume -> Bronze Delta -> Silver cleaned Delta -> Gold analytics -> Databricks AI/BI Dashboard.

**Pipeline scope:** `Hospital_Cost_Report_ETL` currently reads the cleaned Silver table and materializes the five Gold dashboard outputs. API extraction and Silver preparation are notebook-based steps and are not currently orchestrated by this same Lakeflow pipeline.

## Databricks objects
- Raw JSON Volume: `/Volumes/workspace/bronze/raw_data/hospital_cost_report_raw.json`
- Bronze: `workspace.bronze.hospital_cost_report` (6,109 rows, 117 columns)
- Silver: `workspace.silver.hospital_cost_report_cleaned` (6,109 rows, 116 columns)
- Gold: `workspace.gold.etl_dashboard_overall_kpis`
- Gold: `workspace.gold.etl_dashboard_state_data`
- Gold: `workspace.gold.etl_dashboard_control_data`
- Gold: `workspace.gold.etl_dashboard_location_data`
- Gold: `workspace.gold.etl_dashboard_provider_data`

## Dashboard
[Published Hospital Financial and Operational Performance Dashboard](https://dbc-40138ea6-81d0.cloud.databricks.com/dashboardsv3/01f1e6f2747f111d8241f13f71e87572/published?o=7474650750068223)

## Main results
- Report records: 6,109
- Distinct Provider_CCN values: 6,026
- Reported total costs: 1,240,041,775,944
- Reported total income: 125,584,632,062
- Reported net income: 116,177,314,597

Financial totals are sums across the available report records, not a single-year national total. Multiple report records can belong to one provider.

## Technology stack
Python, PySpark, Apache Spark, Databricks, Delta Lake, Unity Catalog, Lakeflow Spark Declarative Pipelines, Databricks SQL, and Databricks AI/BI Dashboards.

## Notebooks
The Databricks `Healthcare` workspace folder contains the project notebooks. Export those notebooks from Databricks and include them with the submission. The raw JSON and Delta tables remain in the Databricks Unity Catalog Volume and catalog; they are not automatically embedded in a notebook export.

## References
- https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report
- https://data.cms.gov/data-api/v1/dataset/bcddce31-68ff-4475-9024-77f6eb3a1e96/data
- https://docs.databricks.com/
