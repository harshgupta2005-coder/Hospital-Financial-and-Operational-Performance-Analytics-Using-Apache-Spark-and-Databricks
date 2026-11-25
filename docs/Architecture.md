# Project Architecture

```mermaid
flowchart TD
    A[CMS Hospital Provider Cost Report API] --> B[Python API Extraction Notebook]
    B --> C[Raw JSON in Unity Catalog Volume]
    C --> D[Bronze Delta Table\nworkspace.bronze.hospital_cost_report]
    D --> E[PySpark Cleaning and Validation]
    E --> F[Silver Delta Table\nworkspace.silver.hospital_cost_report_cleaned]
    F --> G[Lakeflow Spark Declarative Pipeline\nHospital_Cost_Report_ETL]
    G --> H1[Gold: Overall KPIs]
    G --> H2[Gold: State Summary]
    G --> H3[Gold: Control Summary]
    G --> H4[Gold: Location Summary]
    G --> H5[Gold: Provider Summary]
    H1 --> I[Databricks AI/BI Dashboard]
    H2 --> I
    H3 --> I
    H4 --> I
    H5 --> I
```

**Pipeline scope:** The Lakeflow pipeline reads the already-prepared Silver table and produces the five Gold dashboard outputs. API extraction and Silver preparation are performed in separate notebooks.
