## Project Overview

My goal is to clean, transform, analyze, and load the data into **Snowflake** for further analytics.

The project works with three datasets:
- Sales
- Products
- Customers

Logging is implemented in each function to help with monitoring and debugging.

---

## Pipeline Steps

- Data is extracted from **Amazon S3**
- Sales, product, and customer datasets are cleaned and standardized
- The three datasets are merged into a single dataset for analysis
- Sales data is aggregated on a **monthly** basis
- Customers are segmented based on their **total spend**
- Sales anomalies are detected using **total revenue** and **standard deviation**
- A **7-day mean sales forecast** is generated
- Final datasets are loaded into **Snowflake**

---

## Data Validation

Schema validation is applied to ensure data quality for:
- Sales
- Products
- Customers
- Aggregated data
- Customer segments
- Anomalies
- Forecast data


<img width="1777" height="838" alt="etl_pipeline_dag-graph" src="https://github.com/user-attachments/assets/c01982b9-5a20-4d07-8384-57dbb5d340fd" />

