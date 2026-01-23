import pandas as pd

from airflow.decorators import dag, task
from airflow.utils import yaml
from pendulum import datetime

from include.etl.extract_data_s3 import extract_data_from_s3
from include.etl.load_data import load_data_to_snowflake
from include.etl.transform import clean_sales_data, clean_customers_data, clean_products_data, merge_data, \
    compute_monthly_aggregates, segment_customers

with open("include/config.yaml", 'r') as file:
    config = yaml.safe_load(file)


@dag(
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['exercise'],
)
def etl_pipeline_dag():
    @task()
    def extract_data(bucket: str, folder: str, aws_conn_id: str) -> dict:
        return extract_data_from_s3(bucket=bucket, folder=folder, aws_conn_id=aws_conn_id)


    @task()
    def get_sales_file(files: dict) -> str:
        for key, df in files.items():
            if "sales" in key:
                return df.to_json(orient="split")
        raise ValueError("Sales file not found")

    @task()
    def get_customers_file(files: dict) -> str:
        for key, df in files.items():
            if "customer" in key:
                return df.to_json(orient="split")
        raise ValueError("Customers file not found")

    @task()
    def get_products_file(files: dict) -> str:
        for key, df in files.items():
            if "product" in key:
                return df.to_json(orient="split")
        raise ValueError("Product file not found")

    @task()
    def transform_sales_data(sales_file: str) -> str:
        sales_df = pd.read_json(sales_file, orient="split")
        sales_df = clean_sales_data(sales_df)
        return sales_df.to_json(orient="split", date_format="iso")

    @task()
    def transform_customers_file(customers_file: str) -> str:
        customers_df = pd.read_json(customers_file, orient="split")
        customers_df = clean_customers_data(customers_df)
        return customers_df.to_json(orient="split", date_format="iso")

    @task()
    def transform_product_file(products_file: str) -> str:
        product_df = pd.read_json(products_file, orient="split")
        product_df = clean_products_data(product_df)
        return product_df.to_json(orient="split",)

    @task()
    def merged_data_task(transformed_sales: str, transformed_customers: str, transformed_products: str) -> str:
        sales_df = pd.read_json(transformed_sales, orient="split")
        customers_df = pd.read_json(transformed_customers, orient="split")
        products_df = pd.read_json(transformed_products, orient="split")
        merged_df = merge_data(sales_df=sales_df, customers_df=customers_df, products_df=products_df)
        return merged_df.to_json(orient="split")

    @task()
    def aggregated_data_task(merged_data: str) -> str:
        merged_df = pd.read_json(merged_data, orient="split")
        aggregated_df = compute_monthly_aggregates(merged_df=merged_df)
        return aggregated_df.to_json(orient="split", date_format="iso")

    @task()
    def segment_customers_task(sales: str, customers: str) -> str:
        sales_df = pd.read_json(sales, orient="split")
        customers_df = pd.read_json(customers, orient="split")
        segmented_df = segment_customers(sales_df, customers_df)
        return segmented_df.to_json(orient="split", date_format="iso")

    @task()
    def load_to_snowflake_task(final_json: str, database: str, schema_name: str, table_name: str):
        final_df = pd.read_json(final_json, orient="split")
        load_data_to_snowflake(df=final_df, database=database, schema=schema_name, table=table_name)

    files = extract_data(
        bucket=config['s3']['bucket'],
        folder=config['s3']['folder'],
        aws_conn_id=config['aws_conn_id']
    )

    sales_file = get_sales_file(files=files)
    customers_file = get_customers_file(files=files)
    products_file = get_products_file(files=files)

    transformed_sales = transform_sales_data(sales_file=sales_file)
    transformed_customers = transform_customers_file(customers_file=customers_file)
    transformed_products = transform_product_file(products_file=products_file)

    merge_output = merged_data_task(transformed_sales, transformed_customers, transformed_products)

    aggregated_output = aggregated_data_task(merge_output)

    segment_output = segment_customers_task(transformed_sales, transformed_customers)

    load_to_snowflake_task(transformed_sales, config["snowflake"]["database"],
                           config["snowflake"]["targets"]["sales"]["schema"],
                           config["snowflake"]["targets"]["sales"]["tables"],
                           )

    load_to_snowflake_task(transformed_customers, config["snowflake"]["database"],
                           config["snowflake"]["targets"]["customers"]["schema"],
                           config["snowflake"]["targets"]["customers"]["tables"],
                           )

    load_to_snowflake_task(transformed_products, config["snowflake"]["database"],
                           config["snowflake"]["targets"]["products"]["schema"],
                           config["snowflake"]["targets"]["products"]["tables"],
                           )

    load_to_snowflake_task(aggregated_output, config["snowflake"]["database"],
                           config["snowflake"]["targets"]["monthly_sales"]["schema"],
                           config["snowflake"]["targets"]["monthly_sales"]["tables"],
                           )

etl_pipeline_dag()