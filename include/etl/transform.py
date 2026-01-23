import pandas as pd
from ..logger import setup_logger
from ..validations.aggregates_schema import validate_pre_aggregates_schema, validate_post_aggregates_schema
from ..validations.customers_schema import validate_pre_customers_schema, validate_post_customer_schema
from ..validations.products_schema import validate_pre_products_schema, validate_post_products_schema
from ..validations.sales_schema import validate_pre_sales_schema, validate_post_sales_schema
from ..validations.segment_schema import validate_post_segmentation_schema

logging = setup_logger("etl.transform")


def cleaning_fun(my_df: pd.DataFrame) -> pd.DataFrame:
    my_df = my_df.copy()  # avoid modifying original DataFrame outside the function
    my_df.columns = my_df.columns.str.lower().str.replace(' ', '_')
    """
    inplace=True is the same as sales_df = sales_df.dropna()
    """
    my_df.dropna(inplace=True)

    return my_df

def clean_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove missing values and standartisation columns
    """
    logging.info(f"Cleaning sales data from {len(sales_df)} rows")

    sales_df = validate_pre_sales_schema(sales_df)

    sales_df = cleaning_fun(sales_df)
    # mixed - all date formats,
    # coerce - returns not a date (not) if value is not convertable
    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"], format="mixed", errors="coerce")
    sales_df["total_revenue"] = sales_df["amount"] * sales_df["quantity"]

    sales_df = validate_post_sales_schema(sales_df)

    logging.info(f"Cleaned sales data {len(sales_df)} rows")
    return sales_df

def clean_customers_data(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove missing values and standardization columns
    """
    logging.info(f"Cleaning customer data from {len(customers_df)} rows")

    customers_df = validate_pre_customers_schema(customers_df)

    customers_df = cleaning_fun(customers_df)
    customers_df["signup_date"] = pd.to_datetime(customers_df["signup_date"], format="mixed", errors="coerce")

    customers_df = validate_post_customer_schema(customers_df)

    logging.info(f"Cleaned customer data {len(customers_df)} rows")
    return customers_df

def clean_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove missing values and standardization columns
    """
    logging.info(f"Cleaning product data from {len(products_df)} rows")

    products_df = validate_pre_products_schema(products_df)

    products_df = cleaning_fun(products_df)

    products_df = validate_post_products_schema(products_df)

    logging.info(f"Cleaned product data {len(products_df)} rows")
    return products_df

def merge_data(sales_df: pd.DataFrame, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge sales, customers and products data
    """
    logging.info("Merge sales, customers and products data")
    merged_df = sales_df.merge(customers_df, on="customer_id", how="inner").copy()
    merged_df = merged_df.merge(products_df, on="product_id", how="inner").copy()
    merged_df["profit_margin"] = merged_df["profit"] / merged_df["total_revenue"]
    logging.info(f"Merged sales, customers and products data")
    return merged_df

def compute_monthly_aggregates(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregated data by month
    """
    logging.info("Computing monthly aggregates")

    merged_df = validate_pre_aggregates_schema(merged_df)

    merged_df["order_date"] = pd.to_datetime(merged_df["order_date"], format="mixed", errors="coerce")
    # After groupby, pandas does something annoying: order_date becomes the index .reset_index() fixes this

    aggregate_df = merged_df.groupby(pd.Grouper(key="order_date", freq="M")).agg(
        total_sales=("total_revenue", "sum"),
        unique_customers=("customer_id", "nunique")
    ).reset_index().copy()

    aggregate_df = validate_post_aggregates_schema(aggregate_df)

    logging.info(f"Computed monthly aggregates on merged data")
    return aggregate_df

def segment_customers(sales_df: pd.DataFrame, customer_df: pd.DataFrame) -> pd.DataFrame:
    """
    Segment customers based on their total spent
    """
    logging.info("Segment customers based on their total spent")
    total_spent_df = sales_df.groupby("customer_id")["total_revenue"].sum().reset_index().copy()
    total_spent_df.rename(columns={"total_revenue": "total_spent"}, inplace=True)

    segmented_df = customer_df.merge(total_spent_df, on="customer_id", how="left").copy()

    segmented_df.dropna(subset=["total_spent"], inplace=True)

    segmented_df["customer_segments"] = pd.cut(
        segmented_df["total_spent"],
        bins=[0, 1000, 5000, 10000, float("inf")],
        labels=["Low", "Medium", "High", "VIP"],
    )

    segmented_df["segmentation_date"] = pd.to_datetime(customer_df["signup_date"], format="mixed", errors="coerce")
    allowed_columns = ["customer_id", "total_spent", "customer_segment"]
    df_segmented = drop_extra_columns(segmented_df, allowed_columns)

    df_segmented = validate_post_segmentation_schema(df_segmented)

    logging.info(f"Final segmented customers: {len(df_segmented)} rows")

    return df_segmented





def drop_extra_columns(df: pd.DataFrame, allowed_columns: list) -> pd.DataFrame:
    return df.loc[:, df.columns.isin(allowed_columns)].copy()