import pandas as pd
from ..logger import setup_logger

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
    sales_df = cleaning_fun(sales_df)
    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"], format="mixed", errors="coerce")
    sales_df["total_revenue"] = sales_df["amount"] * sales_df["quantity"]

    logging.info(f"Cleaning sales data from {len(sales_df)} rows")
    return sales_df

def clean_customers_data(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove missing values and standardization columns
    """
    logging.info(f"Cleaning customer data from {len(customers_df)} rows")
    customers_df = cleaning_fun(customers_df)
    customers_df["signup_date"] = pd.to_datetime(customers_df["signup_date"], format="mixed", errors="coerce")

    logging.info(f"Cleaning customer data from {len(customers_df)} rows")
    return customers_df

def clean_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove missing values and standardization columns
    """
    logging.info(f"Cleaning product data from {len(products_df)} rows")
    products_df = cleaning_fun(products_df)

    logging.info(f"Cleaning product data from {len(products_df)} rows")
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
    merged_df["order_date"] = pd.to_datetime(merged_df["order_date"], format="mixed", errors="coerce")
    aggregate_df = merged_df.groupby(pd.Grouper(key="order_date", freq="M")).agg(
        total_sales=("total_revenue", "sum"),
        unique_customers=("customer_id", "nunique")
    ).reset_index().copy()
    logging.info(f"Computed monthly aggregates on merged data")
    return aggregate_df
