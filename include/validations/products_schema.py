import pandas as pd
import pandera.pandas as pa

from pandera import Column, Check
from pandera.errors import SchemaError

from .. logger import setup_logger
logging = setup_logger("etl.validation_customers")

pre_products_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "product_name": Column(str),
    "category": Column(str),
    "price": Column(float)
})

post_products_schema = pa.DataFrameSchema({
    "product_id": Column(int,Check.greater_than(0)),
    "product_name": Column(str, Check.str_length(0, 100)),
    "category": Column(str),
    "price": Column(float,Check.greater_than_or_equal_to(0))
})

def validate_pre_products_schema(df: pd.DataFrame) -> pd.DataFrame:
    try:
        return pre_products_schema.validate(df)
    except SchemaError as e:
        logging.warning(f"Pre-products validation failed: {e.failure_cases}")
        return df

def validate_post_products_schema(df: pd.DataFrame) -> pd.DataFrame:
    return post_products_schema.validate(df)