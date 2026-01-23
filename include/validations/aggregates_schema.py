import pandas as pd
import pandera.pandas as pa

from pandera import Column, Check
from pandera.errors import SchemaError

from .. logger import setup_logger
logging = setup_logger("etl.validation.aggregates")

pre_aggregates_schema = pa.DataFrameSchema({
    "order_date": Column(pa.DateTime),
    "unique_customers": Column(int, unique=True),
    "total_sales": Column(int),
})

post_aggregates_schema = pa.DataFrameSchema({
    "order_date": Column(pa.DateTime),
    "unique_customers": Column(int, Check.greater_than_or_equal_to(0)),
    "total_sales": Column(float, Check.greater_than_or_equal_to(0)),
})

def validate_pre_aggregates_schema(df: pd.DataFrame) -> pd.DataFrame:
    try:
        return pre_aggregates_schema.validate(df)
    except SchemaError as e:
        logging.warning(f"Pre-aggregate validation failed: {e.failure_cases}")
        return df

def validate_post_aggregates_schema(df: pd.DataFrame) -> pd.DataFrame:
    return post_aggregates_schema.validate(df)