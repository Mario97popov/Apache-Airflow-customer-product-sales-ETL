import pandas as pd
import pandera.pandas as pa

from pandera import Column, Check
from pandera.errors import SchemaError

from .. logger import setup_logger
logging = setup_logger("etl.validation_customers")

pre_customer_schema = pa.DataFrameSchema({
    "customer_id": Column(int, unique=True),
    "name": Column(str),
    "email": Column(str),
    "signup_date": Column(pa.DateTime)
})

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

post_customer_schema = pa.DataFrameSchema({
    "customer_id": Column(int,Check.greater_than(0), unique=True),
    "name": Column(str, Check.str_length(0, 100)),
    "email": Column(str, Check.str_matches(EMAIL_REGEX)),
    "signup_date": Column(pa.DateTime)
})

def validate_pre_customers_schema(df: pd.DataFrame) -> pd.DataFrame:
    try:
        return pre_customer_schema.validate(df)
    except SchemaError as e:
        logging.warning(f"Pre-customers validation failed: {e.failure_cases}")
        return df

def validate_post_customer_schema(df: pd.DataFrame) -> pd.DataFrame:
    return post_customer_schema.validate(df)












