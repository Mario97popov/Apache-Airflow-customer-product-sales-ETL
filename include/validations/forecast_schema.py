import pandas as pd
import pandera.pandas as pa

from pandera import Column, Check
from pandera.errors import SchemaError

from .. logger import setup_logger
logging = setup_logger("etl.validation.forecast")

forecast_sales_schema = pa.DataFrameSchema({
    "order_date": Column(pa.DateTime),
    "total_revenue": Column(float, Check.greater_than_or_equal_to(0)),
    "sales_forecast": Column(float, Check.greater_than_or_equal_to(0))
})

def validate_post_sales_forecast_schema(df: pd.DataFrame) -> pd.DataFrame:
    return forecast_sales_schema.validate(df)