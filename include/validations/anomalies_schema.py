import pandas as pd
import pandera.pandas as pa

from pandera import Column, Check
from pandera.errors import SchemaError

from .. logger import setup_logger
logging = setup_logger("etl.validation.anomalies")

anomalies_schema = pa.DataFrameSchema({
    "order_id": Column(str),
    "customer_id": Column(int, Check.greater_than(0)),
    "product_id": Column(int, Check.greater_than(0)),
    "order_date": Column(pa.DateTime),
    "total_revenue": Column(float),
})

def validate_post_anomalies_schema(df: pd.DataFrame) -> pd.DataFrame:
    return anomalies_schema.validate(df)