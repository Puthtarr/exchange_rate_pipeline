import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from src.transform.clean_exchange_rate import clean_exchange_rate_df

def test_clean_exchange_rate_df():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-clean-exchange-rate") \
        .getOrCreate()

    raw_data = spark.createDataFrame([
        {"date": "2025-06-12", "base": "USD", "currency": "EUR", "rate": "1.075"},
        {"date": "2025-06-12", "base": "USD", "currency": "JPY", "rate": "na"},
        {"date": "2025-06-12", "base": "USD", "currency": "THB", "rate": ""},
    ])

    cleaned_df = clean_exchange_rate_df(raw_data)
    result = cleaned_df.toPandas()

    assert not result.empty
    assert set(result.columns) == {"date", "base", "currency", "rate"}
    assert result["currency"].tolist() == ["EUR"]
    assert result["rate"].tolist() == [1.075]

    spark.stop()
