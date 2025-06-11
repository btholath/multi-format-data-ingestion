"""
parquet_utils.py

Utility functions for reading and writing Parquet files using pandas.
"""

import pandas as pd

def write_parquet(data, filename):
    """Write a list of dictionaries to a Parquet file."""
    df = pd.DataFrame(data)
    df.to_parquet(filename)
    print(f"Wrote {filename}")

def read_parquet(filename):
    """Read a Parquet file and return as a pandas DataFrame."""
    df = pd.read_parquet(filename)
    return df
