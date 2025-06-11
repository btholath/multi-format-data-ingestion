# tests/test_parquet_utils.py

import os
import tempfile
from utils.parquet_utils import write_parquet, read_parquet

def test_write_and_read_parquet():
    test_data = [
        {"policy_id": "P1", "holder_name": "User One", "age": 45, "sum_assured": 80000, "premium": 900, "term": 15, "plan": "Term"}
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_file = os.path.join(tmpdir, "test.parquet")
        write_parquet(test_data, parquet_file)
        df = read_parquet(parquet_file)
        assert len(df) == 1
        assert df.iloc[0]['holder_name'] == "User One"
        print("test_write_and_read_parquet PASSED")

if __name__ == "__main__":
    test_write_and_read_parquet()
