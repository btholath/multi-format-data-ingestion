"""
test_avro_utils.py

Unit test for Avro utility functions.
"""

import os
import tempfile
from utils.avro_utils import write_avro, read_avro

def test_write_and_read_avro():
    test_data = [
        {"policy_id": "P1", "holder_name": "Test User", "age": 30, "sum_assured": 50000, "premium": 600, "term": 10, "plan": "Whole Life"}
    ]
    avro_schema = {
        "type": "record",
        "name": "LifePolicy",
        "fields": [
            {"name": "policy_id", "type": "string"},
            {"name": "holder_name", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "sum_assured", "type": "int"},
            {"name": "premium", "type": "int"},
            {"name": "term", "type": "int"},
            {"name": "plan", "type": "string"}
        ]
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        avro_file = os.path.join(tmpdir, "test.avro")
        write_avro(test_data, avro_file, avro_schema)
        records, schema = read_avro(avro_file)
        assert len(records) == 1
        assert records[0]["holder_name"] == "Test User"
        assert schema["name"] == "LifePolicy"
        print("test_write_and_read_avro PASSED")

if __name__ == "__main__":
    test_write_and_read_avro()
