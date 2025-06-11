"""
write_and_read_demo.py

Demonstrates how to use utils modules to write and read sample life insurance records in multiple formats.
"""

import os
import numpy as np

from utils.parquet_utils import write_parquet, read_parquet
from utils.avro_utils import write_avro, read_avro
from utils.orc_utils import write_orc, read_orc
from utils.recordio_utils import write_recordio, read_recordio

DATA_DIR = "../data/insurance"

sample_data = [
    {
        "policy_id": "LIP12345",
        "holder_name": "Alice Smith",
        "age": 35,
        "sum_assured": 100000,
        "premium": 800,
        "term": 20,
        "plan": "Whole Life"
    },
    {
        "policy_id": "LIP67890",
        "holder_name": "Bob Jones",
        "age": 42,
        "sum_assured": 150000,
        "premium": 1200,
        "term": 25,
        "plan": "Term Life"
    }
]

def main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    # Parquet
    parquet_file = os.path.join(DATA_DIR, "demo_sample.parquet")
    write_parquet(sample_data, parquet_file)
    df_parquet = read_parquet(parquet_file)
    print("Parquet read:\n", df_parquet)

    # Avro
    avro_file = os.path.join(DATA_DIR, "demo_sample.avro")
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
    write_avro(sample_data, avro_file, avro_schema)
    records_avro, schema_avro = read_avro(avro_file)
    print("Avro read:", records_avro)
    print("Avro schema:", schema_avro)

    # ORC
    orc_file = os.path.join(DATA_DIR, "demo_sample.orc")
    orc_schema = "struct<policy_id:string,holder_name:string,age:int,sum_assured:int,premium:int,term:int,plan:string>"
    tuples = [(
        rec["policy_id"], rec["holder_name"], rec["age"], rec["sum_assured"],
        rec["premium"], rec["term"], rec["plan"]
    ) for rec in sample_data]
    write_orc(tuples, orc_file, orc_schema)
    records_orc, schema_orc = read_orc(orc_file)
    print("ORC read:", records_orc)
    print("ORC schema:", schema_orc)

    # RecordIO
    recordio_file = os.path.join(DATA_DIR, "demo_sample.rec")
    arrays = [
        np.array([rec["age"], rec["sum_assured"], rec["premium"], rec["term"]], dtype=np.int64)
        for rec in sample_data
    ]
    write_recordio(arrays, recordio_file)
    arrays_read = read_recordio(recordio_file)
    print("RecordIO arrays read:", arrays_read)

if __name__ == "__main__":
    main()
