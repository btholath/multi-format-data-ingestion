"""
write_data.py

Purpose: Create sample life insurance policy records and save them in CSV, JSON, Parquet, ORC, Avro, and RecordIO formats in the data/ directory.
"""

import os
import pandas as pd
import json

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

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def write_csv():
    df = pd.DataFrame(sample_data)
    df.to_csv(os.path.join(DATA_DIR, "sample.csv"), index=False)
    print("Wrote sample.csv")

def write_json():
    with open(os.path.join(DATA_DIR, "sample.json"), "w") as f:
        json.dump(sample_data, f, indent=2)
    print("Wrote sample.json")

def write_parquet():
    df = pd.DataFrame(sample_data)
    df.to_parquet(os.path.join(DATA_DIR, "sample.parquet"))
    print("Wrote sample.parquet")

def write_orc():
    import pyorc
    schema = "struct<policy_id:string,holder_name:string,age:int,sum_assured:int,premium:int,term:int,plan:string>"
    tuples = [(
        rec["policy_id"], rec["holder_name"], rec["age"], rec["sum_assured"],
        rec["premium"], rec["term"], rec["plan"]
    ) for rec in sample_data]
    with open(os.path.join(DATA_DIR, "sample.orc"), "wb") as f:
        writer = pyorc.Writer(f, schema)
        for rec in tuples:
            writer.write(rec)
        writer.close()
    print("Wrote sample.orc")

def write_avro():
    import avro.schema, avro.datafile, avro.io
    schema_str = """
    {
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
    """
    schema = avro.schema.parse(schema_str)
    with open(os.path.join(DATA_DIR, "sample.avro"), "wb") as out:
        writer = avro.datafile.DataFileWriter(out, avro.io.DatumWriter(), schema)
        for rec in sample_data:
            writer.append(rec)
        writer.close()
    print("Wrote sample.avro")

def write_recordio():
    import mxnet as mx
    import numpy as np
    arrays = [
        np.array([rec["age"], rec["sum_assured"], rec["premium"], rec["term"]], dtype=np.int64)
        for rec in sample_data
    ]
    writer = mx.recordio.MXRecordIO(os.path.join(DATA_DIR, "sample.rec"), 'w')
    for arr in arrays:
        s = mx.recordio.pack(mx.recordio.IRHeader(0, 0, 0, 0), arr.tobytes())
        writer.write(s)
    writer.close()
    print("Wrote sample.rec")

if __name__ == "__main__":
    ensure_data_dir()
    write_csv()
    write_json()
    write_parquet()
    write_orc()
    write_avro()
    write_recordio()
    print("All data files written to data/")
