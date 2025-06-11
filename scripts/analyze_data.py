"""
analyze_data.py

Purpose: Analyze life insurance policy files (CSV, JSON, Parquet, ORC, Avro, RecordIO) in the data/ directory for Volume, Velocity, and Variety.
"""

import os
import time
import pandas as pd
import json

DATA_DIR = "../data/insurance"

def analyze_csv(filename):
    print("="*10, "CSV", "="*10)
    size_bytes = os.path.getsize(filename)
    start = time.time()
    df = pd.read_csv(filename)
    elapsed = time.time() - start
    num_records = len(df)
    print(f"File size: {size_bytes} bytes")
    print(f"Number of records: {num_records}")
    velocity = num_records / elapsed if elapsed > 0 else 0
    print(f"Processing velocity: {velocity:.2f} records/sec")
    print("Schema and data types:")
    print(df.dtypes)
    print("Columns:", df.columns.tolist())
    print()

def analyze_json(filename):
    print("="*10, "JSON", "="*10)
    size_bytes = os.path.getsize(filename)
    start = time.time()
    with open(filename) as f:
        data = json.load(f)
    elapsed = time.time() - start
    num_records = len(data)
    print(f"File size: {size_bytes} bytes")
    print(f"Number of records: {num_records}")
    velocity = num_records / elapsed if elapsed > 0 else 0
    print(f"Processing velocity: {velocity:.2f} records/sec")
    print("Fields in first record:", list(data[0].keys()) if data else "No records")
    print("Sample record:", data[0] if data else "No records")
    print()

def analyze_parquet(filename):
    print("="*10, "PARQUET", "="*10)
    size_bytes = os.path.getsize(filename)
    start = time.time()
    df = pd.read_parquet(filename)
    elapsed = time.time() - start
    num_records = len(df)
    print(f"File size: {size_bytes} bytes")
    print(f"Number of records: {num_records}")
    velocity = num_records / elapsed if elapsed > 0 else 0
    print(f"Processing velocity: {velocity:.2f} records/sec")
    print("Schema and data types:")
    print(df.dtypes)
    print("Columns:", df.columns.tolist())
    print()

def analyze_orc(filename):
    import pyorc
    print("="*10, "ORC", "="*10)
    size_bytes = os.path.getsize(filename)
    record_count = 0
    start = time.time()
    with open(filename, "rb") as f:
        reader = pyorc.Reader(f)
        schema = reader.schema
        for _ in reader:
            record_count += 1
    elapsed = time.time() - start
    print(f"File size: {size_bytes} bytes")
    print(f"Number of records: {record_count}")
    velocity = record_count / elapsed if elapsed > 0 else 0
    print(f"Processing velocity: {velocity:.2f} records/sec")
    print("ORC schema:", schema)
    print()

def analyze_avro(filename):
    import avro.datafile
    import avro.io
    import json
    print("="*10, "AVRO", "="*10)
    size_bytes = os.path.getsize(filename)
    record_count = 0
    start = time.time()
    with open(filename, "rb") as fo:
        reader = avro.datafile.DataFileReader(fo, avro.io.DatumReader())
        schema = reader.GetMeta('avro.schema').decode()
        for _ in reader:
            record_count += 1
        reader.close()
    elapsed = time.time() - start
    print(f"File size: {size_bytes} bytes")
    print(f"Number of records: {record_count}")
    velocity = record_count / elapsed if elapsed > 0 else 0
    print(f"Processing velocity: {velocity:.2f} records/sec")
    print("Avro schema:", json.loads(schema))  # Pretty-print
    print()

def analyze_recordio(filename):
    import mxnet as mx
    import numpy as np
    print("="*10, "RecordIO", "="*10)
    size_bytes = os.path.getsize(filename)
    record_count = 0
    array_shapes = []
    start = time.time()
    reader = mx.recordio.MXRecordIO(filename, 'r')
    while True:
        item = reader.read()
        if item is None:
            break
        _, data = mx.recordio.unpack(item)
        arr = np.frombuffer(data, dtype=np.int64)
        array_shapes.append(arr.shape)
        record_count += 1
    elapsed = time.time() - start
    velocity = record_count / elapsed if elapsed > 0 else 0
    print(f"File size: {size_bytes} bytes")
    print(f"Number of records: {record_count}")
    print(f"Processing velocity: {velocity:.2f} records/sec")
    print("Array shapes (first 3):", array_shapes[:3] if array_shapes else "No data")
    print()

if __name__ == "__main__":
    analyze_csv(os.path.join(DATA_DIR, "sample.csv"))
    analyze_json(os.path.join(DATA_DIR, "sample.json"))
    analyze_parquet(os.path.join(DATA_DIR, "sample.parquet"))
    analyze_orc(os.path.join(DATA_DIR, "sample.orc"))
    analyze_avro(os.path.join(DATA_DIR, "sample.avro"))
    analyze_recordio(os.path.join(DATA_DIR, "sample.rec"))
