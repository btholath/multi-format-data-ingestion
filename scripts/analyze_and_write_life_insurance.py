import os
import time
import pandas as pd
import json

# --------- 1. SAMPLE DATA (life insurance policies) ---------
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

# --------- 2. FILE CREATION FUNCTIONS ---------
def write_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

def write_json(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

def write_parquet(data, filename):
    df = pd.DataFrame(data)
    df.to_parquet(filename)

def write_orc(data, filename):
    import pyorc
    schema = "struct<policy_id:string,holder_name:string,age:int,sum_assured:int,premium:int,term:int,plan:string>"
    tuples = [(
        rec["policy_id"], rec["holder_name"], rec["age"], rec["sum_assured"],
        rec["premium"], rec["term"], rec["plan"]
    ) for rec in data]
    with open(filename, "wb") as f:
        writer = pyorc.Writer(f, schema)
        for rec in tuples:
            writer.write(rec)
        writer.close()

def write_avro(data, filename):
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
    with open(filename, "wb") as out:
        writer = avro.datafile.DataFileWriter(out, avro.io.DatumWriter(), schema)
        for rec in data:
            writer.append(rec)
        writer.close()

def write_recordio(data, filename):
    import mxnet as mx
    import numpy as np
    # Flatten each record as a tuple of fields for simplicity (just numerics for demo)
    arrays = [
        np.array([rec["age"], rec["sum_assured"], rec["premium"], rec["term"]], dtype=np.int64)
        for rec in data
    ]
    writer = mx.recordio.MXRecordIO(filename, 'w')
    for arr in arrays:
        s = mx.recordio.pack(mx.recordio.IRHeader(0, 0, 0, 0), arr.tobytes())
        writer.write(s)
    writer.close()

# --------- 3. ANALYSIS FUNCTIONS (as above, same as previous script) ---------
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
    print("="*10, "AVRO", "="*10)
    size_bytes = os.path.getsize(filename)
    record_count = 0
    start = time.time()
    with open(filename, "rb") as fo:
        reader = avro.datafile.DataFileReader(fo, avro.io.DatumReader())
        schema = reader.datum_reader.writers_schema
        for _ in reader:
            record_count += 1
        reader.close()
    elapsed = time.time() - start
    print(f"File size: {size_bytes} bytes")
    print(f"Number of records: {record_count}")
    velocity = record_count / elapsed if elapsed > 0 else 0
    print(f"Processing velocity: {velocity:.2f} records/sec")
    print("Avro schema:", schema)
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
        arr = np.frombuffer(data, dtype=np.int64)  # Change dtype as per your data
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
    # 1. Write sample records in all formats
    write_csv(sample_data, "../data/insurance/sample.csv")
    write_json(sample_data, "../data/insurance/sample.json")
    write_parquet(sample_data, "../data/insurance/sample.parquet")
    write_orc(sample_data, "../data/insurance/sample.orc")
    write_avro(sample_data, "../data/insurance/sample.avro")
    write_recordio(sample_data, "../data/insurance/sample.rec")
    # 2. Analyze each file
    analyze_csv("../data/insurance/sample.csv")
    analyze_json("../data/insurance/sample.json")
    analyze_parquet("../data/insurance/sample.parquet")
    analyze_orc("../data/insurance/sample.orc")
    analyze_avro("../data/insurance/sample.avro")
    analyze_recordio("../data/insurance/sample.rec")
