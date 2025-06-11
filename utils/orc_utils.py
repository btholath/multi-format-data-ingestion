"""
orc_utils.py

Utility functions for reading and writing ORC files using pyorc.
"""

import pyorc

def write_orc(data, filename, schema_str):
    """Write a list of tuples to an ORC file with the given schema string."""
    with open(filename, "wb") as f:
        writer = pyorc.Writer(f, schema_str)
        for rec in data:
            writer.write(rec)
        writer.close()
    print(f"Wrote {filename}")

def read_orc(filename):
    """Read an ORC file and return a list of tuples and the schema."""
    with open(filename, "rb") as f:
        reader = pyorc.Reader(f)
        schema = reader.schema
        records = [rec for rec in reader]
    return records, schema
