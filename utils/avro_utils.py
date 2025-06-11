"""
avro_utils.py

Utility functions for reading and writing Avro files.
"""

import avro.schema
import avro.datafile
import avro.io
import json

def write_avro(data, filename, schema_dict):
    """Write a list of dictionaries to an Avro file using the given schema dict."""
    schema = avro.schema.parse(json.dumps(schema_dict))
    with open(filename, "wb") as out:
        writer = avro.datafile.DataFileWriter(out, avro.io.DatumWriter(), schema)
        for rec in data:
            writer.append(rec)
        writer.close()
    print(f"Wrote {filename}")

def read_avro(filename):
    """Read an Avro file and return a list of dictionaries and the schema."""
    with open(filename, "rb") as fo:
        reader = avro.datafile.DataFileReader(fo, avro.io.DatumReader())
        records = [rec for rec in reader]
        schema = reader.GetMeta('avro.schema').decode()
        reader.close()
    return records, json.loads(schema)
