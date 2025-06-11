# multi-format-data-ingestion
Python scripts for generating, writing, parsing, and analyzing life insurance policy data in CSV, JSON, Parquet, ORC, Avro, and RecordIO formats. Demonstrates automated data creation, ingestion, and analysis of volume, velocity, and variety for each file type.

# Purpose
This repository provides end-to-end examples of creating, writing, and analyzing sample life insurance policy records in a wide range of big data and industry-standard file formats. The included Python scripts enable users to:

- Generate and save sample life insurance policy data to CSV, JSON, Apache Parquet, Apache ORC, Apache Avro, and RecordIO files.

- Parse and ingest data from each format, demonstrating best practices for reading and validating records.

- Analyze the properties of each dataset—measuring volume (file size, record count), velocity (records per second), and variety (schema, columns, field types).

The project is ideal for data engineers, analysts, and anyone working on data pipelines, format conversion, or benchmarking big data ingestion and analytics workflows.



# Usage

1. Install dependencies:
   pip install -r requirements.txt

2. Generate sample data:
   python scripts/write_data.py

3. Analyze data:
   python scripts/analyze_data.py

File outputs are in the /data directory.

# Explanation of the key packages:

- pandas — for handling CSV, JSON, and Parquet read/write

- pyarrow — for Parquet support in pandas

- pyorc — for reading and writing ORC files

- avro-python3 — for reading and writing Avro files

- mxnet — for RecordIO file support

- numpy — used with RecordIO for numeric arrays

- tabulate (optional) — for prettier CLI tables

- jsonschema (optional) — for JSON validation if desired