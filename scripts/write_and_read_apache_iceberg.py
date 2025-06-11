from pyspark.sql import SparkSession

DATA_DIR = "../data/insurance"

# Initialize Spark session with Iceberg support
spark = SparkSession.builder \
    .appName("IcebergInsuranceExample") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()

# Create insurance DataFrame
sample_data = [
    ("LIP12345", "Alice Smith", 35, 100000, 800, 20, "Whole Life"),
    ("LIP67890", "Bob Jones", 42, 150000, 1200, 25, "Term Life"),
]

columns = ["policy_id", "holder_name", "age", "sum_assured", "premium", "term", "plan"]
df = spark.createDataFrame(sample_data, columns)

# Write to Iceberg table
df.write.format("iceberg").mode("overwrite").save(f"{DATA_DIR}/iceberg_table")

# Read Iceberg table
iceberg_df = spark.read.format("iceberg").load(f"{DATA_DIR}/iceberg_table")
iceberg_df.show()