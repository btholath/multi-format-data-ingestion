from pyspark.sql import SparkSession

DATA_DIR = "../data/insurance"

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaInsuranceExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# Create insurance DataFrame
sample_data = [
    ("LIP12345", "Alice Smith", 35, 100000, 800, 20, "Whole Life"),
    ("LIP67890", "Bob Jones", 42, 150000, 1200, 25, "Term Life"),
]

columns = ["policy_id", "holder_name", "age", "sum_assured", "premium", "term", "plan"]
df = spark.createDataFrame(sample_data, columns)

# Write to Delta table
df.write.format("delta").mode("overwrite").save(f"{DATA_DIR}/delta_table")

# Read Delta table
delta_df = spark.read.format("delta").load(f"{DATA_DIR}/delta_table")
delta_df.show()