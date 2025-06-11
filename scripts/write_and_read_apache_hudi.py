import os
from pyspark.sql import SparkSession

DATA_DIR = "../data/insurance"
HOME = os.path.expanduser("~")
hudi_jar = f"{HOME}/hudi_jars/hudi-spark3-bundle_2.12-0.13.1.jar"
scala_jar = f"{HOME}/hudi_jars/scala-library-2.12.18.jar"

spark = SparkSession.builder \
    .appName("HudiInsuranceExample") \
    .config("spark.jars", f"{hudi_jar},{scala_jar}") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Create insurance DataFrame
sample_data = [
    ("LIP12345", "Alice Smith", 35, 100000, 800, 20, "Whole Life"),
    ("LIP67890", "Bob Jones", 42, 150000, 1200, 25, "Term Life"),
]

columns = ["policy_id", "holder_name", "age", "sum_assured", "premium", "term", "plan"]
df = spark.createDataFrame(sample_data, columns)

# Write to Hudi table
df.write.format("hudi") \
    .option("hoodie.table.name", "insurance_hudi_table") \
    .mode("overwrite") \
    .save(f"{DATA_DIR}/hudi_table")

# Read Hudi table
hudi_df = spark.read.format("hudi").load(f"{DATA_DIR}/hudi_table")
hudi_df.show()