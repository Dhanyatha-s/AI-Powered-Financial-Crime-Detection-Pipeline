# READ AND VALIDATE THE BRONZE LAYER
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when
from dotenv import load_dotenv

load_dotenv()

# Intilialize the Spark Session with MinIo configurations
# This configuration allows Spark to read from and write to the MinIO bucket where the bronze layer data is stored.
spark = SparkSession.builder \
    .appName("FinancialCrime_Bronze_validator")\
    .config(
    "spark.jars.packages",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")\
    .config("spark.hadoop.fs.s3a.endpoint",
            f"http://{os.getenv('MINIO_ENDPOINT')}")\
    .config("spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY"))\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .getOrCreate() # Create a Spark session with the necessary configurations to connect to MinIO

spark.sparkContext.setLogLevel("ERROR") # Set the log level to ERROR to reduce verbosity, when running the notebook, this will help us focus on any potential issues with the data validation process.
print("Spark Session initialized successfully with MinIO configurations.")
print("=="*20)
print("Reading data from the bronze layer...")
print("=="*20)
# read the data from the bronze layer
df = spark.read.options("multiline","true")\
.json("s3a://brown-raw/transactions/transactions_*.json") # Read the JSON files from the specified MinIO bucket and path, where the bronze layer data is stored.
print("Data read successfully from the bronze layer.")
print("=="*20)
print("Displaying the schema of the DataFrame:")
print("=="*20)
print(f"Total Records : {df.count()}")
print(f"Columns : {len(df.columns)}")
df.printSchema() # Print the schema of the DataFrame to understand the structure of the data and the data types of each column.


# Sapmle records
print("--"*20)
print("Sample Records from the DataFrame:")
print("--"*20)
df.show(5, truncate=True) # Display the first 5 rows of the DataFrame to get a quick look at the data.

# Data Quality Checks
print("--"*20)
print("Performing Data Quality Checks:")
print("--"*20)
# Check for null values in each column
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.colums])
null_counts.show() # Show the count of null values in each column to identify any missing data issues.

# Basic Statistics
print("--"*20)  
print("Basic Statistics of the DataFrame:")
print("--"*20)
df.describe().show() # Show basic statistics (count, mean, stddev, min, max) for numeric columns in the DataFrame to understand the distribution of the data.
df.select("amount").summary().show() # Show summary statistics specifically for the "amount" column to analyze the distribution of transaction amounts, which can be crucial for financial crime detection.

# Transaction Type Distribution
print("--"*20)
print("Distribution of Transaction Types:")
print("--"*20)
df.groupBy("transaction_type").count().orderBy("count", ascending=False).show() # Group the data by "transaction_type" and count the number of occurrences of each type to understand the distribution of different transaction types in the dataset.

# Falg potentially suspicious transactions based on amount
print("--"*20)
print("Flagging Potentially Suspicious Transactions Based on Amount:")
print("--"*20)
df.filter(col("amount")>9000)\
.select("transaction_id","amount","timestamp", "transaction_type")\
.show(10) # Filter the DataFrame to show transactions with an amount greater than 9000, which may be considered potentially suspicious, and display relevant details for these transactions.


spark.stop()
print("Spark Session stopped. Data validation process completed.")
