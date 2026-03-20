# notebooks/04_export_report.py
# ============================================
# Export Gold Flagged Transactions to CSV
# ============================================

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("FinCrime_Export") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint",
            f"http://{os.getenv('MINIO_ENDPOINT')}") \
    .config("spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read flagged gold data
df = spark.read.parquet(
    "s3a://gold-curated/flagged_transactions/"
)

# Export to local CSV for reporting
df.orderBy("risk_score", ascending=False) \
  .toPandas() \
  .to_csv("data/flagged_transactions_report.csv", index=False)

print(f"✅ Exported {df.count()} flagged transactions")
print("📁 Saved to: data/flagged_transactions_report.csv")

spark.stop()
