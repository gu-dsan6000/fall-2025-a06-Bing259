from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand
import os
import sys
import csv

def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("Problem1_LogLevelDistribution")

        # Cluster Configuration
        .master(master_url)  # Connect to Spark cluster

        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster

        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .master(master_url).getOrCreate()
    )

    return spark


def main():
    # 1. Determine Spark master URL
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        mip = os.getenv("MASTER_PRIVATE_IP")
        if not mip:
            return 1
        master_url = f"spark://{mip}:7077"

    print(f"Connecting to Spark master at: {master_url}")

    spark = create_spark_session(master_url)

    # 2. Read all log files from S3
    paths = []
    with open("all_logs.txt") as f:
        for line in f:
            key = line.strip()
            if key:
                paths.append(f"s3a://bn259-assignment-spark-cluster-logs/{key}")

    logs_df = spark.read.text(paths)

    # 3. Parse timestamp and level
    parsed_logs = logs_df.select(
        regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias(
            "timestamp"
        ),
        regexp_extract("value", r"(INFO|WARN|ERROR|DEBUG)", 1).alias("level"),
        col("value").alias("message"),
    ).filter(col("level") != "")

    # 4. Count levels and write CSV
    log_counts = parsed_logs.groupBy("level").count()
    counts_rows = log_counts.collect()
    with open("data/output/problem1_counts.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["level", "count"])
        for row in counts_rows:
            writer.writerow([row["level"], row["count"]])

    # 5. Randomly sample 10 rows
    sample_logs = parsed_logs.orderBy(rand()).limit(10)
    sample_rows = sample_logs.collect()
    with open("data/output/problem1_sample.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "level", "message"])
        for row in sample_rows:
            writer.writerow([row["timestamp"], row["level"], row["message"]])

    # 6. Write summary
    total_lines = logs_df.count()
    lines_with_level = parsed_logs.count()
    unique_levels = parsed_logs.select("level").distinct().count()
    level_distribution = log_counts.collect()

    with open("data/output/problem1_summary.txt", "w") as f:
        f.write(f"Total log lines processed: {total_lines}\n")
        f.write(f"Total lines with log levels: {lines_with_level}\n")
        f.write(f"Unique log levels found: {unique_levels}\n\n")
        f.write("Log level distribution:\n")
        for row in level_distribution:
            pct = (row["count"] / lines_with_level * 100) if lines_with_level else 0
            f.write(f"  {row['level']:5} : {row['count']:10} ({pct:6.2f}%)\n")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
