import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract,
    col,
    input_file_name,
    to_timestamp,
    when,
    min as spark_min,
    max as spark_max,
)

def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("Problem2_ClusterUsage")

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


def build_timeline(spark):
    # 1. Read all log files from S3
    paths = []
    with open("all_logs.txt") as f:
        for line in f:
            key = line.strip()
            if key:
                paths.append(f"s3a://bn259-assignment-spark-cluster-logs/{key}")

    logs_df = spark.read.text(paths)

    # Parse file path and timestamp
    logs_df = logs_df.withColumn("file_path", input_file_name())

    # Extract timestamp from log line using regex
    logs_df = logs_df.withColumn(
        "timestamp_str",
        regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1),
    )

    # Only parse non-empty strings, empty strings become NULL
    logs_df = logs_df.withColumn(
        "timestamp",
        when(col("timestamp_str") != "", to_timestamp("timestamp_str", "yy/MM/dd HH:mm:ss"))
    )

    # Extract application_id
    logs_df = logs_df.withColumn(
        "application_id",
        regexp_extract("file_path", r"(application_\d+_\d+)", 1),
    )

    # Filter out rows without timestamp or application_id
    logs_df = logs_df.filter(col("timestamp").isNotNull())
    logs_df = logs_df.filter(col("application_id") != "")

    # Extract cluster_id and app_number from application_id
    logs_df = logs_df.withColumn(
        "cluster_id",
        regexp_extract("application_id", r"application_(\d+)_\d+", 1),
    )
    logs_df = logs_df.withColumn(
        "app_number",
        regexp_extract("application_id", r"application_\d+_(\d+)", 1),
    )

    # 4. Calculate start and end times for each application
    timeline_df = (
        logs_df.groupBy("cluster_id", "application_id", "app_number")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time"),
        )
    )

    timeline_pd = timeline_df.toPandas()
    return timeline_pd


def make_outputs(timeline_pd):
    os.makedirs("data/output", exist_ok=True)

    # Save timeline CSV
    timeline_pd.to_csv("data/output/problem2_timeline.csv", index=False)

    # Save cluster summary
    summary_df = (
        timeline_pd.groupby("cluster_id")
        .agg(
            num_applications=("application_id", "count"),
            cluster_first_app=("start_time", "min"),
            cluster_last_app=("end_time", "max"),
        )
        .reset_index()
    )
    summary_df.to_csv("data/output/problem2_cluster_summary.csv", index=False)

    # Save statistics
    total_clusters = summary_df.shape[0]
    total_apps = timeline_pd.shape[0]
    avg_apps_per_cluster = total_apps / total_clusters if total_clusters else 0

    most_used = summary_df.sort_values("num_applications", ascending=False)

    with open("data/output/problem2_stats.txt", "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for _, row in most_used.iterrows():
            f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

    # 4) Bar chart: Number of applications per cluster
    plt.figure(figsize=(8, 6))
    sns.barplot(x="cluster_id", y="num_applications", hue="cluster_id", data=most_used)
    for i, row in most_used.reset_index(drop=True).iterrows():
        plt.text(i, row["num_applications"], str(row["num_applications"]),
                 ha="center", va="bottom")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    plt.title("Applications per Cluster")
    plt.tight_layout()
    plt.savefig("data/output/problem2_bar_chart.png")
    plt.close()

    # 5) Duration distribution of the largest cluster (histogram + KDE, log x-axis)
    largest_cluster = most_used.iloc[0]["cluster_id"]
    cluster_jobs = timeline_pd[timeline_pd["cluster_id"] == largest_cluster].copy()

    cluster_jobs["start_time"] = pd.to_datetime(cluster_jobs["start_time"])
    cluster_jobs["end_time"] = pd.to_datetime(cluster_jobs["end_time"])
    cluster_jobs["duration_sec"] = (
        cluster_jobs["end_time"] - cluster_jobs["start_time"]
    ).dt.total_seconds()

    cluster_jobs = cluster_jobs[cluster_jobs["duration_sec"] > 0]

    plt.figure(figsize=(8, 6))
    sns.histplot(cluster_jobs["duration_sec"], bins=30, kde=True, log_scale=True)
    plt.xlabel("Job Duration (seconds, log scale)")
    plt.ylabel("Count")
    plt.title(
        f"Job Duration Distribution - Cluster {largest_cluster} (n={len(cluster_jobs)})"
    )
    plt.tight_layout()
    plt.savefig("data/output/problem2_density_plot.png")
    plt.close()


def main():
    # Determine Spark master URL
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        mip = os.getenv("MASTER_PRIVATE_IP")
        if not mip:
            return 1
        master_url = f"spark://{mip}:7077"

    print(f"Connecting to Spark master at: {master_url}")

    spark = create_spark_session(master_url)

    timeline_pd = build_timeline(spark)
    make_outputs(timeline_pd)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())