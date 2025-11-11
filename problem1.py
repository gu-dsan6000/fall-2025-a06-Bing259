from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand
import glob
import os
import shutil

spark = SparkSession.builder \
    .appName("LogLevelDistribution") \
    .getOrCreate()

# Load log files
log_files = glob.glob("data/sample/application_1485248649253_0052/*.log")
logs_df = spark.read.text(log_files)

# Parse timestamp, level, message
parsed_logs = logs_df.select(
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('level'),
    col('value').alias('message')
).filter(col("level").isNotNull() & (col("level") != ""))

# Function to write a single CSV file from a DataFrame
def write_single_csv(df, output_path):
    temp_dir = output_path + "_tmp"
    df.coalesce(1).write.option("header", "true").csv(temp_dir)

    # Search for the part file
    part_file = None
    for f in os.listdir(temp_dir):
        if f.startswith("part-") and f.endswith(".csv"):
            part_file = os.path.join(temp_dir, f)
            break

    # Move the part file to the final output path
    if part_file is not None:
        shutil.move(part_file, output_path)
    shutil.rmtree(temp_dir)

# Count log levels
log_counts = parsed_logs.groupBy("level").count()
write_single_csv(log_counts, "data/output/problem1_counts.csv")

# Randomly sample 10 logs
sample_logs = parsed_logs.orderBy(rand()).limit(10)
write_single_csv(sample_logs, "data/output/problem1_sample.csv")

# Summary
total_lines = logs_df.count()
lines_with_level = parsed_logs.filter(col("level").isNotNull()).count()
unique_levels = parsed_logs.select("level").distinct().count()
level_distribution = log_counts.collect()

with open("data/output/problem1_summary.txt", "w") as f:
    f.write(f"Total log lines processed: {total_lines}\n")
    f.write(f"Total lines with log levels: {lines_with_level}\n")
    f.write(f"Unique log levels found: {unique_levels}\n\n")
    f.write("Log level distribution:\n")
    for row in level_distribution:
        pct = (row['count'] / lines_with_level * 100) if lines_with_level else 0
        f.write(f"  {row['level']:5} : {row['count']:10} ({pct:6.2f}%)\n")

spark.stop()
