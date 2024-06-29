import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list, struct, count, sum, lit
from pyspark.sql.types import DateType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Healthcare Data Analysis") \
    .getOrCreate()

# Load the data
file_path = "/home/coder/Sachin/healthcare_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Data Cleaning
df_cleaned = df.dropna()
df_cleaned = df_cleaned.withColumn("visit_date", col("visit_date").cast(DateType()))

# Patient Visit History Aggregation
patient_visit_history_df = df_cleaned.groupBy("patient_id") \
    .agg(concat_ws(", ", collect_list(concat_ws("|", col("visit_date").cast("string"), col("diagnosis_description"), col("treatment_description")))).alias("visit_history"))

# Diagnosis Analysis Aggregation
diagnosis_analysis_df = df_cleaned.groupBy("diagnosis_description") \
    .agg(count("patient_id").alias("patient_count"), sum("cost").alias("total_cost"))

# Treatment Effectiveness Analysis
treatment_effectiveness_df = df_cleaned.groupBy("treatment_description") \
    .agg(count("patient_id").alias("patient_count"), sum("cost").alias("total_cost"))

# Output the results
output_path = "/home/coder/Sachin/results"
patient_visit_history_df.repartition(1).write.csv(os.path.join(output_path, "patient_visit_history"), mode="overwrite", header=True)
diagnosis_analysis_df.repartition(1).write.csv(os.path.join(output_path, "diagnosis_analysis"), mode="overwrite", header=True)
treatment_effectiveness_df.repartition(1).write.csv(os.path.join(output_path, "treatment_effectiveness"), mode="overwrite", header=True)

# Print results
print("Patient Visit History Aggregation:")
patient_visit_history_df.show(truncate=False)

print("Diagnosis Analysis:")
diagnosis_analysis_df.show(truncate=False)

print("Treatment Effectiveness Analysis:")
treatment_effectiveness_df.show(truncate=False)

# Stop SparkSession
spark.stop()