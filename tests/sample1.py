# Databricks notebook source
from pyspark.sql.functions import col, expr, avg

# COMMAND ----------

data = [
    [2021, "John", "Albany", "M", 42],
    [2022, "Jane", "Buffalo", "F", 36],
    [2023, "Doe", "Syracuse", "M", 28]
]
columns = ["Year", "First_Name", "County", "Sex", "Count"]

df = spark.createDataFrame(data, schema="Year int, First_Name STRING, County STRING, Sex STRING, Count int")
display(df)

# COMMAND ----------

# Filter rows where the count is greater than 30
filtered_df = df.filter(col("Count") > 30)

# Add a new column that calculates the age in 2025
transformed_df = filtered_df.withColumn("Age_in_2025", expr("2025 - Year"))

# Group by 'Sex' and calculate the average count
grouped_df = transformed_df.groupBy("Sex").agg(avg("Count").alias("Average_Count"))

# Display the final DataFrame
display(grouped_df)