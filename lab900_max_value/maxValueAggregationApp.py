"""
  Max Value Aggregation

  @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, functions as F)


def main(spark):

    filename = "./data/misc/courses.csv"

    raw_df = spark.read\
        .format("csv") \
        .option("header", True) \
        .option("sep", "|") \
        .load(filename)

    # Shows at most 20 rows from the dataframe
    raw_df.show(20)

    # Performs the aggregation, grouping on columns id, batch_id, and
    # session_name
    max_values_df = raw_df.select("*") \
        .groupBy(F.col("id"), F.col("batch_id"), F.col("session_name")) \
        .agg(F.max("time"))

    max_values_df.show(5)

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("Aggregates max values") \
        .master("local[*]")\
        .getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
