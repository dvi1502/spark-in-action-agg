"""
  Orders analytics.

  @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, functions as F)


def main(spark,filename):


    df = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load(filename)

    # Calculating the orders info using the dataframe API
    api_df = df\
        .groupBy(F.col("firstName"), F.col("lastName"), F.col("state")) \
        .agg(F.sum("quantity"), F.sum("revenue"), F.avg("revenue"))

    api_df.show(20)

    # Calculating the orders info using SparkSQL
    df.createOrReplaceTempView("orders")

    sql_query = """
        SELECT firstName,lastName,state,SUM(quantity),SUM(revenue),AVG(revenue) 
        FROM orders 
        GROUP BY firstName, lastName, state
    """
    sql_df = spark.sql(sql_query)
    sql_df.show(20)

if __name__ == "__main__":

    filename = "./data/orders/orders.csv"

    spark = SparkSession\
        .builder\
        .appName("Orders analytics") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark,filename)
    spark.stop()
