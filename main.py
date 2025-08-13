from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pytz
from pyspark.sql.functions import count
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from datetime import datetime
import pytz

def transformed_data(df_cust, df_orders):
    # Select required columns from customer data
    df_cust_final = df_cust.select(
        col("empid").alias("cust_id"),
        col("fname").alias("first_name"),
        col("lname").alias("last_name"),
        col("loc").alias("cust_loc")
    )

    # Select required columns from orders data
    df_orders_final = df_orders.select(
        col("id").alias("cust_id"),
        col("order_id"),
        col("product_name"),
        col("loc").alias("order_loc"),
        col("mob_no").alias("order_mob_no")
    )

    # Join customer & order data
    final_df = df_cust_final.join(
        df_orders_final,
        df_cust_final.cust_id == df_orders_final.cust_id,
        "inner"
    ).select(
        df_cust_final.cust_id,
        df_cust_final.first_name,
        df_cust_final.last_name,
        df_cust_final.cust_loc,
        df_orders_final.order_id,
        df_orders_final.product_name,
        df_orders_final.order_loc,
        df_orders_final.order_mob_no
    ).orderBy(df_cust_final.cust_id)

    # Group by location and count
    final_output = final_df.groupBy("cust_loc").agg(count("*").alias("total_orders"))

    return final_output

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .getOrCreate()

    # Read customer data
    df_cust = spark.read.csv(
        "/Volumes/batch22/project/customer/emp_data_ (2).csv",
        header=True,
        inferSchema=True
    )

    # Read orders data
    df_orders = spark.read.csv(
        "/Volumes/batch22/project/odres/orders.csv",
        header=True,
        inferSchema=True
    )

    # Transform data
    result_df = transformed_data(df_cust, df_orders)

    # Show results
    result_df.show()

if __name__ == "__main__":
    main()
