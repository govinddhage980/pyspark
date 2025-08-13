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

def create_my_sql_data_frame(dbname, table_name, environment, spark):
    """create spark dataframe from mysql"""
    data_frame = None
    mysql_hostname = 'db.pre-prod.flipkart.com'
    mysql_jdbcPort = 3306
    mysql_dbname = dbname
    mysql_username = 'root'
    mysql_password = 'my_pass'

    if environment == 'prod':
        mysql_hostname = 'db.pre.flipkart.com'
        mysql_jdbcPort = 3306
        mysql_dbname = dbname
        mysql_username = 'dev'
        mysql_password = 'pass_2'

    mysql_connectionProperties = {
        "user": mysql_username,
        "password": mysql_password,
        "driver": "com.mysql.jdbc.Driver"
    }

    mysql_jdbc_url = "jdbc:mysql://{0}:{1}/{2}?DateTimeBehavior".format(mysql_hostname,
                                                                                          mysql_jdbcPort,
                                                                                          mysql_dbname)
    data_frame = spark.read.jdbc(url=mysql_jdbc_url, table=table_name, properties=mysql_connectionProperties)

    return data_frame

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

