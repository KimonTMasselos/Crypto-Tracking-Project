from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, StructType, StructField
from pyspark.sql.functions import col, lit, to_date
# from pyspark.sql import functions as F
from datetime import datetime
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Data interval start date (YYYY-MM-DD)")

    args = parser.parse_args()

    reference_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    spark = (
        SparkSession.builder
        .appName("IcebergOnS3")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.526")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.crypto", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.crypto.type", "hadoop")     # We do hadoop to keep it simple
        .config("spark.sql.catalog.crypto.warehouse", "s3a://crypto-project-kimon/")
        .getOrCreate()
    )

    # Complete schema definition for coin prices
    coin_prices_schema = StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
    ])

    # Read the coin price data from S3
    df = spark.read.schema(coin_prices_schema).json(f"s3a://crypto-project-kimon/bronze/coin_price/{reference_date}/")

    # Create the "date" column and select only the relevant columns for the fact table
    df = df.withColumn("date", to_date(lit(reference_date), "yyyy-MM-dd")) \
            .select(col("id"), col("date"), col("price"), col("market_cap"), col("total_volume"))
    

    # Create the fact table if it doesn't exist
    coin_prices_DDL = """
    CREATE TABLE IF NOT EXISTS crypto.silver.fct_coin_price (
        id STRING,
        date DATE,
        price DOUBLE,
        market_cap DOUBLE,
        total_volume DOUBLE
    )
    USING iceberg
    PARTITIONED BY (date);
    """

    spark.sql(coin_prices_DDL)

    # Overwrite the partitions for the given date
    df.writeTo("crypto.silver.fct_coin_price").overwritePartitions()

    spark.stop()


if __name__ == "__main__":
    main()

