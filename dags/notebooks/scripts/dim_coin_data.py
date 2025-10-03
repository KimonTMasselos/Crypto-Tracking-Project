from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, DateType, IntegerType, StructType, StructField
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
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

    # Complete schema definition for coins data
    coins_schema = StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("genesis_date", DateType(), True),
        StructField("hashing_algorithm", StringType(), True),
        StructField("country_origin", StringType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("description", StringType(), True),
        StructField("market_cap_rank", IntegerType(), True),
    ])

    # Read the raw coin data from S3
    df = spark.read.schema(coins_schema).json(f"s3a://crypto-project-kimon/bronze/coin_data/{reference_date}/")

    # Select only the relevant columns for the dimension table
    df_coins = df.select(
        col("id"), 
        col("symbol"), 
        col("name"), 
        col("genesis_date"), 
        col("hashing_algorithm"), 
        col("country_origin"), 
        col("description"), 
        col("market_cap_rank")
    )

    # Create the SCD table if it doesn't exist
    coins_scd_DDL = """
    CREATE TABLE IF NOT EXISTS crypto.silver.dim_coin_scd (
        id STRING,
        symbol STRING,
        name STRING,
        genesis_date DATE,
        hashing_algorithm STRING,
        country_origin STRING,
        description STRING,
        market_cap_rank INT,
        effective_start_date DATE,
        effective_end_date DATE
    )
    USING iceberg
    PARTITIONED BY (effective_start_date);
    """

    spark.sql(coins_scd_DDL)

    # Create a temporary view for the new data
    df_coins.createOrReplaceTempView("coins_now")

    # Get all the new coins as well as all the coins that had changes, 
    # we keep the previous effective_start_date in order to use it in the merge for the update below 
    # (it's the partition column so spark will know where to search
    spark.sql(f"""
        WITH coins_past AS (
            SELECT *
            FROM crypto.silver.dim_coin_scd
            WHERE effective_end_date IS NULL
        )
        SELECT  COALESCE(cp.id, cn.id) AS id,
                COALESCE(cp.symbol, cn.symbol) AS symbol,
                COALESCE(cp.name, cn.name) AS name,
                COALESCE(cp.genesis_date, cn.genesis_date) AS genesis_date,
                COALESCE(cp.hashing_algorithm, cn.hashing_algorithm) AS hashing_algorithm,
                COALESCE(cp.country_origin, cn.country_origin) AS country_origin,
                COALESCE(cp.description, cn.description) AS description,
                COALESCE(cp.market_cap_rank, cn.market_cap_rank) AS market_cap_rank,
                cp.effective_start_date AS prev_effective_start_date,
                DATE('{reference_date}') AS effective_start_date,
                NULL AS effective_end_date
        FROM coins_past cp
        FULL OUTER JOIN coins_now cn ON cn.id = cp.id
        WHERE COALESCE(cp.hashing_algorithm, 'xxx') <> cn.hashing_algorithm
                OR COALESCE(cp.description, 'xxx') <> cn.description
                OR COALESCE(cp.market_cap_rank, -1) <> cn.market_cap_rank
                OR cp.id IS NULL
    """).createOrReplaceTempView("new_rows")

    # Set the effective_end_date of the updated rows to reference_date - 1, since there is a new record
    spark.sql(f"""
        MERGE INTO crypto.silver.dim_coin_scd AS t
        USING new_rows AS s
            ON t.effective_start_date = s.prev_effective_start_date
                AND t.id = s.id
        WHEN MATCHED THEN
            UPDATE SET t.effective_end_date = DATE_SUB(DATE('{reference_date}'), 1)
    """)

    # Insert the new & updated rows
    spark.sql("""
        INSERT INTO crypto.silver.dim_coin_scd
        SELECT  id,
                symbol,
                name,
                genesis_date,
                hashing_algorithm,
                country_origin,
                description,
                market_cap_rank,
                effective_start_date,
                effective_end_date
        FROM new_rows
    """)

    # Explode categories into a separate table
    df_categories = df.withColumn("category", explode("categories")).select(col("id"), col("category"))

    # Create the SCD table if it doesn't exist
    coin_categories_scd_DDL = """
    CREATE TABLE IF NOT EXISTS crypto.silver.dim_coin_category_scd (
        id STRING,
        category STRING,
        effective_start_date DATE,
        effective_end_date DATE
    )
    USING iceberg
    PARTITIONED BY (effective_start_date);
    """

    spark.sql(coin_categories_scd_DDL)

    # Create a temporary view for the new data
    df_categories.createOrReplaceTempView("coin_categories_now")


    # Get all the new coin categories as well as all the coin categories that don't apply to the coin_id anymore, that way we keep the history of every coin's categories
    # we keep the previous effective_start_date in order to use it in the merge for the update below 
    # (it's the partition column so spark will know where to search)
    spark.sql(f"""
        WITH coin_categories_past AS (
            SELECT *
            FROM crypto.silver.dim_coin_category_scd
            WHERE effective_end_date IS NULL
        )
        SELECT  COALESCE(cp.id, cn.id) AS id,
                COALESCE(cp.category, cn.category) AS category,
                cp.effective_start_date AS prev_effective_start_date,
                DATE('{reference_date}') AS effective_start_date,
                NULL AS effective_end_date
        FROM coin_categories_past cp
        FULL OUTER JOIN coin_categories_now cn ON cn.id = cp.id AND cn.category = cp.category
        WHERE cp.id IS NULL
                OR cn.id IS NULL
    """).createOrReplaceTempView("new_rows")

    # Set the effective_end_date of the updated rows to reference_date - 1, since the coin doesn't fall under the category anymore
    spark.sql(f"""
        MERGE INTO crypto.silver.dim_coin_category_scd AS t
        USING new_rows AS s
            ON t.effective_start_date = s.prev_effective_start_date
                AND t.id = s.id
                AND t.category = s.category
        WHEN MATCHED THEN
            UPDATE SET t.effective_end_date = DATE_SUB(DATE('{reference_date}'), 1)
    """)

    # Insert the new rows
    spark.sql("""
        INSERT INTO crypto.silver.dim_coin_category_scd
        SELECT  id,
                category,
                effective_start_date,
                effective_end_date
        FROM new_rows
    """)

    spark.stop()


if __name__ == "__main__":
    main()

