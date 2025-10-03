from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import json
import time

from include.variables import BUCKET_NAME, coin_ids

# BUCKET_NAME = "crypto-project-kimon"
# coin_ids = ['bitcoin', 'ethereum']


aws_conn = BaseHook.get_connection("AwsS3Bucket")
aws_access_key = aws_conn.login      # AWS access key
aws_secret_key = aws_conn.password   # AWS secret key


def upload_json_to_s3(s3_hook, bucket, key, data):
    s3_hook.load_string(
        string_data=json.dumps(json.loads(data)),
        key=key,
        bucket_name=bucket,
        replace=True
    )

def filter_price_json(data: dict) -> dict:
    return {
        "id": data.get("id"),
        "symbol": data.get("symbol"),
        "name": data.get("name"),
        "price": data.get("market_data", {})
                            .get("current_price", {})
                            .get("usd"),
        "market_cap": data.get("market_data", {})
                            .get("market_cap", {})
                            .get("usd"),
        "total_volume": data.get("market_data", {})
                            .get("total_volume", {})
                            .get("usd")
    }


@dag(
    start_date=datetime(2025, 8, 2, 0, 40),
    schedule="40 0 * * *", # at 00:40 UTC (the data is available for the previous day at 00:35 UTC)
    catchup=False,
    tags=['crypto', 'daily']
)
def coin_price():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests
        
        api = BaseHook.get_connection('coingecko_api')
        url = f"{api.host}v3/ping"
        print(url)
        response = requests.get(url, headers=json.loads(api.extra))
        print(response.json())
        # When the API is available, it should return the following response
        condition = response.json()['gecko_says'] == '(V3) To the Moon!'
        # I convert the xcom_value to a list with one element so that it works with the expand operator
        return PokeReturnValue(is_done=condition, xcom_value=[f"{api.host}v3/coins/"])
    

    # We use the coingecko_api_pool, which limits the number of concurrent tasks to avoid hitting API rate limits (30 per minute)
    @task(pool="coingecko_api_pool")
    def get_coin_price(url: str, coin_id: str, data_interval_start=None) -> str:
        import requests
        
        url = f"{url}{coin_id}/history?date={data_interval_start.strftime("%Y-%m-%d")}&localization=false"
        api = BaseHook.get_connection('coingecko_api')
        response = requests.get(url, headers=json.loads(api.extra))
        print(url)

        # Filter the JSON response to keep only the necessary fields
        filtered = filter_price_json(response.json())

        time.sleep(60)  # In order to enforce the requests per minute limit

        return json.dumps(filtered)
    
    @task
    def store_coin_price(coin: str, data_interval_start=None):
        
        upload_json_to_s3(
                    s3_hook=S3Hook(aws_conn_id="AwsS3Bucket"),
                    bucket=BUCKET_NAME,
                    key=f"bronze/coin_price/{data_interval_start.strftime("%Y-%m-%d")}/price_{json.loads(coin)['id']}.json",
                    data=coin
                )

    transform_coin_price = SparkSubmitOperator(
        task_id='transform_coin_price',
        application='/opt/airflow/dags/notebooks/scripts/fct_coin_price.py',
        conn_id='spark_default',
        conf={
            "spark.hadoop.fs.s3a.access.key": aws_access_key,
            "spark.hadoop.fs.s3a.secret.key": aws_secret_key,
            "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        },
        verbose=True,
        total_executor_cores=2,
        executor_memory='2G',
        driver_memory='1G',
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.526",
        application_args=[
            "--date", "{{ data_interval_start.strftime('%Y-%m-%d') }}"
        ]
    )

    # Get coin prices dynamically
    coin_price_results = get_coin_price.expand(
        url=is_api_available(),  # XCom value from sensor
        coin_id=coin_ids         # list of coin_ids
    )

    # Store each coin's data in S3
    store_coin_price.expand(
        coin=coin_price_results
    ) >> transform_coin_price


coin_price()







