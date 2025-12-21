# dags/breweries_pipeline_dag.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from bees_breweries.clients.brewery import BreweryAPIClient
from bees_breweries.pipelines.bronze_pipeline import BronzeBreweriesPipeline
from bees_breweries.pipelines.silver_pipeline import SilverBreweriesPipeline
from bees_breweries.pipelines.gold_pipeline import GoldBreweriesPipeline
from bees_breweries.storage.bronze_storage import BronzeStorage
from bees_breweries.storage.silver_storage import SilverStorage
from bees_breweries.storage.gold_storage import GoldStorage
from bees_breweries.config.settings import settings


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="breweries_medallion_pipeline",
    description="End-to-end breweries pipeline using medallion architecture",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["breweries", "medallion", "data-lake"],
) as dag:

    def run_bronze():
        client = BreweryAPIClient()
        storage = BronzeStorage(base_path=settings.BRONZE_PATH)
        pipeline = BronzeBreweriesPipeline(client=client, storage=storage)
        pipeline.run()

    def run_silver():
        storage = SilverStorage(base_path=settings.SILVER_PATH)
        pipeline = SilverBreweriesPipeline(
            bronze_path=settings.BRONZE_PATH,
            storage=storage,
        )
        pipeline.run()

    def run_gold():
        storage = GoldStorage(base_path=settings.GOLD_PATH)
        pipeline = GoldBreweriesPipeline(
            silver_path=settings.SILVER_PATH,
            gold_storage=storage,
        )
        pipeline.run()

    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
    )

    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold,
    )

    bronze_task >> silver_task >> gold_task
