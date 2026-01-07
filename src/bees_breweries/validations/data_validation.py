from pathlib import Path
import json
import pandas as pd

from bees_breweries.config.settings import settings


class DataLakeValidator:
    """
    Simple validator for Bronze, Silver and Gold data layers.

    Ensures record presence, basic schema integrity and consistency
    between aggregation layers.
    """
    def __init__(self):
        """
        Initialize validator with Bronze, Silver and Gold paths.
        """
        self.bronze_path = settings.BRONZE_BREWERIES_PATH
        self.silver_path = settings.SILVER_BREWERIES_PATH
        self.gold_path = settings.GOLD_BREWERIES_PATH

    def validate_bronze(self) -> dict:
        """
        Validate presence and volume of data in the Bronze layer.
        """
        partitions = 0
        files = 0
        records = 0

        for partition in self.bronze_path.glob("ingestion_date=*"):
            if not partition.is_dir():
                continue

            partitions += 1

            for file in partition.glob("*.json"):
                files += 1
                with file.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    records += len(data)

        if records == 0:
            raise ValueError("Bronze validation failed: no records found")

        print(
            f"[BRONZE] partitions={partitions} | files={files} | records={records}"
        )

        return {
            "partitions": partitions,
            "files": files,
            "records": records,
        }

    def validate_silver(self) -> dict:
        """
        Validate data completeness and basic integrity in the Silver layer.
        """
        df = pd.read_parquet(self.silver_path)

        total_records = len(df)
        countries = df["country"].nunique()
        states = df["state"].nunique()
        null_ids = df["id"].isnull().sum()

        if null_ids > 0:
            raise ValueError(
                f"Silver validation failed: {null_ids} null ids found"
            )

        print(
            f"[SILVER] records={total_records} | countries={countries} | states={states}"
        )

        return {
            "records": total_records,
            "countries": countries,
            "states": states,
        }

    def validate_gold(self, silver_records: int) -> dict:
        """
        Validate aggregation consistency between Silver and Gold layers.
        """
        gold_file = (
            self.gold_path / "breweries_by_type_and_location.parquet"
        )

        if not gold_file.exists():
            raise FileNotFoundError(
                f"Gold parquet not found: {gold_file}"
            )

        df = pd.read_parquet(gold_file)

        rows = len(df)
        total_breweries = int(df["brewery_count"].sum())

        if total_breweries != silver_records:
            raise ValueError(
                "Gold validation failed: aggregated total does not match silver"
            )

        print(
            f"[GOLD] rows={rows} | total_breweries={total_breweries}"
        )

        return {
            "rows": rows,
            "total_breweries": total_breweries,
        }

    def run(self):
        """
        Execute end-to-end validation across all data lake layers.
        """
        print("\n=== DATA LAKE VALIDATION STARTED ===\n")

        bronze_metrics = self.validate_bronze()
        silver_metrics = self.validate_silver()
        gold_metrics = self.validate_gold(
            silver_records=silver_metrics["records"]
        )

        print("\n--- VALIDATION SUMMARY ---")
        print(f"Bronze records : {bronze_metrics['records']}")
        print(f"Silver records : {silver_metrics['records']}")
        print(f"Gold total     : {gold_metrics['total_breweries']}")

        print("\n✅ DATA LAKE VALIDATION FINISHED SUCCESSFULLY\n")
