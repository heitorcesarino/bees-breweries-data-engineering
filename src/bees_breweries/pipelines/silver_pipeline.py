import json
from pathlib import Path
import pandas as pd

from bees_breweries.models.brewery import Brewery
from bees_breweries.storage.silver_storage import SilverStorage
from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


class SilverBreweriesPipeline:
    def __init__(
        self,
        bronze_path: str = "data/bronze/breweries",
        storage: SilverStorage | None = None,
    ):
        self.bronze_path = Path(bronze_path)
        self.storage = storage or SilverStorage()

    def _load_bronze_files(self) -> list[dict]:
        if not self.bronze_path.exists():
            raise FileNotFoundError(f"Bronze path not found: {self.bronze_path}")

        records: list[dict] = []

        # percorre as partições ingestion_date=YYYY-MM-DD
        for partition in self.bronze_path.glob("ingestion_date=*"):
            if not partition.is_dir():
                continue

            for file in partition.glob("*.json"):
                logger.info("Reading bronze file %s", file)
                with open(file, "r", encoding="utf-8") as f:
                    records.extend(json.load(f))

        if not records:
            raise ValueError("No records found in bronze layer")

        return records

    def _normalize(self, records: list[dict]) -> pd.DataFrame:
        breweries = [Brewery(**record) for record in records]
        df = pd.DataFrame([brewery.model_dump() for brewery in breweries])
        return df

    def run(self) -> Path:
        logger.info("Starting Silver Pipeline")

        records = self._load_bronze_files()
        df = self._normalize(records)

        path = self.storage.save_breweries(df)

        logger.info("Silver Pipeline finished successfully")
        return path
