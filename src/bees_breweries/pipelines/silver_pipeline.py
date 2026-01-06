import json
from pathlib import Path
from urllib.parse import unquote

import pandas as pd

from bees_breweries.config.settings import settings
from bees_breweries.models.brewery import Brewery
from bees_breweries.storage.silver_storage import SilverStorage
from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


def normalize_string(value: str | None) -> str:
    """
    Normalize string fields to be filesystem-safe and query-friendly.

    - Remove URL encoding (%20, etc)
    - Trim whitespace
    - Lowercase
    - Replace spaces with underscores
    """
    if not value:
        return "unknown"

    return (
        unquote(str(value))
        .strip()
        .lower()
        .replace(" ", "_")
    )


class SilverBreweriesPipeline:
    def __init__(
        self,
        bronze_path: Path | None = None,
        storage: SilverStorage | None = None,
    ):
        self.bronze_path = bronze_path or settings.BRONZE_BREWERIES_PATH
        self.storage = storage or SilverStorage()

    def _load_bronze_files(self) -> list[dict]:
        if not self.bronze_path.exists():
            raise FileNotFoundError(f"Bronze path not found: {self.bronze_path}")

        records: list[dict] = []

        for partition in self.bronze_path.glob("ingestion_date=*"):
            if not partition.is_dir():
                continue

            for file in partition.glob("*.json"):
                logger.info("Reading bronze file %s", file)
                with file.open("r", encoding="utf-8") as f:
                    records.extend(json.load(f))

        if not records:
            raise ValueError(
                f"No records found in bronze layer at {self.bronze_path}"
            )

        logger.info("Loaded %s records from bronze layer", len(records))
        return records

    def _normalize(self, records: list[dict]) -> pd.DataFrame:
        breweries = [Brewery(**record) for record in records]
        df = pd.DataFrame([brewery.model_dump() for brewery in breweries])

        # Normalize location and categorical fields
        df["country"] = df["country"].apply(normalize_string)
        df["state"] = df["state"].apply(normalize_string)
        df["city"] = df["city"].apply(normalize_string)
        df["brewery_type"] = df["brewery_type"].apply(normalize_string)

        return df

    def run(self) -> Path:
        logger.info("Starting Silver Breweries Pipeline")

        records = self._load_bronze_files()
        df = self._normalize(records)

        path = self.storage.save_breweries(df)

        logger.info(
            "Silver Breweries Pipeline finished successfully",
            extra={"output_path": str(path)},
        )

        return path
