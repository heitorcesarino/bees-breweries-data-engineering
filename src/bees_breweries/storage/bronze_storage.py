import json
from datetime import date
from pathlib import Path
from typing import List, Dict, Optional

from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


class BronzeStorage:
    """
    Handles persistence of raw (bronze layer) data.

    This storage is responsible for saving raw JSON data exactly as received
    from the source system, partitioned by ingestion date.
    """

    def __init__(self, base_path: str = "data/bronze"):
        self.base_path = Path(base_path)

    def _build_partition_path(
        self,
        dataset: str,
        ingestion_date: Optional[date] = None,
    ) -> Path:
        """
        Build the partition path following the pattern:
        data/bronze/<dataset>/ingestion_date=YYYY-MM-DD/
        """
        ingestion_date = ingestion_date or date.today()
        return (
            self.base_path
            / dataset
            / f"ingestion_date={ingestion_date.isoformat()}"
        )

    def save_json(
        self,
        dataset: str,
        data: List[Dict],
        ingestion_date: Optional[date] = None,
        filename: str = "data.json",
    ) -> Path:
        """
        Persist raw JSON data to the bronze layer.

        Args:
            dataset: Logical dataset name (e.g. "breweries")
            data: Raw data returned from the API
            ingestion_date: Date of ingestion (defaults to today)
            filename: Output file name

        Returns:
            Path to the persisted file
        """
        if not data:
            raise ValueError("No data provided to persist in bronze layer")

        partition_path = self._build_partition_path(dataset, ingestion_date)
        partition_path.mkdir(parents=True, exist_ok=True)

        file_path = partition_path / filename

        logger.info(
            "Persisting bronze data",
            extra={
                "dataset": dataset,
                "records": len(data),
                "path": str(file_path),
            },
        )

        with file_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)

        return file_path
