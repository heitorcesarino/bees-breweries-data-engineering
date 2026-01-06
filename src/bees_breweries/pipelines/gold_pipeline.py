from pathlib import Path
import pandas as pd

from bees_breweries.config.settings import settings
from bees_breweries.storage.gold_storage import GoldStorage
from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


class GoldBreweriesPipeline:
    def __init__(
        self,
        silver_path: Path | None = None,
        storage: GoldStorage | None = None,
    ):
        self.silver_path = silver_path or settings.SILVER_BREWERIES_PATH
        self.storage = storage or GoldStorage(settings.GOLD_BREWERIES_PATH)

    def run(self) -> Path:
        logger.info("Starting Gold Breweries Pipeline")

        if not self.silver_path.exists():
            raise FileNotFoundError(
                f"Silver dataset not found: {self.silver_path}"
            )

        df = pd.read_parquet(self.silver_path)

        aggregated_df = (
            df
            .groupby(
                ["country", "state", "city", "brewery_type"],
                observed=True,
                dropna=False,
            )
            .size()
            .reset_index(name="brewery_count")
        )

        path = self.storage.save_breweries(aggregated_df)

        logger.info(
            "Gold Breweries Pipeline finished successfully",
            extra={"output_path": str(path)},
        )
        return path
