from pathlib import Path
import pandas as pd

from bees_breweries.config.settings import settings
from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


class SilverStorage:
    def __init__(self, base_path: Path | None = None):
        # data/silver/breweries
        self.base_path = base_path or settings.SILVER_BREWERIES_PATH
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_breweries(
        self,
        df: pd.DataFrame,
    ) -> Path:
        logger.info(
            "Saving silver breweries data (partitioned)",
            extra={
                "records": len(df),
                "path": str(self.base_path),
                "partitions": ["country", "state", "city"],
            },
        )

        df.to_parquet(
            self.base_path,
            partition_cols=["country"],
            index=False,
        )

        return self.base_path
